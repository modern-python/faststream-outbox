"""Test broker with an in-memory ``OutboxClient`` substitute.

``TestOutboxBroker`` wraps an ``OutboxBroker`` and swaps in a ``FakeOutboxClient``
backed by a list of ``_FakeRow`` records. Publishing runs the broker's real producer
and publish middlewares; only the storage is fake, so ``broker.publish`` needs a
session here too (``fake_session()`` returns a mock one). Defaults to **sync dispatch**: ``await broker.publish(...)``
finds the matching subscriber and awaits its consume pipeline before returning, the
same model as ``TestKafkaBroker`` / ``TestRabbitBroker``. Timers fire immediately in
sync mode — ``activate_in`` / ``activate_at`` are recorded on the fake row but not
honored. Pass ``run_loops=True`` to restore the loop-driven behavior — the real
``_fetch_loop`` / ``_worker_loop`` run against the in-memory client; required for
tests that exercise retry rescheduling, lease expiry, fetch-loop error recovery,
or scheduled delivery actually waiting. ``feed()`` simulates a row insert.
"""

import datetime as dt
import json
import typing
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from unittest import mock

from faststream._internal.testing.broker import TestBroker, patch_broker_calls
from sqlalchemy.ext.asyncio import AsyncSession

from faststream_outbox._scheduling import resolve_next_attempt_client_side
from faststream_outbox._session import SessionOrConnection
from faststream_outbox._time import utcnow
from faststream_outbox.broker import OutboxBroker
from faststream_outbox.client import _NO_DLQ_TABLE_MSG, AbstractOutboxClient
from faststream_outbox.message import OutboxInnerMessage
from faststream_outbox.response import _MAX_QUEUE_LENGTH, _REQUEST_UNSUPPORTED_MSG, OutboxPublishCommand
from faststream_outbox.schema import _DLQ_INJECTED_COLUMNS, _DLQ_PROJECTION


if typing.TYPE_CHECKING:
    from collections.abc import Generator, Sequence

    from faststream._internal.producer import ProducerProto

    from faststream_outbox.subscriber.usecase import OutboxSubscriber


@dataclass
class _FakeRow:
    id: int
    queue: str
    payload: bytes
    headers: dict[str, str] | None
    attempts_count: int = 0
    deliveries_count: int = 0
    created_at: dt.datetime = field(default_factory=utcnow)
    next_attempt_at: dt.datetime = field(default_factory=utcnow)
    first_attempt_at: dt.datetime | None = None
    last_attempt_at: dt.datetime | None = None
    acquired_at: dt.datetime | None = None
    acquired_token: uuid.UUID | None = None
    timer_id: str | None = None


def _claim_fake_row(row: _FakeRow, *, now: dt.datetime, token: uuid.UUID) -> None:
    """Set the lease and bump ``deliveries_count`` — the shared claim mechanics (F1-08).

    Both ``FakeOutboxClient.fetch`` (loop mode) and ``_sync_dispatch`` (sync mode) route
    through here, so the ``max_deliveries`` boundary is exercised on one path. Eligibility
    gating (``next_attempt_at <= now``) stays in ``fetch`` only — sync mode deliberately
    fires future-dated rows immediately, so it claims its row without that gate.
    """
    row.acquired_at = now
    row.acquired_token = token
    row.deliveries_count += 1


def _lease_token_matches(row_token: "uuid.UUID | None", claim_token: "uuid.UUID | None") -> bool:
    """Mirror SQL's ``WHERE acquired_token = :token`` guard, ``NULL = NULL`` included.

    ``NULL = NULL`` is NULL (no match) in SQL, so a ``None`` claim token must never match —
    not even a row whose own token is ``None``. A ``None`` claim short-circuits to ``False``;
    otherwise the row token must equal it (``None == uuid`` is ``False``). Names the rule the
    real client's terminal writes encode in SQL; parity with that SQL is pinned by
    ``tests/test_client_contract.py`` (delete / mark_pending token-match scenarios), not by
    these comments.
    """
    return claim_token is not None and row_token == claim_token


def _validate_like_postgres(*, queue: str, timer_id: str | None, headers: dict[str, str] | None) -> None:
    """Reject what the INSERT would fail on: over-long ``String(255)`` values and headers JSONB can't hold."""
    if len(queue) > _MAX_QUEUE_LENGTH:
        msg = f"queue must be at most {_MAX_QUEUE_LENGTH} characters (got {len(queue)})"
        raise ValueError(msg)
    if timer_id is not None and len(timer_id) > _MAX_QUEUE_LENGTH:
        msg = f"timer_id must be at most {_MAX_QUEUE_LENGTH} characters (got {len(timer_id)})"
        raise ValueError(msg)
    try:
        json.dumps(headers)
    except (TypeError, ValueError) as exc:
        msg = f"headers must be JSON-serializable: {exc}"
        raise ValueError(msg) from exc


class FakeOutboxClient(AbstractOutboxClient):
    """In-memory ``OutboxClient`` substitute. Same surface, list-of-rows storage.

    ``dlq_enabled=False`` mirrors an ``OutboxClient`` built without a ``dlq_table``: a
    ``dlq_payload`` is refused instead of producing an audit row.
    """

    def __init__(self, *, dlq_enabled: bool = True) -> None:
        self._dlq_enabled = dlq_enabled
        self._rows: list[_FakeRow] = []
        self._next_id = 1
        # Populated when ``delete_with_lease`` receives a ``dlq_payload``. Mirrors
        # the real client's CTE side-effect: outbox row gone + DLQ row created in
        # the same call. Tests assert against ``test_broker.fake_client.dlq_rows``.
        self._dlq_rows: list[dict[str, typing.Any]] = []
        self._dispatchable: list[_FakeRow] = []

    def feed(
        self,
        *,
        queue: str,
        payload: bytes,
        headers: dict[str, str] | None = None,
        next_attempt_at: dt.datetime | None = None,
        timer_id: str | None = None,
    ) -> int | None:
        # P31: reject naive datetimes up front — the publish path is tz-strict, and a
        # naive next_attempt_at otherwise blows up deep in a patched-away logger with no
        # diagnostic. Match the production contract here.
        if next_attempt_at is not None and next_attempt_at.tzinfo is None:
            msg = "feed() requires next_attempt_at to be timezone-aware"
            raise ValueError(msg)
        _validate_like_postgres(queue=queue, timer_id=timer_id, headers=headers)
        row = self._store(
            queue=queue, payload=payload, headers=headers, next_attempt_at=next_attempt_at, timer_id=timer_id
        )
        return None if row is None else row.id

    async def insert(
        self,
        session: SessionOrConnection,
        *,
        queue: str,
        payload: bytes,
        headers: dict[str, str] | None,
        activate_in: dt.timedelta | None = None,
        activate_at: dt.datetime | None = None,
        timer_id: str | None = None,
    ) -> int | None:
        del session  # ignored (no real DB)
        _validate_like_postgres(queue=queue, timer_id=timer_id, headers=headers)
        row = self._store(
            queue=queue,
            payload=payload,
            headers=headers,
            next_attempt_at=resolve_next_attempt_client_side(activate_in, activate_at, utcnow()),
            timer_id=timer_id,
        )
        if row is None:
            return None
        self._dispatchable.append(row)
        return row.id

    async def insert_batch(
        self,
        session: SessionOrConnection,
        *,
        queue: str,
        rows: "Sequence[tuple[bytes, dict[str, str] | None]]",
        activate_in: dt.timedelta | None = None,
        activate_at: dt.datetime | None = None,
    ) -> None:
        del session  # ignored (no real DB)
        for _, headers in rows:
            _validate_like_postgres(queue=queue, timer_id=None, headers=headers)
        next_at = resolve_next_attempt_client_side(activate_in, activate_at, utcnow())
        for payload, headers in rows:
            self._dispatchable.append(
                self._append(queue=queue, payload=payload, headers=headers, next_attempt_at=next_at, timer_id=None)
            )

    def _take_dispatchable(self) -> list[_FakeRow]:
        """Return and forget the rows ``insert`` / ``insert_batch`` stored since the last call."""
        rows, self._dispatchable = self._dispatchable, []
        return rows

    def _store(
        self,
        *,
        queue: str,
        payload: bytes,
        headers: dict[str, str] | None,
        next_attempt_at: dt.datetime | None,
        timer_id: str | None,
    ) -> _FakeRow | None:
        # Mirror the real client's partial-unique-on-(queue, timer_id) behavior:
        # re-feeding a timer that already exists is a no-op.
        if timer_id is not None and any(r.queue == queue and r.timer_id == timer_id for r in self._rows):
            return None
        return self._append(
            queue=queue, payload=payload, headers=headers, next_attempt_at=next_attempt_at, timer_id=timer_id
        )

    def _append(
        self,
        *,
        queue: str,
        payload: bytes,
        headers: dict[str, str] | None,
        next_attempt_at: dt.datetime | None,
        timer_id: str | None,
    ) -> _FakeRow:
        row = _FakeRow(
            id=self._next_id,
            queue=queue,
            payload=payload,
            # P32: store a copy so a handler mutating msg.headers can't corrupt the
            # "persisted" row (the real client round-trips through the DB; the fake must
            # not share the dict by reference).
            headers=dict(headers) if headers is not None else None,
            next_attempt_at=next_attempt_at or utcnow(),
            timer_id=timer_id,
        )
        self._rows.append(row)
        self._next_id += 1
        return row

    @property
    def rows(self) -> list[_FakeRow]:
        return self._rows

    @property
    def dlq_rows(self) -> list[dict[str, typing.Any]]:
        """Audit copies produced by ``delete_with_lease(..., dlq_payload=...)``."""
        return self._dlq_rows

    @property
    def table(self) -> typing.Any:
        return None

    @property
    def engine(self) -> None:
        """No real engine — signals the subscriber loop to use the polling-only path."""
        return None

    async def fetch(
        self,
        conn: typing.Any,  # noqa: ARG002
        queues: "Sequence[str]",
        *,
        limit: int,
        lease_ttl_seconds: float,
    ) -> list[OutboxInnerMessage]:
        if not queues:
            return []
        now = utcnow()
        lease_cutoff = now - dt.timedelta(seconds=lease_ttl_seconds)
        token = uuid.uuid4()
        out: list[OutboxInnerMessage] = []
        eligible = sorted(
            (
                r
                for r in self._rows
                if r.queue in queues
                and r.next_attempt_at <= now
                and (r.acquired_token is None or (r.acquired_at is not None and r.acquired_at < lease_cutoff))
            ),
            key=lambda r: (r.next_attempt_at, r.id),
        )
        for row in eligible[:limit]:
            _claim_fake_row(row, now=now, token=token)
            out.append(_to_inner(row))
        return out

    async def delete_with_lease(
        self,
        conn: typing.Any,  # noqa: ARG002
        message_id: int,
        acquired_token: uuid.UUID,
        *,
        dlq_payload: "typing.Mapping[str, typing.Any] | None" = None,
    ) -> bool:
        for i, row in enumerate(self._rows):
            if row.id == message_id and _lease_token_matches(row.acquired_token, acquired_token):
                if dlq_payload is not None:
                    if not self._dlq_enabled:
                        raise RuntimeError(_NO_DLQ_TABLE_MSG)
                    # Mirror the real CTE side-effect: DLQ row materializes in the same
                    # call as the DELETE, before the row is removed. Built from the shared
                    # _DLQ_PROJECTION so the fake can't drift from the real CTE; failed_at
                    # mirrors the DLQ column server_default.
                    dlq_row: dict[str, typing.Any] = {
                        dlq_col: getattr(row, outbox_col) for outbox_col, dlq_col in _DLQ_PROJECTION
                    }
                    dlq_row["failed_at"] = utcnow()
                    for col in _DLQ_INJECTED_COLUMNS:
                        dlq_row[col] = dlq_payload[col]
                    self._dlq_rows.append(dlq_row)
                del self._rows[i]
                return True
        return False

    async def delete_batch_with_lease(
        self,
        conn: typing.Any,  # noqa: ARG002
        pairs: "Sequence[tuple[int, uuid.UUID]]",
    ) -> set[tuple[int, uuid.UUID]]:
        # Set-based form of the same rule as `_lease_token_matches`: the `ptok is not None`
        # filter is the claim-side NULL guard, `row.acquired_token is not None` the row-side —
        # so a NULL token on either side never matches (SQL `NULL = NULL`).
        wanted = {(pid, ptok) for pid, ptok in pairs if ptok is not None}
        deleted: set[tuple[int, uuid.UUID]] = set()
        # Iterate a copy of indices high-to-low so deletions don't shift unscanned rows.
        for i in range(len(self._rows) - 1, -1, -1):
            row = self._rows[i]
            if row.acquired_token is not None and (row.id, row.acquired_token) in wanted:
                deleted.add((row.id, row.acquired_token))
                del self._rows[i]
        return deleted

    async def mark_pending_with_lease(
        self,
        conn: typing.Any,  # noqa: ARG002
        message_id: int,
        acquired_token: uuid.UUID,
        *,
        delay_seconds: float,
        attempts_count: int,
        first_attempt_at: dt.datetime,
        last_attempt_at: dt.datetime,
    ) -> bool:
        for row in self._rows:
            if row.id == message_id and _lease_token_matches(row.acquired_token, acquired_token):
                row.next_attempt_at = utcnow() + dt.timedelta(seconds=max(0.0, delay_seconds))
                row.attempts_count = attempts_count
                row.first_attempt_at = first_attempt_at
                row.last_attempt_at = last_attempt_at
                row.acquired_at = None
                row.acquired_token = None
                return True
        return False

    async def cancel_timer(self, *, queue: str, timer_id: str, session: SessionOrConnection) -> bool:
        """Mirror :meth:`OutboxBroker.cancel_timer` — drop a not-yet-leased timer row."""
        del session  # ignored (no real DB), matching the fake publish contract
        for i, row in enumerate(self._rows):
            if row.queue == queue and row.timer_id == timer_id and row.acquired_token is None:
                del self._rows[i]
                return True
        return False

    async def fetch_unprocessed(
        self,
        *,
        session: SessionOrConnection,
        queue: str | None = None,
        limit: int = 1000,
    ) -> list[OutboxInnerMessage]:
        """Mirror :meth:`OutboxBroker.fetch_unprocessed` — read rows from the in-memory store."""
        del session  # ignored (no real DB), matching the fake publish contract
        if limit < 1:
            # Mirror the real client's validation so a non-positive limit can't
            # silently mis-slice (rows[:-1]) under the test broker (F4-04).
            msg = f"limit must be >= 1, got {limit}"
            raise ValueError(msg)
        rows = sorted(self.rows, key=lambda r: r.id)
        if queue is not None:
            rows = [r for r in rows if r.queue == queue]
        # Mirror production's ``limit`` (default 1000) so a valid
        # ``broker.fetch_unprocessed(..., limit=N)`` call doesn't TypeError under
        # the test broker (B16).
        return [_to_inner(r) for r in rows[:limit]]

    async def validate_schema(self, *, check_autovacuum: bool = False) -> None:
        # Silently passing here would give tests false confidence — a user calling
        # ``broker.validate_schema()`` against ``TestOutboxBroker`` would see a green
        # test regardless of whether the real DB schema matches the canonical one.
        # Raise loudly so the operator routes schema-validation tests through a real
        # ``OutboxClient`` against the same DSN their migrations ran against.
        msg = (
            "validate_schema is unavailable on TestOutboxBroker / FakeOutboxClient "
            "(no real DB connection). Use OutboxClient(real_engine, table) in tests "
            "that need to verify the live schema."
        )
        raise NotImplementedError(msg)

    async def ping(self) -> bool:
        return True


def _to_inner(row: _FakeRow) -> OutboxInnerMessage:
    return OutboxInnerMessage(
        id=row.id,
        queue=row.queue,
        payload=row.payload,
        headers=dict(row.headers) if row.headers is not None else None,  # P32: don't share the dict by reference
        attempts_count=row.attempts_count,
        deliveries_count=row.deliveries_count,
        created_at=row.created_at,
        next_attempt_at=row.next_attempt_at,
        first_attempt_at=row.first_attempt_at,
        last_attempt_at=row.last_attempt_at,
        acquired_at=row.acquired_at,
        acquired_token=row.acquired_token,
        timer_id=row.timer_id,
    )


def _find_subscriber_for_queue(broker: OutboxBroker, queue: str) -> "OutboxSubscriber | None":
    """First matching subscriber wins (deterministic).

    NB: this does NOT mirror production for *overlapping* subscribers — there, multiple
    subscribers on the same queue compete via ``FOR UPDATE SKIP LOCKED``, so which one
    claims a given row is nondeterministic. The fake picks the first match for test
    repeatability (P35).
    """
    for raw_subscriber in broker.subscribers:
        sub = typing.cast("OutboxSubscriber", raw_subscriber)
        if not sub.calls:
            continue
        if queue in sub._config.queues:  # noqa: SLF001
            return sub
    return None


async def _sync_dispatch(fake_client: FakeOutboxClient, broker: OutboxBroker, queue: str, row_id: int) -> None:
    """Acquire the just-fed row's lease in place and run it through the subscriber pipeline."""
    subscriber = _find_subscriber_for_queue(broker, queue)
    if subscriber is None:
        # No handler for this queue — leave the row in the fake client for inspection.
        return
    fake_row = next((r for r in fake_client.rows if r.id == row_id), None)
    if fake_row is None:  # pragma: no cover - only if an earlier handler in this batch deleted the row off the fake
        return
    _claim_fake_row(fake_row, now=utcnow(), token=uuid.uuid4())
    await subscriber.dispatch_one(_to_inner(fake_row))


def _notify_subscriber(broker: OutboxBroker, queue: str) -> None:
    """P30: wake the matching subscriber's fetch loop immediately, mirroring production NOTIFY.

    Only meaningful in loop mode; callers gate on ``run_loops``. Lets loop-mode tests rely
    on prompt wakeup instead of a tight ``min_fetch_interval`` poll.
    """
    subscriber = _find_subscriber_for_queue(broker, queue)
    if subscriber is not None:
        subscriber._notify_event.set()  # noqa: SLF001


class _DispatchingProducer:
    """Runs the broker's real producer, then hands the rows it stored to the test broker's dispatch."""

    def __init__(self, producer: "ProducerProto[OutboxPublishCommand]", test_broker: "TestOutboxBroker") -> None:
        self._producer = producer
        self._test_broker = test_broker
        self._parser = producer._parser  # noqa: SLF001  # ProducerProto declares it private
        self._decoder = producer._decoder  # noqa: SLF001  # ProducerProto declares it private
        self.codec = producer.codec

    async def publish(self, cmd: OutboxPublishCommand) -> int | None:
        row_id = await self._producer.publish(cmd)
        await self._test_broker._dispatch_stored()  # noqa: SLF001  # internal hook between two testing.py classes
        return typing.cast("int | None", row_id)

    async def publish_batch(self, cmd: OutboxPublishCommand) -> None:
        await self._producer.publish_batch(cmd)
        await self._test_broker._dispatch_stored()  # noqa: SLF001  # internal hook between two testing.py classes

    async def request(self, cmd: OutboxPublishCommand) -> typing.NoReturn:
        raise NotImplementedError(_REQUEST_UNSUPPORTED_MSG)


def fake_session() -> "AsyncSession":
    """Return a mock ``AsyncSession`` for publishing under ``TestOutboxBroker``.

    It passes the session type check and reports an open transaction, so
    ``OutboxResponse`` accepts it too. Every method is a no-op: ``commit`` and
    ``rollback`` change nothing, and the fake store keeps rows either way.
    """
    session = mock.AsyncMock(spec=AsyncSession)
    session.in_transaction.return_value = True
    return session


class TestOutboxBroker(TestBroker[OutboxBroker, OutboxBroker], broker=OutboxBroker):
    """Test harness for ``OutboxBroker``. Two dispatch modes.

    Default (``run_loops=False``): ``broker.publish`` synchronously drives the matching
    subscriber's consume pipeline, so handlers run before ``publish`` returns. Matches the
    FastStream test-broker idiom — ``TestKafkaBroker`` / ``TestRabbitBroker`` behave the
    same way. Future-dated rows (``activate_in`` / ``activate_at``) **fire immediately** in
    sync mode — sync dispatch ignores ``next_attempt_at``. The future-dated gate only applies
    in loop mode (``run_loops=True``), where the fetch loop honors ``next_attempt_at``.

    Pass ``run_loops=True`` to spin up the real ``_fetch_loop`` / ``_worker_loop`` against
    the in-memory client. Required for tests that exercise loop-driven behavior:
    retry rescheduling, lease expiry reclaim, scheduled-delivery waiting, or fetch-loop
    error recovery.
    """

    fake_client: FakeOutboxClient
    run_loops: bool

    def __init__(self, broker: OutboxBroker, *, run_loops: bool = False, **kwargs: typing.Any) -> None:
        super().__init__(broker, **kwargs)
        self.fake_client = FakeOutboxClient(dlq_enabled=broker.config.broker_config.dlq_table is not None)
        self.run_loops = run_loops
        self._outbox_broker = broker  # for feed()'s P30 wakeup
        # Guards against the upstream harness spawning the loops twice (B14); reset
        # by _fake_close so a re-entered context can spawn again.
        self._loops_spawned = False

    def feed(
        self,
        queue: str,
        payload: bytes,
        *,
        headers: dict[str, str] | None = None,
        next_attempt_at: dt.datetime | None = None,
        timer_id: str | None = None,
    ) -> int | None:
        """Insert a row directly into the in-memory store. Returns the row id, or None on timer_id conflict."""
        row_id = self.fake_client.feed(
            queue=queue,
            payload=payload,
            headers=headers,
            next_attempt_at=next_attempt_at,
            timer_id=timer_id,
        )
        if self.run_loops:
            _notify_subscriber(self._outbox_broker, queue)  # P30: wake the fetch loop, like a production NOTIFY
        return row_id

    @contextmanager
    def _patch_producer(self, broker: OutboxBroker) -> "Generator[None]":
        original_producer = broker.config.broker_config.producer
        broker.config.broker_config.producer = _DispatchingProducer(original_producer, self)
        try:
            yield
        finally:
            broker.config.broker_config.producer = original_producer
            self.fake_client._take_dispatchable()  # noqa: SLF001  # drop rows no publish dispatched

    @contextmanager
    def _patch_broker(self, broker: OutboxBroker) -> "Generator[None]":
        original_client = broker.config.broker_config.client
        broker.config.broker_config.client = self.fake_client
        try:
            with super()._patch_broker(broker):
                yield
        finally:
            broker.config.broker_config.client = original_client

    async def _dispatch_stored(self) -> None:
        """Deliver what the last publish stored: run each row's handler (sync mode) or wake the fetch loops."""
        rows = self.fake_client._take_dispatchable()  # noqa: SLF001  # internal hook between two testing.py classes
        if self.run_loops:
            for queue in dict.fromkeys(row.queue for row in rows):
                _notify_subscriber(self._outbox_broker, queue)  # P30: the loops own dispatch; just wake them
            return
        for row in rows:
            # Sync dispatch ignores next_attempt_at (timers fire immediately in test mode).
            await _sync_dispatch(self.fake_client, self._outbox_broker, row.queue, row.id)

    def create_publisher_fake_subscriber(  # pragma: no cover - TestBroker abstract method; _fake_start skips its caller
        self,
        broker: OutboxBroker,
        publisher: typing.Any,
    ) -> tuple["OutboxSubscriber", bool]:
        # Required by FastStream's TestBroker abstract base, but never called —
        # we skip the publisher fake-subscriber loop in ``_fake_start`` because
        # the real producer already lands rows in the fake client AND ``_dispatch_stored``
        # drives the real subscriber. The FastStream
        # publisher-spy infrastructure would mock the real handler and break that.
        del self, broker, publisher
        msg = "TestOutboxBroker dispatches publisher rows itself; this is unreachable."
        raise NotImplementedError(msg)

    def _fake_start(self, broker: OutboxBroker, *args: typing.Any, **kwargs: typing.Any) -> None:
        del args, kwargs
        # Skip the parent's publisher iteration — see ``create_publisher_fake_subscriber``
        # for why. We still need to fan out ``_post_start`` on subscribers so their
        # call models build (matches what TestBroker._fake_start does last).
        patch_broker_calls(broker)
        for subscriber in broker.subscribers:
            typing.cast("OutboxSubscriber", subscriber)._stopping = False  # noqa: SLF001
            subscriber._post_start()  # noqa: SLF001
        broker._warn_on_unstarted_foreign_publishers()  # noqa: SLF001
        # In sync mode, publish drives dispatch directly — don't spawn the loops.
        if not self.run_loops:
            return
        # The upstream TestBroker harness calls the patched start() twice (once via
        # ``async with broker`` -> __aenter__ -> start, once via _do_start ->
        # broker.start()). Spawn the loops only once or a max_workers=1 test silently
        # runs two workers (B14).
        if self._loops_spawned:
            return
        self._loops_spawned = True
        # Loop mode: spin up the real subscriber loops against the in-memory fake client.
        # Skip subscribers without a registered handler — matches OutboxSubscriber.start()'s
        # ``if not self.calls: return`` behavior so the test broker doesn't access ``_client``
        # for an inert subscriber.
        for raw_subscriber in broker.subscribers:
            sub = typing.cast("OutboxSubscriber", raw_subscriber)
            if not sub.calls:
                continue
            for _ in range(sub._config.max_workers):  # noqa: SLF001
                sub.add_task(sub._worker_loop)  # noqa: SLF001
            sub.add_task(sub._fetch_loop)  # noqa: SLF001

    def _fake_close(self, broker: OutboxBroker, *args: typing.Any, **kwargs: typing.Any) -> None:
        # Upstream's _fake_close only flips ``sub.running = False``; in loop mode the
        # spawned fetch/worker tasks are left pending (workers parked on
        # ``_inflight.get()`` never wake), leaking "Task was destroyed but it is pending!"
        # noise and stale workers that fire on a re-entered context (B15). Cancel and
        # clear them, and reset the spawn guard so a re-entered context starts fresh.
        if self.run_loops:
            for raw_subscriber in broker.subscribers:
                sub = typing.cast("OutboxSubscriber", raw_subscriber)
                for task in sub.tasks:
                    if not task.done():
                        task.cancel()
                sub.tasks.clear()
                sub._reset_inflight()  # noqa: SLF001
            self._loops_spawned = False
        super()._fake_close(broker, *args, **kwargs)

    async def _fake_connect(self, broker: OutboxBroker, *args: typing.Any, **kwargs: typing.Any) -> None:  # noqa: ARG002
        return
