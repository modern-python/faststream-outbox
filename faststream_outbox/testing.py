"""
Test broker with an in-memory ``OutboxClient`` substitute.

``TestOutboxBroker`` wraps an ``OutboxBroker`` and swaps in a ``FakeOutboxClient``
backed by a list of ``_FakeRow`` records. Defaults to **sync dispatch**: ``await broker.publish(...)``
finds the matching subscriber and awaits its consume pipeline before returning, the
same model as ``TestKafkaBroker`` / ``TestRabbitBroker``. Timers fire immediately in
sync mode — ``activate_in`` / ``activate_at`` are recorded on the fake row but not
honored. Pass ``run_loops=True`` to restore the loop-driven behavior — the real
``_fetch_loop`` / ``_worker_loop`` run against the in-memory client; required for
tests that exercise retry rescheduling, lease expiry, fetch-loop error recovery,
or scheduled delivery actually waiting. ``feed()`` simulates a row insert.
"""

import datetime as _dt
import typing
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from unittest import mock

from faststream._internal.testing.broker import TestBroker, patch_broker_calls

from faststream_outbox._time import utcnow
from faststream_outbox.broker import (
    OutboxBroker,
    _compute_next_at_client_side,
    _validate_activate_args,
)
from faststream_outbox.client import AbstractOutboxClient
from faststream_outbox.envelope import _encode_payload
from faststream_outbox.message import OutboxInnerMessage
from faststream_outbox.metrics import _safe_emit
from faststream_outbox.response import _REQUEST_UNSUPPORTED_MSG, OutboxPublishCommand


if typing.TYPE_CHECKING:
    from collections.abc import Iterator, Sequence

    from faststream_outbox.subscriber.usecase import OutboxSubscriber


@dataclass
class _FakeRow:
    id: int
    queue: str
    payload: bytes
    headers: dict[str, str] | None
    attempts_count: int = 0
    deliveries_count: int = 0
    created_at: _dt.datetime = field(default_factory=utcnow)
    next_attempt_at: _dt.datetime = field(default_factory=utcnow)
    first_attempt_at: _dt.datetime | None = None
    last_attempt_at: _dt.datetime | None = None
    acquired_at: _dt.datetime | None = None
    acquired_token: uuid.UUID | None = None
    timer_id: str | None = None


class FakeOutboxClient(AbstractOutboxClient):
    """In-memory ``OutboxClient`` substitute. Same surface, list-of-rows storage."""

    def __init__(self) -> None:
        self._rows: list[_FakeRow] = []
        self._next_id = 1
        # Populated when ``delete_with_lease`` receives a ``dlq_payload``. Mirrors
        # the real client's CTE side-effect: outbox row gone + DLQ row created in
        # the same call. Tests assert against ``test_broker.fake_client.dlq_rows``.
        self._dlq_rows: list[dict[str, typing.Any]] = []

    def feed(
        self,
        *,
        queue: str,
        payload: bytes,
        headers: dict[str, str] | None = None,
        next_attempt_at: _dt.datetime | None = None,
        timer_id: str | None = None,
    ) -> int | None:
        # P31: reject naive datetimes up front — the publish path is tz-strict, and a
        # naive next_attempt_at otherwise blows up deep in a patched-away logger with no
        # diagnostic. Match the production contract here.
        if next_attempt_at is not None and next_attempt_at.tzinfo is None:
            msg = "feed() requires next_attempt_at to be timezone-aware"
            raise ValueError(msg)
        # Mirror the real client's partial-unique-on-(queue, timer_id) behavior:
        # re-feeding a timer that already exists is a no-op.
        if timer_id is not None and any(r.queue == queue and r.timer_id == timer_id for r in self._rows):
            return None
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
        return row.id

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
        lease_cutoff = now - _dt.timedelta(seconds=max(0.0, lease_ttl_seconds))
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
            row.acquired_at = now
            row.acquired_token = token
            row.deliveries_count += 1
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
            # ``acquired_token is not None`` mirrors SQL's ``WHERE acquired_token = :token``:
            # ``NULL = NULL`` is NULL (no match), so a None token must never match — even a
            # row whose own token is None. Without this, the fake's ``None == None`` would
            # delete where the real client no-ops.
            if row.id == message_id and acquired_token is not None and row.acquired_token == acquired_token:
                if dlq_payload is not None:
                    # Mirror the real CTE side-effect: DLQ row materializes in the
                    # same call as the DELETE, before the row is removed.
                    self._dlq_rows.append(
                        {
                            "original_id": row.id,
                            "queue": row.queue,
                            "payload": row.payload,
                            "headers": row.headers,
                            "deliveries_count": row.deliveries_count,
                            "created_at": row.created_at,
                            "failed_at": utcnow(),
                            "failure_reason": dlq_payload["failure_reason"],
                            "last_exception": dlq_payload["last_exception"],
                            "timer_id": row.timer_id,  # P9 parity with the real DLQ CTE
                        },
                    )
                del self._rows[i]
                return True
        return False

    async def mark_pending_with_lease(
        self,
        conn: typing.Any,  # noqa: ARG002
        message_id: int,
        acquired_token: uuid.UUID,
        *,
        delay_seconds: float,
        attempts_count: int,
        first_attempt_at: _dt.datetime,
        last_attempt_at: _dt.datetime,
    ) -> bool:
        for row in self._rows:
            # Mirror SQL ``NULL = NULL`` semantics — see ``delete_with_lease``.
            if row.id == message_id and acquired_token is not None and row.acquired_token == acquired_token:
                row.next_attempt_at = utcnow() + _dt.timedelta(seconds=max(0.0, delay_seconds))
                row.attempts_count = attempts_count
                row.first_attempt_at = first_attempt_at
                row.last_attempt_at = last_attempt_at
                row.acquired_at = None
                row.acquired_token = None
                return True
        return False

    async def cancel_timer(self, *, queue: str, timer_id: str) -> bool:
        """Mirror :meth:`OutboxBroker.cancel_timer` — drop a not-yet-leased timer row."""
        for i, row in enumerate(self._rows):
            if row.queue == queue and row.timer_id == timer_id and row.acquired_token is None:
                del self._rows[i]
                return True
        return False

    async def validate_schema(self) -> None:
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
    """
    First matching subscriber wins (deterministic).

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
    if fake_row is None:  # pragma: no cover  # defensive: feed just returned this id
        return
    fake_row.acquired_token = uuid.uuid4()
    fake_row.acquired_at = utcnow()
    fake_row.deliveries_count += 1
    await subscriber.dispatch_one(_to_inner(fake_row))


def _emit_published(broker: OutboxBroker, queue: str, *, count: int, size_bytes: int) -> None:
    """Emit the producer-side ``published`` metric on the broker's recorder (test-broker parity)."""
    _safe_emit(
        broker.config.broker_config.metrics_recorder,
        "published",
        {
            "queue": queue,
            "status": "success",
            "count": count,
            "size_bytes": size_bytes,
            "duration_seconds": 0.0,
        },
    )


def _notify_subscriber(broker: OutboxBroker, queue: str) -> None:
    """
    P30: wake the matching subscriber's fetch loop immediately, mirroring production NOTIFY.

    Only meaningful in loop mode; callers gate on ``run_loops``. Lets loop-mode tests rely
    on prompt wakeup instead of a tight ``min_fetch_interval`` poll.
    """
    subscriber = _find_subscriber_for_queue(broker, queue)
    if subscriber is not None:
        subscriber._notify_event.set()  # noqa: SLF001


async def _fake_publish_one(
    fake_client: FakeOutboxClient,
    broker: OutboxBroker,
    serializer: typing.Any,
    *,
    body: typing.Any,
    queue: str,
    headers: dict[str, str] | None,
    correlation_id: str | None,
    next_at: "_dt.datetime | None",
    timer_id: str | None,
    run_loops: bool,
) -> int | None:
    """Shared single-row insert for ``FakeOutboxProducer.publish`` and the ``broker.publish`` patch (P29)."""
    payload, hdrs = _encode_payload(body, headers=headers, correlation_id=correlation_id, serializer=serializer)
    row_id = fake_client.feed(queue=queue, payload=payload, headers=hdrs, next_attempt_at=next_at, timer_id=timer_id)
    _emit_published(broker, queue, count=0 if row_id is None else 1, size_bytes=len(payload))
    if run_loops:
        _notify_subscriber(broker, queue)  # P30: the loops own dispatch; just wake them
    elif row_id is not None:
        # Sync dispatch ignores next_attempt_at (timers fire immediately in test mode);
        # skip only when the insert was a timer-dedup no-op.
        await _sync_dispatch(fake_client, broker, queue, row_id)
    return row_id


async def _fake_publish_many(
    fake_client: FakeOutboxClient,
    broker: OutboxBroker,
    serializer: typing.Any,
    *,
    bodies: "Sequence[typing.Any]",
    queue: str,
    headers: dict[str, str] | None,
    next_at: "_dt.datetime | None",
    run_loops: bool,
) -> None:
    """
    Shared batch insert for both batch paths (P29).

    S5: insert the WHOLE batch, emit ``published``, then dispatch — mirroring production
    (atomic batch INSERT -> published -> subscriber fetch), so a handler never observes a
    half-inserted batch and the event order isn't inverted.
    """
    total_size = 0
    landed_ids: list[int] = []
    for body in bodies:
        payload, hdrs = _encode_payload(body, headers=headers, serializer=serializer)
        total_size += len(payload)
        row_id = fake_client.feed(queue=queue, payload=payload, headers=hdrs, next_attempt_at=next_at)
        if row_id is not None:
            landed_ids.append(row_id)
    _emit_published(broker, queue, count=len(landed_ids), size_bytes=total_size)
    if run_loops:
        _notify_subscriber(broker, queue)
    else:
        for row_id in landed_ids:
            await _sync_dispatch(fake_client, broker, queue, row_id)


class FakeOutboxProducer:
    """
    In-memory ``OutboxProducer`` substitute routing inserts through ``FakeOutboxClient``.

    Used by ``TestOutboxBroker`` so ``broker.publisher(queue).publish(body, session=...)``
    drives the same in-memory fake store as ``broker.publish(body, session=...)``. The
    *session* on the command is ignored — the fake client has no transaction.

    In sync mode (``run_loops=False``), each successful insert short-circuits into
    ``_sync_dispatch`` so handlers run before ``publish`` returns — matches the
    ``broker.publish`` patch in ``_build_fake_publish``.
    """

    _parser: typing.Any = None
    _decoder: typing.Any = None
    # ProducerProto[0.7] requires `codec`. The fake producer ignores it at
    # runtime, same as OutboxProducer.
    codec: typing.Any = None

    def __init__(
        self,
        fake_client: FakeOutboxClient,
        broker: OutboxBroker,
        serializer: typing.Any,
        *,
        run_loops: bool,
    ) -> None:
        self._fake_client = fake_client
        self._broker = broker
        self._serializer = serializer
        self._run_loops = run_loops

    async def publish(self, cmd: OutboxPublishCommand) -> int | None:
        _validate_activate_args("broker.publish", cmd.activate_in, cmd.activate_at)
        next_at = _compute_next_at_client_side(cmd.activate_in, cmd.activate_at)
        return await _fake_publish_one(
            self._fake_client,
            self._broker,
            self._serializer,
            body=cmd.body,
            queue=cmd.queue,
            headers=cmd.headers,
            correlation_id=cmd.correlation_id,
            next_at=next_at,
            timer_id=cmd.timer_id,
            run_loops=self._run_loops,
        )

    async def publish_batch(self, cmd: OutboxPublishCommand) -> None:
        _validate_activate_args("broker.publish_batch", cmd.activate_in, cmd.activate_at)
        next_at = _compute_next_at_client_side(cmd.activate_in, cmd.activate_at)
        await _fake_publish_many(
            self._fake_client,
            self._broker,
            self._serializer,
            bodies=cmd.batch_bodies,
            queue=cmd.queue,
            headers=cmd.headers,
            next_at=next_at,
            run_loops=self._run_loops,
        )

    async def request(self, cmd: OutboxPublishCommand) -> typing.NoReturn:
        raise NotImplementedError(_REQUEST_UNSUPPORTED_MSG)

    def connect(self, connection: typing.Any = None, serializer: typing.Any = None) -> None:  # noqa: ARG002
        if serializer is not None:
            self._serializer = serializer

    def disconnect(self) -> None:
        pass


def _build_fake_publish(
    fake_client: FakeOutboxClient,
    broker: OutboxBroker,
    serializer: typing.Any,
    *,
    run_loops: bool,
) -> typing.Callable[..., typing.Awaitable[int | None]]:
    async def fake_publish(
        body: typing.Any,
        *,
        queue: str,
        session: typing.Any = None,
        headers: dict[str, str] | None = None,
        correlation_id: str | None = None,
        activate_in: _dt.timedelta | None = None,
        activate_at: _dt.datetime | None = None,
        timer_id: str | None = None,
    ) -> int | None:
        # P33: test mode is intentionally lenient about ``session`` — the fake client has
        # no transaction, so any value (including None) is accepted here. This DIVERGES from
        # production and from ``publisher.publish()`` / ``OutboxResponse``, which require a
        # real AsyncSession; tests that assert that contract must use those paths.
        del session
        _validate_activate_args("broker.publish", activate_in, activate_at)
        next_at = _compute_next_at_client_side(activate_in, activate_at)
        return await _fake_publish_one(
            fake_client,
            broker,
            serializer,
            body=body,
            queue=queue,
            headers=headers,
            correlation_id=correlation_id,
            next_at=next_at,
            timer_id=timer_id,
            run_loops=run_loops,
        )

    return fake_publish


def _build_fake_publish_batch(
    fake_client: FakeOutboxClient,
    broker: OutboxBroker,
    serializer: typing.Any,
    *,
    run_loops: bool,
) -> typing.Callable[..., typing.Awaitable[None]]:
    async def fake_publish_batch(
        *bodies: typing.Any,
        queue: str,
        session: typing.Any = None,
        headers: dict[str, str] | None = None,
        activate_in: _dt.timedelta | None = None,
        activate_at: _dt.datetime | None = None,
    ) -> None:
        del session
        _validate_activate_args("broker.publish_batch", activate_in, activate_at)
        if not bodies:
            return
        next_at = _compute_next_at_client_side(activate_in, activate_at)
        await _fake_publish_many(
            fake_client,
            broker,
            serializer,
            bodies=bodies,
            queue=queue,
            headers=headers,
            next_at=next_at,
            run_loops=run_loops,
        )

    return fake_publish_batch


def _build_fake_cancel_timer(
    fake_client: FakeOutboxClient,
) -> typing.Callable[..., typing.Awaitable[bool]]:
    async def fake_cancel_timer(
        *,
        queue: str,
        timer_id: str,
        session: typing.Any = None,
    ) -> bool:
        del session
        return await fake_client.cancel_timer(queue=queue, timer_id=timer_id)

    return fake_cancel_timer


def _build_fake_fetch_unprocessed(
    fake_client: FakeOutboxClient,
) -> typing.Callable[..., typing.Awaitable[list[OutboxInnerMessage]]]:
    async def fake_fetch_unprocessed(
        *,
        session: typing.Any = None,
        queue: str | None = None,
        limit: int = 1000,
    ) -> list[OutboxInnerMessage]:
        del session
        if limit < 1:
            # Mirror the real broker's validation so a non-positive limit can't
            # silently mis-slice (rows[:-1]) under the test broker (F4-04).
            msg = f"limit must be >= 1, got {limit}"
            raise ValueError(msg)
        rows = sorted(fake_client.rows, key=lambda r: r.id)
        if queue is not None:
            rows = [r for r in rows if r.queue == queue]
        # Mirror production's ``limit`` (default 1000) so a valid
        # ``broker.fetch_unprocessed(..., limit=N)`` call doesn't TypeError under
        # the test broker (B16).
        return [_to_inner(r) for r in rows[:limit]]

    return fake_fetch_unprocessed


class TestOutboxBroker(TestBroker[OutboxBroker, OutboxBroker]):  # ty: ignore[invalid-type-arguments]
    """
    Test harness for ``OutboxBroker``. Two dispatch modes.

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
        self.fake_client = FakeOutboxClient()
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
        next_attempt_at: _dt.datetime | None = None,
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
    def _patch_producer(self, broker: OutboxBroker) -> "Iterator[None]":
        # Swap the broker's producer slot for one that routes inserts through
        # the in-memory fake client. ``OutboxPublisher.publish`` flows through
        # ``_basic_publish(cmd, producer=...)``, so replacing the producer is
        # how publisher.publish() lands rows in the fake store. ``broker.publish``
        # is patched separately to bypass the producer — that path is the
        # canonical sync-dispatch entry point and matches existing tests.
        serializer = broker.config.broker_config.fd_config._serializer  # noqa: SLF001
        fake_producer = FakeOutboxProducer(
            self.fake_client,
            broker,
            serializer,
            run_loops=self.run_loops,
        )
        original_producer = broker.config.broker_config.producer
        broker.config.broker_config.producer = fake_producer
        try:
            yield
        finally:
            broker.config.broker_config.producer = original_producer

    @contextmanager
    def _patch_broker(self, broker: OutboxBroker) -> "Iterator[None]":
        original_client = broker.config.broker_config.client
        broker.config.broker_config.client = self.fake_client
        # Mirror real publish's serializer wiring so pydantic / dataclass bodies
        # encode identically in tests.
        serializer = broker.config.broker_config.fd_config._serializer  # noqa: SLF001
        fake_publish = _build_fake_publish(self.fake_client, broker, serializer, run_loops=self.run_loops)
        fake_publish_batch = _build_fake_publish_batch(self.fake_client, broker, serializer, run_loops=self.run_loops)
        fake_cancel_timer = _build_fake_cancel_timer(self.fake_client)
        fake_fetch_unprocessed = _build_fake_fetch_unprocessed(self.fake_client)
        try:
            with (
                mock.patch.object(broker, "publish", new=fake_publish),
                mock.patch.object(broker, "publish_batch", new=fake_publish_batch),
                mock.patch.object(broker, "cancel_timer", new=fake_cancel_timer),
                mock.patch.object(broker, "fetch_unprocessed", new=fake_fetch_unprocessed),
                super()._patch_broker(broker),
            ):
                yield
        finally:
            broker.config.broker_config.client = original_client

    def create_publisher_fake_subscriber(  # pragma: no cover
        self,
        broker: OutboxBroker,
        publisher: typing.Any,
    ) -> tuple["OutboxSubscriber", bool]:
        # Required by FastStream's TestBroker abstract base, but never called —
        # we skip the publisher fake-subscriber loop in ``_fake_start`` because
        # ``FakeOutboxProducer`` already lands rows in the fake client AND drives
        # the real subscriber via ``_sync_dispatch``. The FastStream
        # publisher-spy infrastructure would mock the real handler and break that.
        del self, broker, publisher
        msg = "TestOutboxBroker handles publisher dispatch via FakeOutboxProducer; this is unreachable."
        raise NotImplementedError(msg)

    def _fake_start(self, broker: OutboxBroker, *args: typing.Any, **kwargs: typing.Any) -> None:
        del args, kwargs
        # Skip the parent's publisher iteration — see ``create_publisher_fake_subscriber``
        # for why. We still need to fan out ``_post_start`` on subscribers so their
        # call models build (matches what TestBroker._fake_start does last).
        patch_broker_calls(broker)  # ty: ignore[invalid-argument-type]
        for subscriber in broker.subscribers:
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
            self._loops_spawned = False
        super()._fake_close(broker, *args, **kwargs)

    async def _fake_connect(self, broker: OutboxBroker, *args: typing.Any, **kwargs: typing.Any) -> None:  # noqa: ARG002
        return
