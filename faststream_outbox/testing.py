"""
Test broker with an in-memory ``OutboxClient`` substitute.

``TestOutboxBroker`` wraps an ``OutboxBroker`` and swaps in a ``FakeOutboxClient``
backed by a list of dicts. Defaults to **sync dispatch**: ``await broker.publish(...)``
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

from faststream._internal.testing.broker import TestBroker

from faststream_outbox.broker import OutboxBroker
from faststream_outbox.envelope import _encode_payload
from faststream_outbox.message import OutboxInnerMessage


if typing.TYPE_CHECKING:
    from collections.abc import Iterator, Sequence

    from faststream_outbox.subscriber.usecase import OutboxSubscriber


def _utcnow() -> _dt.datetime:
    return _dt.datetime.now(tz=_dt.UTC)


@dataclass
class _FakeRow:
    id: int
    queue: str
    payload: bytes
    headers: dict[str, str] | None
    attempts_count: int = 0
    deliveries_count: int = 0
    created_at: _dt.datetime = field(default_factory=_utcnow)
    next_attempt_at: _dt.datetime = field(default_factory=_utcnow)
    first_attempt_at: _dt.datetime | None = None
    last_attempt_at: _dt.datetime | None = None
    acquired_at: _dt.datetime | None = None
    acquired_token: uuid.UUID | None = None
    timer_id: str | None = None


class FakeOutboxClient:
    """In-memory ``OutboxClient`` substitute. Same surface, list-of-rows storage."""

    def __init__(self) -> None:
        self._rows: list[_FakeRow] = []
        self._next_id = 1

    def feed(
        self,
        *,
        queue: str,
        payload: bytes,
        headers: dict[str, str] | None = None,
        next_attempt_at: _dt.datetime | None = None,
        timer_id: str | None = None,
    ) -> int | None:
        # Mirror the real client's partial-unique-on-(queue, timer_id) behavior:
        # re-feeding a timer that already exists is a no-op.
        if timer_id is not None and any(r.queue == queue and r.timer_id == timer_id for r in self._rows):
            return None
        row = _FakeRow(
            id=self._next_id,
            queue=queue,
            payload=payload,
            headers=headers,
            next_attempt_at=next_attempt_at or _utcnow(),
            timer_id=timer_id,
        )
        self._rows.append(row)
        self._next_id += 1
        return row.id

    @property
    def rows(self) -> list[_FakeRow]:
        return self._rows

    @property
    def table(self) -> typing.Any:
        return None

    @property
    def engine(self) -> None:
        """No real engine — signals the subscriber loop to use the polling-only path."""
        return None

    async def fetch_with_conn(
        self,
        conn: typing.Any,  # noqa: ARG002
        queues: "Sequence[str]",
        *,
        limit: int,
        lease_ttl_seconds: float,
    ) -> list[OutboxInnerMessage]:
        """Mirror :meth:`OutboxClient.fetch_with_conn`; *conn* is ignored by the fake."""
        return await self.fetch(queues, limit=limit, lease_ttl_seconds=lease_ttl_seconds)

    async def fetch(
        self,
        queues: "Sequence[str]",
        *,
        limit: int,
        lease_ttl_seconds: float,
    ) -> list[OutboxInnerMessage]:
        if not queues:
            return []
        now = _utcnow()
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
            key=lambda r: r.next_attempt_at,
        )
        for row in eligible[:limit]:
            row.acquired_at = now
            row.acquired_token = token
            row.deliveries_count += 1
            out.append(_to_inner(row))
        return out

    async def delete_with_lease(self, message_id: int, acquired_token: uuid.UUID) -> bool:
        for i, row in enumerate(self._rows):
            if row.id == message_id and row.acquired_token == acquired_token:
                del self._rows[i]
                return True
        return False

    async def mark_pending_with_lease(  # noqa: PLR0913
        self,
        message_id: int,
        acquired_token: uuid.UUID,
        *,
        delay_seconds: float,
        attempts_count: int,
        first_attempt_at: _dt.datetime,
        last_attempt_at: _dt.datetime,
    ) -> bool:
        for row in self._rows:
            if row.id == message_id and row.acquired_token == acquired_token:
                row.next_attempt_at = _utcnow() + _dt.timedelta(seconds=max(0.0, delay_seconds))
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
        return

    async def ping(self) -> bool:
        return True


def _to_inner(row: _FakeRow) -> OutboxInnerMessage:
    return OutboxInnerMessage(
        id=row.id,
        queue=row.queue,
        payload=row.payload,
        headers=row.headers,
        attempts_count=row.attempts_count,
        deliveries_count=row.deliveries_count,
        created_at=row.created_at,
        next_attempt_at=row.next_attempt_at,
        first_attempt_at=row.first_attempt_at,
        last_attempt_at=row.last_attempt_at,
        acquired_at=row.acquired_at,
        acquired_token=row.acquired_token,
    )


def _find_subscriber_for_queue(broker: OutboxBroker, queue: str) -> "OutboxSubscriber | None":
    """First matching subscriber wins — mirrors production fetch behavior for overlapping subscribers."""
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
    fake_row.acquired_at = _utcnow()
    fake_row.deliveries_count += 1
    await subscriber.dispatch_one(_to_inner(fake_row))


def _build_fake_publish(
    fake_client: FakeOutboxClient,
    broker: OutboxBroker,
    serializer: typing.Any,
    *,
    run_loops: bool,
) -> typing.Callable[..., typing.Awaitable[int | None]]:
    async def fake_publish(  # noqa: PLR0913
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
        # session is ignored in test mode — the fake client has no transaction.
        del session
        if activate_in is not None and activate_at is not None:
            msg = "broker.publish accepts at most one of activate_in / activate_at"
            raise ValueError(msg)
        payload, hdrs = _encode_payload(
            body,
            headers=headers,
            correlation_id=correlation_id,
            serializer=serializer,
        )
        next_at: _dt.datetime | None = None
        if activate_in is not None:
            next_at = _utcnow() + activate_in
        elif activate_at is not None:
            next_at = activate_at
        row_id = fake_client.feed(
            queue=queue,
            payload=payload,
            headers=hdrs,
            next_attempt_at=next_at,
            timer_id=timer_id,
        )
        # Sync dispatch ignores next_attempt_at — timers fire immediately in test mode.
        # Skip only when loop mode is on (loops would re-dispatch) or the insert was a
        # timer-dedup no-op.
        if not run_loops and row_id is not None:
            await _sync_dispatch(fake_client, broker, queue, row_id)
        return row_id

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
        if activate_in is not None and activate_at is not None:
            msg = "broker.publish_batch accepts at most one of activate_in / activate_at"
            raise ValueError(msg)
        if not bodies:
            return
        next_at: _dt.datetime | None = None
        if activate_in is not None:
            next_at = _utcnow() + activate_in
        elif activate_at is not None:
            next_at = activate_at
        for body in bodies:
            payload, hdrs = _encode_payload(body, headers=headers, serializer=serializer)
            row_id = fake_client.feed(
                queue=queue,
                payload=payload,
                headers=hdrs,
                next_attempt_at=next_at,
            )
            if not run_loops and row_id is not None:
                await _sync_dispatch(fake_client, broker, queue, row_id)

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
    ) -> list[OutboxInnerMessage]:
        del session
        rows = sorted(fake_client.rows, key=lambda r: r.id)
        if queue is not None:
            rows = [r for r in rows if r.queue == queue]
        return [_to_inner(r) for r in rows]

    return fake_fetch_unprocessed


class TestOutboxBroker(TestBroker[OutboxBroker]):  # ty: ignore[invalid-type-arguments]
    """
    Test harness for ``OutboxBroker``. Two dispatch modes.

    Default (``run_loops=False``): ``broker.publish`` synchronously drives the matching
    subscriber's consume pipeline, so handlers run before ``publish`` returns. Matches the
    FastStream test-broker idiom — ``TestKafkaBroker`` / ``TestRabbitBroker`` behave the
    same way. Future-dated rows (``activate_in`` / ``activate_at``) stay in the fake
    client and are *not* dispatched, mirroring production where they wait for the gate.

    Pass ``run_loops=True`` to spin up the real ``_fetch_loop`` / ``_worker_loop`` against
    the in-memory client. Required for tests that exercise loop-driven behavior:
    retry rescheduling, lease expiry reclaim, or fetch-loop error recovery.
    """

    fake_client: FakeOutboxClient
    run_loops: bool

    def __init__(self, broker: OutboxBroker, *, run_loops: bool = False, **kwargs: typing.Any) -> None:
        super().__init__(broker, **kwargs)
        self.fake_client = FakeOutboxClient()
        self.run_loops = run_loops

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
        return self.fake_client.feed(
            queue=queue,
            payload=payload,
            headers=headers,
            next_attempt_at=next_attempt_at,
            timer_id=timer_id,
        )

    @contextmanager
    def _patch_producer(self, broker: OutboxBroker) -> "Iterator[None]":
        # OutboxBroker has no producer to patch.
        del broker
        yield

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

    def _fake_start(self, broker: OutboxBroker, *args: typing.Any, **kwargs: typing.Any) -> None:
        # Run the parent _fake_start (sets up publisher fakes, calls _post_start, etc.)
        super()._fake_start(broker, *args, **kwargs)
        # In sync mode, publish drives dispatch directly — don't spawn the loops.
        if not self.run_loops:
            return
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

    async def _fake_connect(self, broker: OutboxBroker, *args: typing.Any, **kwargs: typing.Any) -> None:  # noqa: ARG002
        return
