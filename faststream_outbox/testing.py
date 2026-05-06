"""
Test broker with an in-memory ``OutboxClient`` substitute.

``TestOutboxBroker`` wraps an ``OutboxBroker`` and swaps in a ``FakeOutboxClient``
backed by a list of dicts. The real ``OutboxSubscriber`` runs unmodified — same
fetch / worker / release-stuck loops — so tests exercise the actual delivery
path, not a shortcut. ``feed()`` simulates a row insert.
"""

import datetime as _dt
import typing
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field

from faststream._internal.testing.broker import TestBroker

from faststream_outbox.broker import OutboxBroker
from faststream_outbox.message import OutboxInnerMessage
from faststream_outbox.schema import OutboxState


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
    state: str = OutboxState.PENDING.value
    attempts_count: int = 0
    deliveries_count: int = 0
    created_at: _dt.datetime = field(default_factory=_utcnow)
    next_attempt_at: _dt.datetime = field(default_factory=_utcnow)
    first_attempt_at: _dt.datetime | None = None
    last_attempt_at: _dt.datetime | None = None
    acquired_at: _dt.datetime | None = None
    acquired_token: uuid.UUID | None = None


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
    ) -> int:
        row = _FakeRow(
            id=self._next_id,
            queue=queue,
            payload=payload,
            headers=headers,
            next_attempt_at=next_attempt_at or _utcnow(),
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

    async def fetch(self, queues: "Sequence[str]", *, limit: int) -> list[OutboxInnerMessage]:
        if not queues:
            return []
        now = _utcnow()
        token = uuid.uuid4()
        out: list[OutboxInnerMessage] = []
        eligible = sorted(
            (
                r
                for r in self._rows
                if r.state == OutboxState.PENDING.value and r.queue in queues and r.next_attempt_at <= now
            ),
            key=lambda r: r.next_attempt_at,
        )
        for row in eligible[:limit]:
            row.state = OutboxState.PROCESSING.value
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
        next_attempt_at: _dt.datetime,
        attempts_count: int,
        first_attempt_at: _dt.datetime,
        last_attempt_at: _dt.datetime,
    ) -> bool:
        for row in self._rows:
            if row.id == message_id and row.acquired_token == acquired_token:
                row.state = OutboxState.PENDING.value
                row.next_attempt_at = next_attempt_at
                row.attempts_count = attempts_count
                row.first_attempt_at = first_attempt_at
                row.last_attempt_at = last_attempt_at
                row.acquired_at = None
                row.acquired_token = None
                return True
        return False

    async def release_stuck(self, *, timeout_seconds: float) -> int:
        cutoff = _utcnow() - _dt.timedelta(seconds=timeout_seconds)
        released = 0
        for row in self._rows:
            if row.state == OutboxState.PROCESSING.value and row.acquired_at is not None and row.acquired_at < cutoff:
                row.state = OutboxState.PENDING.value
                row.acquired_at = None
                row.acquired_token = None
                released += 1
        return released

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
        state=OutboxState(row.state),
        attempts_count=row.attempts_count,
        deliveries_count=row.deliveries_count,
        created_at=row.created_at,
        next_attempt_at=row.next_attempt_at,
        first_attempt_at=row.first_attempt_at,
        last_attempt_at=row.last_attempt_at,
        acquired_at=row.acquired_at,
        acquired_token=row.acquired_token,
    )


class TestOutboxBroker(TestBroker[OutboxBroker]):  # ty: ignore[invalid-type-arguments]
    """Test harness that runs the real subscriber loops against an in-memory client."""

    fake_client: FakeOutboxClient

    def __init__(self, broker: OutboxBroker, **kwargs: typing.Any) -> None:
        super().__init__(broker, **kwargs)
        self.fake_client = FakeOutboxClient()

    def feed(
        self,
        queue: str,
        payload: bytes,
        *,
        headers: dict[str, str] | None = None,
        next_attempt_at: _dt.datetime | None = None,
    ) -> int:
        """Insert a row directly into the in-memory store. Returns the row id."""
        return self.fake_client.feed(
            queue=queue,
            payload=payload,
            headers=headers,
            next_attempt_at=next_attempt_at,
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
        try:
            with super()._patch_broker(broker):
                yield
        finally:
            broker.config.broker_config.client = original_client

    def _fake_start(self, broker: OutboxBroker, *args: typing.Any, **kwargs: typing.Any) -> None:

        # Run the parent _fake_start (sets up publisher fakes, calls _post_start, etc.)
        super()._fake_start(broker, *args, **kwargs)
        # Then spin up the real subscriber loops against the in-memory fake client. Without this,
        # ``feed()`` would drop rows on the floor — there's no producer to fall back to.
        for raw_subscriber in broker.subscribers:
            sub = typing.cast("OutboxSubscriber", raw_subscriber)
            for _ in range(sub._config.max_workers):  # noqa: SLF001
                sub.add_task(sub._worker_loop)  # noqa: SLF001
            sub.add_task(sub._fetch_loop)  # noqa: SLF001
            sub.add_task(sub._release_stuck_loop)  # noqa: SLF001

    async def _fake_connect(self, broker: OutboxBroker, *args: typing.Any, **kwargs: typing.Any) -> None:  # noqa: ARG002
        return
