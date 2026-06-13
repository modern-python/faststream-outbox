"""
Publish-side DTO for the outbox transport.

``OutboxPublishCommand`` carries the domain fields that travel from the
publisher / broker into ``OutboxProducer.publish``: the user's ``AsyncSession``
(required — outbox rows commit with the caller's transaction), the queue name,
scheduling args, the timer dedup key. Centralizing the activate-args validation
in the constructor ensures ``broker.publish``, ``broker.publish_batch`` and
``OutboxPublisher.publish`` all reject the same misconfigurations.

The relay path (``@publisher`` decorating a subscriber) is not supported, so
``from_cmd`` raises — see ``OutboxPublisher.__call__`` for the rationale.
"""

import datetime as _dt
import typing

from faststream.response.publish_type import PublishType
from faststream.response.response import BatchPublishCommand, PublishCommand, Response
from sqlalchemy.ext.asyncio import AsyncSession


# Matches the ``queue`` column width in ``make_outbox_table`` (``String(255)``).
_MAX_QUEUE_LENGTH = 255


class OutboxPublishCommand(BatchPublishCommand):
    """Outbox-specific publish command: carries session + scheduling fields end-to-end."""

    def __init__(
        self,
        body: typing.Any,
        /,
        *bodies: typing.Any,
        queue: str,
        session: AsyncSession,
        headers: dict[str, str] | None = None,
        correlation_id: str | None = None,
        activate_in: _dt.timedelta | None = None,
        activate_at: _dt.datetime | None = None,
        timer_id: str | None = None,
        _publish_type: PublishType = PublishType.PUBLISH,
    ) -> None:
        if not isinstance(session, AsyncSession):
            msg = "OutboxPublishCommand requires an sqlalchemy.ext.asyncio.AsyncSession"
            raise TypeError(msg)
        if activate_in is not None and activate_at is not None:
            msg = "OutboxPublishCommand accepts at most one of activate_in / activate_at"
            raise ValueError(msg)
        if activate_at is not None and activate_at.tzinfo is None:
            msg = "OutboxPublishCommand requires activate_at to be timezone-aware"
            raise ValueError(msg)
        # P5: ``queue`` reaches SQL unvalidated — empty / non-str / over the
        # ``String(255)`` column would surface as an opaque DB error (or silent
        # truncation). Reject at the single-source-of-truth constructor instead.
        if not isinstance(queue, str):
            msg = f"queue must be a str, got {type(queue).__name__}"
            raise TypeError(msg)
        if not queue:
            msg = "queue must be a non-empty string"
            raise ValueError(msg)
        if len(queue) > _MAX_QUEUE_LENGTH:
            msg = f"queue must be at most {_MAX_QUEUE_LENGTH} characters (got {len(queue)})"
            raise ValueError(msg)
        # P4: timer_id / correlation_id are per-row single-publish concepts the batch
        # path silently drops (each batched row gets its own auto correlation_id and
        # no dedup key). Reject them on a batch command rather than accept-and-ignore.
        if bodies and (timer_id is not None or correlation_id is not None):
            msg = "timer_id / correlation_id are not supported for batch publishes (multiple bodies)"
            raise ValueError(msg)
        super().__init__(
            body,
            *bodies,
            _publish_type=_publish_type,
            destination=queue,
            correlation_id=correlation_id,
            headers=headers,
        )
        self.session = session
        self.activate_in = activate_in
        self.activate_at = activate_at
        self.timer_id = timer_id

    @property
    def queue(self) -> str:
        return self.destination

    @property
    def batch_bodies(self) -> tuple[typing.Any, ...]:
        # Upstream's PublishCommand.batch_bodies drops ``self.body`` when it is
        # None, so a leading (or sole) None body silently vanishes from the batch
        # — a lost row with no error and no metric. The outbox treats None as a
        # valid body (``publish(None)`` inserts ``b""``), so every positional body
        # must survive, in order. ``OutboxPublishCommand`` is the single source of
        # truth, so overriding here fixes the producer, the fake producer, and the
        # OpenTelemetry batch-count attribute in one place.
        return (self.body, *self.extra_bodies)

    @classmethod
    def from_cmd(
        cls,
        cmd: "PublishCommand",
        *,
        batch: bool = False,
    ) -> "OutboxPublishCommand":
        # The relay path (handler returns a value → publisher._publish) cannot
        # source an AsyncSession from FastStream's dispatch flow without breaking
        # the outbox transactional contract, so relay chaining is rejected at
        # decoration time in OutboxPublisher.__call__. This adapter therefore has
        # no legitimate caller — make the failure mode explicit.
        del cmd, batch
        msg = (
            "OutboxPublishCommand.from_cmd is not supported — relay chaining is rejected at "
            "decoration time. Construct OutboxPublishCommand directly with an AsyncSession."
        )
        raise NotImplementedError(msg)


class OutboxResponse(Response):
    """
    Handler return type — auto-published as a follow-on outbox row.

    Idiomatic FastStream shape: ``async def h(...) -> OutboxResponse``. Requires
    ``session=...`` for the same reason ``broker.publish`` does — the new row must
    commit with the caller's domain writes. Validation (session type, activate
    args mutex, tz-aware datetime) is deferred to ``OutboxPublishCommand.__init__``
    on ``as_publish_command()`` so there's a single source of truth.

    ``correlation_id`` defaults to the inbound message's correlation_id when not set
    — FastStream's ``SubscriberUsecase.process_message`` does the inheritance before
    publishing the response.
    """

    def __init__(
        self,
        body: typing.Any,
        *,
        queue: str,
        session: AsyncSession,
        headers: dict[str, str] | None = None,
        correlation_id: str | None = None,
        activate_in: _dt.timedelta | None = None,
        activate_at: _dt.datetime | None = None,
        timer_id: str | None = None,
    ) -> None:
        super().__init__(body=body, headers=headers, correlation_id=correlation_id)
        self.queue = queue
        self.session = session
        self.activate_in = activate_in
        self.activate_at = activate_at
        self.timer_id = timer_id

    def as_publish_command(self) -> OutboxPublishCommand:
        return OutboxPublishCommand(
            self.body,
            queue=self.queue,
            session=self.session,
            headers=self.headers,
            correlation_id=self.correlation_id,
            activate_in=self.activate_in,
            activate_at=self.activate_at,
            timer_id=self.timer_id,
        )
