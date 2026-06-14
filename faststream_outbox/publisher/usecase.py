"""
``OutboxPublisher`` — typed, queue-scoped handle around ``broker.publish``.

The publisher is intentionally **not** usable as a relay decorator on a
subscriber (``__call__`` raises). The dispatch flow that would invoke
``_publish`` has no reachable ``AsyncSession`` without breaking the outbox's
transactional contract (row commits with the caller's domain writes). Users
wanting outbox→outbox chaining should call ``broker.publish(...)`` inside their
handler on the same session that owns the inbound row's terminal write.
"""

import datetime as _dt
import typing
from typing import override

from faststream._internal.endpoint.publisher import PublisherUsecase
from sqlalchemy.ext.asyncio import AsyncSession

from faststream_outbox.publisher.config import OutboxPublisherConfig
from faststream_outbox.publisher.specification import OutboxPublisherSpecification
from faststream_outbox.response import OutboxPublishCommand


if typing.TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from faststream._internal.basic_types import SendableMessage
    from faststream._internal.endpoint.call_wrapper import HandlerCallWrapper
    from faststream._internal.types import P_HandlerParams, PublisherMiddleware, T_HandlerReturn
    from faststream.response.response import PublishCommand

    from faststream_outbox.configs import OutboxBrokerConfig


_REJECT_RELAY_MSG = (
    "OutboxPublisher cannot decorate a subscriber handler — relay chaining is not supported. "
    "Call `await broker.publish(value, queue=..., session=session)` inside your handler instead, "
    "reusing the session that owns the inbound row's terminal write."
)


class OutboxPublisher(PublisherUsecase):
    """Queue-scoped publisher. Standalone use only — no relay decorator support."""

    _outer_config: "OutboxBrokerConfig"

    def __init__(
        self,
        config: OutboxPublisherConfig,
        specification: OutboxPublisherSpecification,
    ) -> None:
        super().__init__(config, specification)  # ty: ignore[invalid-argument-type]
        self.config = config
        self._queue = config.queue
        self.headers = config.headers or {}

    @property
    def queue(self) -> str:
        return self._queue

    @override
    def __call__(
        self,
        func: "Callable[P_HandlerParams, T_HandlerReturn]",
    ) -> "HandlerCallWrapper[P_HandlerParams, T_HandlerReturn]":
        raise NotImplementedError(_REJECT_RELAY_MSG)

    async def publish(  # ty: ignore[invalid-method-override]
        self,
        body: typing.Any,  # parity with broker.publish (both build OutboxPublishCommand(body: Any))
        *,
        session: AsyncSession,
        headers: dict[str, str] | None = None,
        correlation_id: str | None = None,
        activate_in: _dt.timedelta | None = None,
        activate_at: _dt.datetime | None = None,
        timer_id: str | None = None,
    ) -> int | None:
        """
        Insert one outbox row scoped to this publisher's queue.

        Same transactional contract as :meth:`OutboxBroker.publish`: runs on the
        caller's session and commits with their transaction. Static *headers* on
        the publisher are merged with per-call headers (per-call wins on conflict).

        Returns the inserted row's id, or ``None`` on a timer_id conflict.
        """
        merged_headers = {**self.headers, **(headers or {})}
        cmd = OutboxPublishCommand(
            body,
            queue=self._queue,
            session=session,
            headers=merged_headers,
            correlation_id=correlation_id,
            activate_in=activate_in,
            activate_at=activate_at,
            timer_id=timer_id,
        )
        result = await self._basic_publish(
            cmd,
            producer=self._outer_config.producer,
            _extra_middlewares=(),
        )
        return typing.cast("int | None", result)

    @override
    async def _publish(
        self,
        cmd: "PublishCommand",
        *,
        _extra_middlewares: "Iterable[PublisherMiddleware]",
    ) -> None:
        # Unreachable in normal use — __call__ raises so FastStream's dispatch
        # loop never attaches this publisher to a handler. Kept for protocol
        # completeness with a clear failure if someone bypasses __call__.
        raise NotImplementedError(_REJECT_RELAY_MSG)

    @override
    async def request(
        self,
        message: "SendableMessage" = None,
        /,
        *,
        correlation_id: str | None = None,
    ) -> typing.NoReturn:
        msg = "OutboxBroker does not support request-reply"
        raise NotImplementedError(msg)
