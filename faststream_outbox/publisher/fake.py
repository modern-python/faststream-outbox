"""
Internal response publisher for handlers that ``return OutboxResponse(...)``.

Wired up by ``OutboxSubscriber._make_response_publisher``. The
``isinstance(cmd, OutboxPublishCommand)`` gate is load-bearing: plain handler
returns (``None`` / ``dict`` / etc.) produce a generic ``PublishCommand``
without an ``AsyncSession`` and would explode in ``OutboxProducer.publish`` —
the gate makes them silent no-ops.

Duck-typed against FastStream's ``PublisherProto`` (only ``_publish`` is
called from ``SubscriberUsecase.process_message``). Native brokers extend
``FakePublisher`` for the ``publish``/``request`` ``NotImplementedError``
boilerplate, but those methods are unreachable here — the outbox flow never
calls them — so we skip the inheritance.
"""

import typing
from collections.abc import Iterable
from functools import partial

from faststream.response.publish_type import PublishType

from faststream_outbox.response import OutboxPublishCommand


if typing.TYPE_CHECKING:
    from faststream._internal.basic_types import AsyncFunc
    from faststream._internal.producer import ProducerProto
    from faststream._internal.types import PublisherMiddleware
    from faststream.response.response import PublishCommand


class OutboxFakePublisher:
    """Response publisher used when a handler returns ``OutboxResponse``."""

    def __init__(self, producer: "ProducerProto[OutboxPublishCommand]") -> None:
        self._producer = producer

    async def _publish(
        self,
        cmd: "PublishCommand",
        *,
        _extra_middlewares: Iterable["PublisherMiddleware"],
    ) -> typing.Any:
        if not isinstance(cmd, OutboxPublishCommand):
            return None
        cmd.publish_type = PublishType.REPLY
        # ty diagnostic: ``producer.publish`` narrows to ``OutboxPublishCommand``
        # while ``PublisherMiddleware`` is generic over the base ``PublishCommand``.
        # Contravariance makes this safe at runtime; ty's union inference doesn't
        # see it. Matches FastStream's own native ``FakePublisher._publish`` shape.
        call: AsyncFunc = self._producer.publish
        for m in _extra_middlewares:
            call = partial(m, call)  # ty: ignore[invalid-argument-type]
        return await call(cmd)
