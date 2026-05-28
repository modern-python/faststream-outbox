"""
Public surface for the outbox FastAPI integration.

Mirrors ``faststream.kafka.fastapi``: the router and the ``Annotated[..., Context(...)]``
shortcuts handlers will reference. Import from this module when wiring an outbox
into a FastAPI app::

    from faststream_outbox.fastapi import OutboxRouter, OutboxBroker, OutboxMessage

    router = OutboxRouter(engine, outbox_table=t)

    @router.subscriber("orders")
    async def handle(msg: OutboxMessage, broker: OutboxBroker) -> None: ...
"""

from typing import Annotated

from faststream._internal.fastapi.context import Context, ContextRepo, Logger

from faststream_outbox.broker import OutboxBroker as _OutboxBrokerCls
from faststream_outbox.client import AbstractOutboxClient as _AbstractOutboxClient
from faststream_outbox.message import OutboxMessage as _OutboxMessageCls
from faststream_outbox.publisher.producer import OutboxProducer as _OutboxProducerCls
from .router import OutboxRouter


__all__ = (
    "Context",
    "ContextRepo",
    "Logger",
    "OutboxBroker",
    "OutboxClient",
    "OutboxMessage",
    "OutboxProducer",
    "OutboxRouter",
)


OutboxMessage = Annotated[_OutboxMessageCls, Context("message")]
OutboxBroker = Annotated[_OutboxBrokerCls, Context("broker")]
# ``broker._producer`` is a property on ``BrokerUsecase`` returning ``self.config.producer``.
OutboxProducer = Annotated[_OutboxProducerCls, Context("broker._producer")]
# The client lives only on ``OutboxBrokerConfig`` (not as a property on the broker),
# so we point at it directly.
OutboxClient = Annotated[_AbstractOutboxClient, Context("broker.config.broker_config.client")]
