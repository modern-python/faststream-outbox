"""
Annotated context shortcuts for handler signatures.

Mirrors the native FastStream convention (see ``faststream.kafka.annotations``):
the class names are imported with ``as`` aliases and re-exported as
``Annotated[..., Context(...)]`` so handler signatures stay short and idiomatic::

    from faststream_outbox.annotations import OutboxBroker, OutboxMessage

    @broker.subscriber("orders")
    async def handle(msg: OutboxMessage, broker: OutboxBroker) -> None: ...

The Annotated aliases live here; the plain class names (used at construction
time, ``isinstance`` checks, etc.) stay in their own modules and are exposed
from the package root.
"""

from typing import Annotated

from faststream._internal.context import Context
from faststream.annotations import ContextRepo, Logger
from faststream.params import NoCast

from faststream_outbox.broker import OutboxBroker as _OutboxBroker
from faststream_outbox.client import AbstractOutboxClient as _AbstractOutboxClient
from faststream_outbox.message import OutboxMessage as _OutboxMessage
from faststream_outbox.publisher.producer import OutboxProducer as _OutboxProducer


__all__ = (
    "ContextRepo",
    "Logger",
    "NoCast",
    "OutboxBroker",
    "OutboxClient",
    "OutboxMessage",
    "OutboxProducer",
)


OutboxMessage = Annotated[_OutboxMessage, Context("message")]
OutboxBroker = Annotated[_OutboxBroker, Context("broker")]
# ``broker._producer`` resolves via the ``BrokerUsecase._producer`` property
# (returns ``self.config.producer``) — same path Kafka's annotations use.
OutboxProducer = Annotated[_OutboxProducer, Context("broker._producer")]
# The client lives only on the outbox-specific config layer — not exposed as a
# property on the broker — so we point at it directly.
OutboxClient = Annotated[_AbstractOutboxClient, Context("broker.config.broker_config.client")]
