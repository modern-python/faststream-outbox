"""
Broker config for the outbox transport.

The user owns the ``AsyncEngine``; the broker never closes it. The engine is
stored on ``OutboxBrokerConfig`` so the broker can hand the same reference to its
client and to subscribers.
"""

import typing
from dataclasses import dataclass, field

from faststream._internal.configs import BrokerConfig

from faststream_outbox.metrics import MetricsRecorder, _noop_recorder


if typing.TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine

    from faststream_outbox.client import AbstractOutboxClient


@dataclass(kw_only=True)
class OutboxBrokerConfig(BrokerConfig):
    engine: "AsyncEngine | None" = None
    client: "AbstractOutboxClient | None" = None
    metrics_recorder: MetricsRecorder = field(default=_noop_recorder)

    async def connect(self) -> None:
        # Engine and client are wired up by the broker's constructor; nothing to do here.
        pass

    async def disconnect(self) -> None:
        # Caller owns the engine — never dispose it here.
        pass
