"""
Broker config for the outbox transport.

The user owns the ``AsyncEngine``; the broker never closes it. The engine is
stored on ``OutboxBrokerConfig`` so the broker can hand the same reference to its
client and to subscribers.
"""

import typing
from dataclasses import dataclass

from faststream._internal.configs import BrokerConfig

from faststream_outbox.metrics import MetricsRecorder, _noop_recorder


if typing.TYPE_CHECKING:
    from sqlalchemy import Table
    from sqlalchemy.ext.asyncio import AsyncEngine

    from faststream_outbox.client import AbstractOutboxClient


@dataclass(kw_only=True)
class OutboxBrokerConfig(BrokerConfig):
    engine: "AsyncEngine | None" = None
    client: "AbstractOutboxClient | None" = None
    metrics_recorder: MetricsRecorder = _noop_recorder
    # When non-None, terminal failures (max_deliveries / retry_terminal / rejected)
    # copy audit data into this table in the same statement as the outbox DELETE.
    # See ``OutboxClient.delete_with_lease`` for the CTE shape.
    dlq_table: "Table | None" = None

    async def connect(self) -> None:
        # Engine and client are wired up by the broker's constructor; nothing to do here.
        pass

    async def disconnect(self) -> None:
        # Caller owns the engine — never dispose it here.
        pass
