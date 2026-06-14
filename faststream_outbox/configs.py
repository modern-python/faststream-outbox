"""
Broker config for the outbox transport.

The user owns the ``AsyncEngine``; the broker never closes it. The engine is
stored on ``OutboxBrokerConfig`` so the broker can hand the same reference to its
client and to subscribers.
"""

import typing
from collections.abc import Callable
from dataclasses import dataclass

from faststream._internal.configs import BrokerConfig

from faststream_outbox.metrics import MetricsRecorder, _noop_recorder


if typing.TYPE_CHECKING:
    from sqlalchemy import Table
    from sqlalchemy.ext.asyncio import AsyncEngine

    from faststream_outbox.client import AbstractOutboxClient


# Renders a handler exception into the DLQ ``last_exception`` column. Returning ``None``
# stores nothing. Default (config value ``None``) renders ``repr(exc)``.
LastExceptionRenderer = Callable[[BaseException], "str | None"]


@dataclass(kw_only=True)
class OutboxBrokerConfig(BrokerConfig):
    engine: "AsyncEngine | None" = None
    client: "AbstractOutboxClient | None" = None
    metrics_recorder: MetricsRecorder = _noop_recorder
    # When non-None, terminal failures (max_deliveries / retry_terminal / rejected)
    # copy audit data into this table in the same statement as the outbox DELETE.
    # See ``OutboxClient.delete_with_lease`` for the CTE shape.
    dlq_table: "Table | None" = None
    # F3-01: opt-in transform for the DLQ ``last_exception`` column. None → ``repr(exc)``
    # (full forensic detail). Set it to redact PII/secrets or drop the detail entirely.
    last_exception_renderer: LastExceptionRenderer | None = None
    # P24: the former connect()/disconnect() overrides were dead code — BrokerConfig has
    # no such hooks and nothing in the package or upstream called them. The broker's own
    # start()/connect() wire the engine/client; the caller owns the engine lifecycle.
