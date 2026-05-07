"""
Broker config + engine wrapper.

The user owns the ``AsyncEngine``; the broker never closes it. ``EngineState`` is
just a tiny wrapper that lets the broker hand the same engine reference to its
client and to subscribers.
"""

import typing
from dataclasses import dataclass, field

from faststream._internal.configs import BrokerConfig
from faststream.exceptions import IncorrectState


if typing.TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine

    from faststream_outbox.client import OutboxClient


class EngineState:
    """
    Lazy holder so the broker can be constructed before the engine is wired up.

    Callers may pass an engine at broker construction (the common case) or attach
    one later via ``set_engine`` (used by the test broker).
    """

    def __init__(self, engine: "AsyncEngine | None" = None) -> None:
        self._engine = engine

    @property
    def engine(self) -> "AsyncEngine":
        if self._engine is None:
            msg = "Engine not available. Pass an AsyncEngine to OutboxBroker(...)."
            raise IncorrectState(msg)
        return self._engine

    def set_engine(self, engine: "AsyncEngine") -> None:
        self._engine = engine


@dataclass(kw_only=True)
class OutboxBrokerConfig(BrokerConfig):
    engine_state: EngineState = field(default_factory=EngineState)
    client: "OutboxClient | None" = None

    async def connect(self) -> None:
        # Engine and client are wired up by the broker's constructor; nothing to do here.
        pass

    async def disconnect(self) -> None:
        # Caller owns the engine — never dispose it here.
        pass


@dataclass(kw_only=True)
class OutboxRouterConfig(BrokerConfig):
    @property
    def engine_state(self) -> None:  # pragma: no cover
        raise IncorrectState
