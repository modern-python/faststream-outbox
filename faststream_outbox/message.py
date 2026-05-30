"""
Outbox message representations.

``OutboxInnerMessage`` is the in-memory mirror of a row claimed by the fetch loop.
Its ``ack``/``nack``/``reject`` methods only mutate in-memory intent — the actual
``DELETE`` or ``UPDATE`` is issued by the worker loop, scoped by ``acquired_token``
so a re-claimed row's lease holder is the only writer.

``OutboxMessage`` adapts the inner message to FastStream's ``StreamMessage``.
"""

import datetime as _dt
import logging
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal

from faststream.message.message import StreamMessage


if TYPE_CHECKING:
    from faststream._internal.basic_types import LoggerProto

    from faststream_outbox.retry import RetryStrategyProto


# Public contract for the DLQ ``failure_reason`` column and the ``reason`` tag on
# ``nacked_terminal`` / ``dlq_written`` metric events. Operators query against these
# literals (and dashboards key labels off them) — adding a new value is a public
# API change. The DLQ column is sized to accommodate growth; see ``schema.py``.
DLQFailureReason = Literal["max_deliveries", "retry_terminal", "rejected"]


def _utcnow() -> _dt.datetime:
    return _dt.datetime.now(tz=_dt.UTC)


@dataclass(kw_only=True)
class OutboxInnerMessage:
    """
    In-memory copy of a claimed outbox row, plus ack/nack/reject intent helpers.

    The ack/nack/reject methods set in-memory intent flags (``to_delete``,
    ``pending_delay_seconds``). The worker loop reads those flags and issues the
    actual DB write, scoped by ``acquired_token``.
    """

    id: int
    queue: str
    payload: bytes
    headers: dict[str, str] | None
    attempts_count: int
    deliveries_count: int
    created_at: _dt.datetime
    next_attempt_at: _dt.datetime
    first_attempt_at: _dt.datetime | None
    last_attempt_at: _dt.datetime | None
    acquired_at: _dt.datetime | None
    acquired_token: uuid.UUID | None

    retry_strategy: "RetryStrategyProto | None" = None
    last_exception: BaseException | None = None

    state_set: bool = field(default=False, init=False)
    to_delete: bool = field(default=False, init=False)
    # Set by ``_nack`` when the strategy schedules a retry; consumed by the
    # subscriber's ``_flush_retry`` to drive ``mark_pending_with_lease``.
    pending_delay_seconds: float | None = field(default=None, init=False)
    # Set on terminal-failure paths (``allow_delivery`` False, ``_nack`` exhausted,
    # ``_reject``). ``_flush_terminal`` reads it to decide whether to build a DLQ
    # payload; ``dispatch_one`` reads it to pick the ``nacked_terminal`` reason
    # tag. Stays ``None`` on the success (``_ack``) path so handler-success
    # never touches the DLQ.
    terminal_failure_reason: "DLQFailureReason | None" = field(default=None, init=False)

    async def ack(self) -> None:
        await self._update_state_if_not_set(self._ack)

    async def nack(self) -> None:
        await self._update_state_if_not_set(self._nack)

    async def reject(self) -> None:
        await self._update_state_if_not_set(self._reject)

    async def _update_state_if_not_set(self, fn: Callable[[], Awaitable[None]]) -> None:
        if self.state_set:
            return
        await fn()
        self.state_set = True

    async def _ack(self) -> None:
        self._record_attempt()
        self.to_delete = True

    async def _nack(self) -> None:
        self._record_attempt()
        delay = (
            self.retry_strategy.get_next_attempt_delay(
                first_attempt_at=self.first_attempt_at or self.last_attempt_at,  # ty: ignore[invalid-argument-type]
                last_attempt_at=self.last_attempt_at,  # ty: ignore[invalid-argument-type]
                attempts_count=self.attempts_count,
                exception=self.last_exception,
            )
            if self.retry_strategy is not None
            else None
        )
        if delay is None:
            self.to_delete = True
            self.terminal_failure_reason = "retry_terminal"
        else:
            self.pending_delay_seconds = delay

    async def _reject(self) -> None:
        self._record_attempt()
        self.to_delete = True
        self.terminal_failure_reason = "rejected"

    def _record_attempt(self) -> None:
        self.attempts_count += 1
        now = _utcnow()
        self.last_attempt_at = now
        if self.first_attempt_at is None:
            self.first_attempt_at = now

    def allow_delivery(self, *, max_deliveries: int | None, logger: "LoggerProto | None") -> bool:
        """If ``max_deliveries`` is set and exceeded, mark for deletion without invoking the handler."""
        if max_deliveries is not None and self.deliveries_count > max_deliveries:
            self.to_delete = True
            self.state_set = True
            self.terminal_failure_reason = "max_deliveries"
            if logger is not None:
                logger.log(
                    logging.ERROR,
                    f"Outbox message {self} exceeded max_deliveries={max_deliveries}; rejecting",
                )
            return False
        return True

    async def assert_state_set(self, logger: "LoggerProto | None") -> None:
        """Manual-ack fallback: if the handler returned without ack/nack/reject, reject."""
        if not self.state_set:
            if logger is not None:
                logger.log(
                    logging.ERROR,
                    f"Outbox message {self} state not set after handler returned; rejecting as fallback",
                )
            await self.reject()

    def __repr__(self) -> str:
        return f"OutboxInnerMessage(id={self.id}, queue={self.queue!r})"


class OutboxMessage(StreamMessage[OutboxInnerMessage]):
    """FastStream stream-message wrapper. Forwards ack/nack/reject to the inner row."""

    async def ack(self) -> None:
        await self.raw_message.ack()
        await super().ack()

    async def nack(self) -> None:
        await self.raw_message.nack()
        await super().nack()

    async def reject(self) -> None:
        await self.raw_message.reject()
        await super().reject()
