import random
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Protocol


class RetryStrategyProto(Protocol):
    """
    Decides whether a Nack'ed row gets another attempt and when.

    Implementations return ``None`` to signal terminal failure (the row will be
    deleted). The current ``exception`` (if any) is passed through so users can
    subclass to retry only on transient errors.
    """

    def get_next_attempt_at(
        self,
        *,
        first_attempt_at: datetime,
        last_attempt_at: datetime,
        attempts_count: int,
        exception: BaseException | None = None,
    ) -> datetime | None: ...


@dataclass(kw_only=True)
class _RetryStrategyTemplate(ABC, RetryStrategyProto):
    max_attempts: int | None = None
    max_total_delay_seconds: float | None = None

    @abstractmethod
    def _delay_seconds(self, *, attempts_count: int) -> float: ...

    def get_next_attempt_at(
        self,
        *,
        first_attempt_at: datetime,
        last_attempt_at: datetime,
        attempts_count: int,
        exception: BaseException | None = None,  # noqa: ARG002
    ) -> datetime | None:
        if self.max_attempts is not None and attempts_count >= self.max_attempts:
            return None
        delay = self._delay_seconds(attempts_count=attempts_count)
        next_attempt_at = last_attempt_at + timedelta(seconds=delay)
        if (
            self.max_total_delay_seconds is not None
            and (next_attempt_at - first_attempt_at).total_seconds() > self.max_total_delay_seconds
        ):
            return None
        return next_attempt_at


@dataclass(kw_only=True)
class NoRetry(RetryStrategyProto):
    """No retry — first nack is terminal."""

    def get_next_attempt_at(
        self,
        *,
        first_attempt_at: datetime,  # noqa: ARG002
        last_attempt_at: datetime,  # noqa: ARG002
        attempts_count: int,  # noqa: ARG002
        exception: BaseException | None = None,  # noqa: ARG002
    ) -> None:
        return None


@dataclass(kw_only=True)
class ConstantRetry(_RetryStrategyTemplate):
    delay_seconds: float

    def _delay_seconds(self, *, attempts_count: int) -> float:  # noqa: ARG002
        return self.delay_seconds


@dataclass(kw_only=True)
class LinearRetry(_RetryStrategyTemplate):
    initial_delay_seconds: float
    step_seconds: float

    def _delay_seconds(self, *, attempts_count: int) -> float:
        return self.initial_delay_seconds + self.step_seconds * max(0, attempts_count - 1)


@dataclass(kw_only=True)
class ExponentialRetry(_RetryStrategyTemplate):
    initial_delay_seconds: float
    multiplier: float = 2.0
    max_delay_seconds: float | None = None
    jitter_factor: float = 0.0
    _random: random.Random = field(default_factory=random.Random)

    def _delay_seconds(self, *, attempts_count: int) -> float:
        delay = self.initial_delay_seconds * (self.multiplier ** max(0, attempts_count - 1))
        if self.max_delay_seconds is not None:
            delay = min(delay, self.max_delay_seconds)
        if self.jitter_factor:
            delay += self._random.uniform(0.0, delay * self.jitter_factor)
        return delay
