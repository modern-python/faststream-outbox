import random
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Protocol


# Absolute ceiling on a single computed delay. Guards two failure modes for an
# ExponentialRetry left unbounded (no ``max_attempts`` and no ``max_delay_seconds``):
# (1) ``multiplier ** attempts`` raising ``OverflowError`` at very high attempt
# counts (``2.0 ** 1024``), and (2) producing a delay larger than Postgres'
# ``make_interval(secs => …)`` can represent. ~100 years is far beyond any sane
# retry horizon, so this never changes behavior for a real configuration.
_MAX_DELAY_SECONDS = 100.0 * 365.0 * 24.0 * 60.0 * 60.0

# Jitter is applied as ``delay *= 1 + U(-j/2, +j/2)``; j > 2 makes the lower bound
# negative, so a jittered delay can go negative → an immediate (hot) retry. Bound it.
_MAX_JITTER_FACTOR = 2.0


def _validate_jitter_factor(jitter_factor: float) -> None:
    if not (0.0 <= jitter_factor <= _MAX_JITTER_FACTOR):
        msg = f"jitter_factor must be between 0 and {_MAX_JITTER_FACTOR}, got {jitter_factor}"
        raise ValueError(msg)


class RetryStrategyProto(Protocol):
    """
    Decides whether a Nack'ed row gets another attempt and how long to wait.

    Implementations return the delay in seconds before the next attempt, or
    ``None`` to signal terminal failure (the row will be deleted). The DB
    computes the actual ``next_attempt_at`` from this delay using its own clock,
    so retry timing is immune to skew between worker and DB hosts. The current
    ``exception`` (if any) is passed through so users can subclass to retry only
    on transient errors.
    """

    def get_next_attempt_delay(
        self,
        *,
        first_attempt_at: datetime,
        last_attempt_at: datetime,
        attempts_count: int,
        exception: BaseException | None = None,
    ) -> float | None: ...


@dataclass(kw_only=True)
class _RetryStrategyTemplate(ABC, RetryStrategyProto):
    max_attempts: int | None = None
    max_total_delay_seconds: float | None = None

    def __post_init__(self) -> None:
        # P23: reject knobs that produce nonsense (a hot-retry loop, an unreachable cap).
        if self.max_attempts is not None and self.max_attempts < 1:
            msg = f"max_attempts must be >= 1 if set, got {self.max_attempts}"
            raise ValueError(msg)
        if self.max_total_delay_seconds is not None and self.max_total_delay_seconds <= 0:
            msg = f"max_total_delay_seconds must be > 0 if set, got {self.max_total_delay_seconds}"
            raise ValueError(msg)

    @abstractmethod
    def _delay_seconds(self, *, attempts_count: int) -> float: ...

    def get_next_attempt_delay(
        self,
        *,
        first_attempt_at: datetime,
        last_attempt_at: datetime,
        attempts_count: int,
        exception: BaseException | None = None,  # noqa: ARG002
    ) -> float | None:
        if self.max_attempts is not None and attempts_count >= self.max_attempts:
            return None
        delay = self._delay_seconds(attempts_count=attempts_count)
        if self.max_total_delay_seconds is not None:
            elapsed_so_far = (last_attempt_at - first_attempt_at).total_seconds()
            if elapsed_so_far + delay > self.max_total_delay_seconds:
                return None
        return delay


@dataclass(kw_only=True)
class NoRetry(RetryStrategyProto):
    """No retry — first nack is terminal."""

    def get_next_attempt_delay(
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
    jitter_factor: float = 0.0
    _random: random.Random = field(default_factory=random.Random)

    def __post_init__(self) -> None:
        super().__post_init__()
        if self.delay_seconds <= 0:
            msg = f"delay_seconds must be > 0, got {self.delay_seconds}"
            raise ValueError(msg)
        _validate_jitter_factor(self.jitter_factor)

    def _delay_seconds(self, *, attempts_count: int) -> float:  # noqa: ARG002
        delay = self.delay_seconds
        if self.jitter_factor:
            delay *= 1.0 + self._random.uniform(-self.jitter_factor / 2, self.jitter_factor / 2)
        return delay


@dataclass(kw_only=True)
class LinearRetry(_RetryStrategyTemplate):
    initial_delay_seconds: float
    step_seconds: float
    jitter_factor: float = 0.0
    _random: random.Random = field(default_factory=random.Random)

    def __post_init__(self) -> None:
        super().__post_init__()
        if self.initial_delay_seconds <= 0:
            msg = f"initial_delay_seconds must be > 0, got {self.initial_delay_seconds}"
            raise ValueError(msg)
        if self.step_seconds < 0:
            msg = f"step_seconds must be >= 0, got {self.step_seconds}"
            raise ValueError(msg)
        _validate_jitter_factor(self.jitter_factor)

    def _delay_seconds(self, *, attempts_count: int) -> float:
        delay = self.initial_delay_seconds + self.step_seconds * max(0, attempts_count - 1)
        if self.jitter_factor:
            delay *= 1.0 + self._random.uniform(-self.jitter_factor / 2, self.jitter_factor / 2)
        return delay


@dataclass(kw_only=True)
class ExponentialRetry(_RetryStrategyTemplate):
    initial_delay_seconds: float
    multiplier: float = 2.0
    max_delay_seconds: float | None = None
    jitter_factor: float = 0.0
    _random: random.Random = field(default_factory=random.Random)

    def __post_init__(self) -> None:
        super().__post_init__()
        if self.initial_delay_seconds <= 0:
            msg = f"initial_delay_seconds must be > 0, got {self.initial_delay_seconds}"
            raise ValueError(msg)
        if self.multiplier <= 0:
            msg = f"multiplier must be > 0, got {self.multiplier}"
            raise ValueError(msg)
        if self.max_delay_seconds is not None and self.max_delay_seconds <= 0:
            msg = f"max_delay_seconds must be > 0 if set, got {self.max_delay_seconds}"
            raise ValueError(msg)
        _validate_jitter_factor(self.jitter_factor)

    def _delay_seconds(self, *, attempts_count: int) -> float:
        try:
            delay = self.initial_delay_seconds * (self.multiplier ** max(0, attempts_count - 1))
        except OverflowError:
            # An unbounded exponential eventually overflows float (``2.0 ** 1024``).
            # Saturate at the absolute ceiling rather than letting the strategy
            # raise into the destructive reject fallback.
            delay = _MAX_DELAY_SECONDS
        # Jitter before clamp so max_delay_seconds is the true ceiling.
        if self.jitter_factor:
            delay *= 1.0 + self._random.uniform(-self.jitter_factor / 2, self.jitter_factor / 2)
        if self.max_delay_seconds is not None:
            delay = min(delay, self.max_delay_seconds)
        # Absolute backstop so an unbounded config can't emit a delay Postgres'
        # make_interval() can't represent.
        return min(delay, _MAX_DELAY_SECONDS)
