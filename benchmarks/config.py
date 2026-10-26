"""Workload configuration for a single benchmark run."""

import dataclasses
import typing


# Matches the payload size in Tsvettsikh's «50 оттенков Transactional Outbox»
# benchmarks, so our numbers stay commensurable with his.
DEFAULT_PAYLOAD_BYTES = 256

# The DSN the harness runs against. Overridden by POSTGRES_DSN in compose.
DEFAULT_DSN = "postgresql+asyncpg://outbox:outbox@localhost:5432/outbox"

# How an extend point's table is vacuumed: no reloptions, outbox_autovacuum_ddl's defaults,
# or those defaults with autovacuum's cost throttle turned off.
VacuumVariant = typing.Literal["untuned", "tuned", "unthrottled"]

# Tags the harness' own connections so the probe can assert it owns the database.
APPLICATION_NAME = "faststream-outbox-bench"


@dataclasses.dataclass(frozen=True)
class RunConfig:
    """One point in the benchmark sweep."""

    messages: int = 5_000
    max_workers: int = 1
    fetch_batch_size: int = 100
    payload_bytes: int = DEFAULT_PAYLOAD_BYTES
    queue: str = "bench"
    # 1 = today's per-row terminal DELETE; >1 coalesces plain-delete acks into one
    # batched ``DELETE ... RETURNING`` per batch (the opt-in batched-flush path).
    terminal_flush_batch_size: int = 1
    # Consumer (min_fetch_interval, max_fetch_interval) in seconds.
    fetch_interval: tuple[float, float] = (0.001, 0.001)
    # Producer publishes per transaction; None puts every publish in one transaction.
    publishes_per_transaction: int | None = None


@dataclasses.dataclass(frozen=True)
class ExtendConfig:
    """One point of the timer extend sweep: heartbeats at *rate* pings/s over *timers* identities."""

    name: str
    rate: int
    timers: int
    timeout_seconds: float = 6.0
    pings_per_transaction: int = 1
    # Longest a ping waits for its transaction to fill before it is flushed anyway.
    flush_interval: float = 0.01
    seconds: float = 30.0
    # This share of identities stops pinging at silence_at, so their timers must fire.
    silent_fraction: float = 0.05
    pool_size: int = 20
    max_fetch_interval: float = 1.0
    queue: str = "timeouts"
    vacuum: VacuumVariant = "tuned"
    # Seconds between table-health samples taken while the load runs.
    sample_interval: float = 10.0

    def __post_init__(self) -> None:
        if self.vacuum not in typing.get_args(VacuumVariant):
            msg = f"{self.name}: vacuum must be one of {typing.get_args(VacuumVariant)}, got {self.vacuum!r}"
            raise ValueError(msg)
        if self.sample_interval <= 0:
            msg = f"{self.name}: sample_interval must be positive, got {self.sample_interval}"
            raise ValueError(msg)
        if self.ping_interval >= self.timeout_seconds:
            msg = f"{self.name}: each identity pings every {self.ping_interval:.2f}s, not before its timeout"
            raise ValueError(msg)
        if self.silence_at + self.timeout_seconds + self.max_fetch_interval + 2.0 > self.seconds:
            msg = f"{self.name}: {self.seconds}s is too short for the silent timers to fire inside the run"
            raise ValueError(msg)

    @property
    def ping_interval(self) -> float:
        return self.timers / self.rate

    @property
    def silence_at(self) -> float:
        return self.seconds / 4

    @property
    def silent_timers(self) -> int:
        return int(self.timers * self.silent_fraction)
