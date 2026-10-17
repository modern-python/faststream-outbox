"""Workload configuration for a single benchmark run."""

import dataclasses


# Matches the payload size in Tsvettsikh's «50 оттенков Transactional Outbox»
# benchmarks, so our numbers stay commensurable with his.
DEFAULT_PAYLOAD_BYTES = 256

# The DSN the harness runs against. Overridden by POSTGRES_DSN in compose.
DEFAULT_DSN = "postgresql+asyncpg://outbox:outbox@localhost:5432/outbox"

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
