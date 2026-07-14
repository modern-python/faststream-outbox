"""The workloads: drive the real broker, measure what Postgres did.

Two scenarios:

* **consumer** -- seed N rows directly, then start the broker with a no-op handler
  and drain them. A no-op handler is the point: what is measured is transport cost,
  not handler cost.
* **producer** -- N ``publish()`` calls in one transaction. Surfaces the two
  statements per publish (``INSERT ... RETURNING`` + ``SELECT pg_notify``).

Each run gets a fresh table (UUID suffix, mirroring the ``outbox_table`` fixture in
tests/conftest.py) so pg_stat_user_tables counters start at zero.

Two rules the probe imposes on everything below (see :func:`benchmarks.probes.probe`):

* the ``AsyncEngine`` is built by :func:`make_engine` and passed in, never constructed
  inside a probe block -- SQLAlchemy's one-time dialect init would bake 6 extra
  statements into the baseline;
* every connection is released before the probe block exits (the broker is stopped and
  every ``engine.begin()`` / ``AsyncSession`` block is closed inside the window), because
  ``dispose()`` cannot flush the stats of a backend that is still checked out.
"""

import asyncio
import dataclasses
import time
import uuid

from sqlalchemy import MetaData, Table, insert, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine

from benchmarks.config import APPLICATION_NAME, RunConfig
from benchmarks.probes import ProbeResult, assert_owns_database, ensure_extension, probe
from faststream_outbox import OutboxBroker, make_outbox_table


_DRAIN_TIMEOUT_SECONDS = 600.0


@dataclasses.dataclass(frozen=True)
class RunResult:
    """One scenario's wall time plus the catalog deltas it produced."""

    scenario: str
    config: RunConfig
    wall_seconds: float
    probe: ProbeResult


def make_engine(dsn: str) -> AsyncEngine:
    """Engine tagged with APPLICATION_NAME so the ownership check can recognize us."""
    return create_async_engine(
        dsn,
        future=True,
        connect_args={"server_settings": {"application_name": APPLICATION_NAME}},
    )


async def _create_table(engine: AsyncEngine) -> tuple[MetaData, Table]:
    metadata = MetaData()
    table = make_outbox_table(metadata, table_name=f"bench_{uuid.uuid4().hex[:10]}")
    async with engine.begin() as conn:
        await conn.run_sync(metadata.create_all)
    return metadata, table


async def _drop_table(engine: AsyncEngine, metadata: MetaData) -> None:
    async with engine.begin() as conn:
        await conn.run_sync(metadata.drop_all)


async def _checkpoint(engine: AsyncEngine) -> None:
    """Force a checkpoint before measuring.

    Done consistently so the full-page-image cost is at least reproducible. WAL *bytes*
    still swing run to run because of FPI; that is why the gate is on wal_records, not
    wal_bytes.
    """
    async with engine.connect() as conn:
        await conn.execution_options(isolation_level="AUTOCOMMIT")
        await conn.execute(text("CHECKPOINT"))


async def run_consumer(engine: AsyncEngine, cfg: RunConfig) -> RunResult:
    """Seed N rows, drain them through the real broker with a no-op handler."""
    await ensure_extension(engine)
    metadata, table = await _create_table(engine)
    try:
        payload = b"x" * cfg.payload_bytes
        async with engine.begin() as conn:
            await conn.execute(
                insert(table),
                [{"queue": cfg.queue, "payload": payload, "headers": {}} for _ in range(cfg.messages)],
            )

        await _checkpoint(engine)
        await assert_owns_database(engine)

        broker = OutboxBroker(engine, outbox_table=table)
        drained = asyncio.Event()
        seen = 0

        @broker.subscriber(
            cfg.queue,
            max_workers=cfg.max_workers,
            fetch_batch_size=cfg.fetch_batch_size,
        )
        async def _handler(msg: bytes) -> None:  # noqa: ARG001
            nonlocal seen
            seen += 1
            if seen >= cfg.messages:
                drained.set()

        async with probe(engine, table.name) as sink:
            started = time.perf_counter()
            await broker.start()
            try:
                await asyncio.wait_for(drained.wait(), timeout=_DRAIN_TIMEOUT_SECONDS)
            finally:
                # Must complete inside the probe block: a still-checked-out connection
                # never flushes its stats.
                await broker.stop()
            wall = time.perf_counter() - started

        return RunResult(scenario="consumer", config=cfg, wall_seconds=wall, probe=sink[0])
    finally:
        await _drop_table(engine, metadata)


async def run_producer(engine: AsyncEngine, cfg: RunConfig) -> RunResult:
    """N publish() calls in one transaction -- the write-path cost."""
    await ensure_extension(engine)
    metadata, table = await _create_table(engine)
    try:
        await _checkpoint(engine)
        await assert_owns_database(engine)

        broker = OutboxBroker(engine, outbox_table=table)
        payload = b"x" * cfg.payload_bytes

        async with probe(engine, table.name) as sink:
            started = time.perf_counter()
            async with AsyncSession(engine) as session, session.begin():
                for _ in range(cfg.messages):
                    await broker.publish(payload, queue=cfg.queue, session=session)
            wall = time.perf_counter() - started

        return RunResult(scenario="producer", config=cfg, wall_seconds=wall, probe=sink[0])
    finally:
        await _drop_table(engine, metadata)
