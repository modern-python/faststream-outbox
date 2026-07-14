"""Postgres catalog probes: snapshot the server's counters around a workload.

Every statement issued here contains the substring ``pg_stat`` in its SQL text,
and the round-trip aggregation filters those out (``query NOT ILIKE '%pg_stat%'``).
That is how the probe avoids counting itself. The workload's own statements
(INSERT/UPDATE/DELETE/SELECT on the outbox table, ``pg_notify``) never match the
filter, so they are all counted.

Two hazards, both hit while prototyping this design:

* ``pg_stat_user_tables`` **lags**. A bare read straight after the workload
  returned ``n_tup_upd = 0`` for 10,000 updates that had definitely happened.
  :func:`_settle` polls until the counters stop moving.
* ``pg_stat_statements`` is **database-wide**. A stray psql session would silently
  corrupt the round-trip count, so :func:`assert_owns_database` fails loudly rather
  than reporting a quietly wrong number.
"""

import asyncio
import contextlib
import dataclasses
from collections.abc import AsyncIterator

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from benchmarks.config import APPLICATION_NAME


_SETTLE_POLL_SECONDS = 0.25
_SETTLE_STABLE_READS = 3
_SETTLE_TIMEOUT_SECONDS = 15.0

# Resets the statement counters and captures every cumulative origin in ONE round-trip,
# so exactly one probe statement lands in pg_stat_statements before the workload runs
# (and it is filtered out again by the NOT ILIKE '%pg_stat%' predicate below).
#
# The table counters MUST be captured here, not assumed zero: pg_stat_user_tables is
# cumulative from table creation, and the consumer scenario seeds its rows *before* the
# probe opens. Treating the end-of-run snapshot as the delta would report the seed's
# 5,000 inserts as workload inserts. coalesce+LEFT JOIN because a freshly created table
# may have no pg_stat_user_tables row at all until it is first touched.
_START_SQL = text(
    """
    SELECT
      pg_stat_statements_reset() IS NOT NULL AS reset,
      pg_current_wal_lsn() AS lsn,
      (SELECT w.wal_records FROM pg_stat_wal w) AS wal_records,
      (SELECT w.wal_fpi FROM pg_stat_wal w) AS wal_fpi,
      coalesce((SELECT t.n_tup_ins FROM pg_stat_user_tables t WHERE t.relname = :table), 0) AS tup_ins,
      coalesce((SELECT t.n_tup_upd FROM pg_stat_user_tables t WHERE t.relname = :table), 0) AS tup_upd,
      coalesce((SELECT t.n_tup_del FROM pg_stat_user_tables t WHERE t.relname = :table), 0) AS tup_del,
      coalesce((SELECT t.n_tup_hot_upd FROM pg_stat_user_tables t WHERE t.relname = :table), 0) AS tup_hot_upd,
      coalesce((SELECT t.n_tup_newpage_upd FROM pg_stat_user_tables t WHERE t.relname = :table), 0)
        AS tup_newpage_upd
    """,
)

# One statement for the whole end-of-run snapshot. Every aggregate excludes the
# probe's own queries by SQL text.
_SNAPSHOT_SQL = text(
    """
    SELECT
      (SELECT coalesce(sum(s.calls), 0)
         FROM pg_stat_statements s WHERE s.query NOT ILIKE '%pg_stat%') AS calls,
      (SELECT coalesce(sum(s.total_exec_time), 0.0)
         FROM pg_stat_statements s WHERE s.query NOT ILIKE '%pg_stat%') AS exec_ms,
      (SELECT coalesce(sum(s.shared_blks_hit), 0)
         FROM pg_stat_statements s WHERE s.query NOT ILIKE '%pg_stat%') AS blks_hit,
      (SELECT coalesce(sum(s.shared_blks_read), 0)
         FROM pg_stat_statements s WHERE s.query NOT ILIKE '%pg_stat%') AS blks_read,
      w.wal_records AS wal_records,
      w.wal_fpi AS wal_fpi,
      pg_wal_lsn_diff(pg_current_wal_lsn(), CAST(:lsn0 AS pg_lsn)) AS wal_bytes,
      t.n_tup_ins, t.n_tup_upd, t.n_tup_del,
      t.n_tup_hot_upd, t.n_tup_newpage_upd, t.n_dead_tup,
      pg_relation_size(t.relid) AS heap_bytes,
      pg_indexes_size(t.relid) AS index_bytes
    FROM pg_stat_wal w, pg_stat_user_tables t
    WHERE t.relname = :table
    """,
)

# Contains 'pg_stat', so the settle polls are excluded from the round-trip count too.
_SETTLE_SQL = text(
    "SELECT coalesce(n_tup_upd, 0) + coalesce(n_tup_del, 0) AS mutations "
    "FROM pg_stat_user_tables WHERE relname = :table",
)

_OWNERSHIP_SQL = text(
    "SELECT count(*) FROM pg_stat_activity "
    "WHERE datname = current_database() "
    "AND backend_type = 'client backend' "
    "AND pid <> pg_backend_pid() "
    "AND coalesce(application_name, '') <> :app",
)

_EXTENSION_SQL = text("CREATE EXTENSION IF NOT EXISTS pg_stat_statements")


@dataclasses.dataclass(frozen=True)
class ProbeResult:
    """Catalog deltas across one workload."""

    calls: int
    exec_ms: float
    blks_hit: int
    blks_read: int
    wal_records: int
    wal_fpi: int
    wal_bytes: int
    tup_ins: int
    tup_upd: int
    tup_del: int
    tup_hot_upd: int
    tup_newpage_upd: int
    dead_tup: int
    heap_bytes: int
    index_bytes: int


async def ensure_extension(engine: AsyncEngine) -> None:
    """Create pg_stat_statements if the server preloaded it; raise a legible error if not."""
    async with engine.begin() as conn:
        try:
            await conn.execute(_EXTENSION_SQL)
        except Exception as exc:
            msg = (
                "pg_stat_statements is unavailable. The postgres service must run with "
                "`-c shared_preload_libraries=pg_stat_statements` (see docker-compose.yml). "
                f"Underlying error: {exc}"
            )
            raise RuntimeError(msg) from exc


async def assert_owns_database(engine: AsyncEngine) -> None:
    """Fail loudly if a foreign backend shares the database.

    pg_stat_statements is database-wide, so any other session's queries land in the
    round-trip count. Checked once, *before* the broker starts — at that point the
    only connections are the harness' own (tagged with APPLICATION_NAME).
    """
    async with engine.connect() as conn:
        foreign = (await conn.execute(_OWNERSHIP_SQL, {"app": APPLICATION_NAME})).scalar_one()
    if foreign:
        msg = (
            f"{foreign} foreign backend(s) are connected to the benchmark database. "
            "pg_stat_statements is database-wide, so the round-trip count would be wrong. "
            "Close other sessions (psql, an IDE, a stray app) and re-run."
        )
        raise RuntimeError(msg)


async def _settle(engine: AsyncEngine, table_name: str) -> None:
    """Poll until pg_stat_user_tables stops moving.

    The stats collector flushes lazily; a bare read right after the workload can
    report zero for mutations that have definitely happened.
    """
    deadline = asyncio.get_running_loop().time() + _SETTLE_TIMEOUT_SECONDS
    previous = -1
    stable = 0
    while stable < _SETTLE_STABLE_READS:
        await asyncio.sleep(_SETTLE_POLL_SECONDS)
        async with engine.connect() as conn:
            current = (await conn.execute(_SETTLE_SQL, {"table": table_name})).scalar_one_or_none() or 0
        stable = stable + 1 if current == previous else 0
        previous = current
        if asyncio.get_running_loop().time() > deadline:
            msg = f"pg_stat_user_tables for {table_name!r} never settled within {_SETTLE_TIMEOUT_SECONDS}s"
            raise RuntimeError(msg)


@contextlib.asynccontextmanager
async def probe(engine: AsyncEngine, table_name: str) -> AsyncIterator[list[ProbeResult]]:
    """Snapshot the catalogs around the wrapped workload.

    Yields a list that holds exactly one :class:`ProbeResult` once the block exits.
    A list (rather than a return value) is the only way an async context manager can
    hand a result back to its caller.
    """
    sink: list[ProbeResult] = []
    # Settle first: the seed's inserts must be visible in pg_stat_user_tables before we
    # capture the origin, or they leak into the delta.
    await _settle(engine, table_name)
    async with engine.begin() as conn:
        start = (await conn.execute(_START_SQL, {"table": table_name})).one()

    yield sink

    await _settle(engine, table_name)
    async with engine.connect() as conn:
        row = (await conn.execute(_SNAPSHOT_SQL, {"lsn0": start.lsn, "table": table_name})).one()

    sink.append(
        ProbeResult(
            # pg_stat_statements was reset at start, so these are already deltas.
            calls=int(row.calls),
            exec_ms=float(row.exec_ms),
            blks_hit=int(row.blks_hit),
            blks_read=int(row.blks_read),
            # Server-wide and table-level counters are cumulative -- subtract the origin.
            wal_records=int(row.wal_records) - int(start.wal_records),
            wal_fpi=int(row.wal_fpi) - int(start.wal_fpi),
            wal_bytes=int(row.wal_bytes),
            tup_ins=int(row.n_tup_ins) - int(start.tup_ins),
            tup_upd=int(row.n_tup_upd) - int(start.tup_upd),
            tup_del=int(row.n_tup_del) - int(start.tup_del),
            tup_hot_upd=int(row.n_tup_hot_upd) - int(start.tup_hot_upd),
            tup_newpage_upd=int(row.n_tup_newpage_upd) - int(start.tup_newpage_upd),
            # Point-in-time, not a delta.
            dead_tup=int(row.n_dead_tup),
            heap_bytes=int(row.heap_bytes),
            index_bytes=int(row.index_bytes),
        ),
    )
