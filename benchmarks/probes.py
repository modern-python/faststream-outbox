"""Postgres catalog probes: snapshot the server's counters around a workload.

Every statement the probe issues is a plain ``SELECT`` whose SQL text contains the
substring ``pg_stat``, and it runs on an **AUTOCOMMIT** connection so SQLAlchemy emits
no implicit ``BEGIN``/``COMMIT``/``ROLLBACK`` around it. The round-trip aggregation
filters those SELECTs out (``query NOT ILIKE '%pg_stat%'``); with no implicit
transaction control to leak, that filter is enough for the probe to fully exclude
itself. The workload's statements -- including *its* transaction control, which is real
round-trip work -- never match the filter, so they are all counted, by design.

Two hazards, both hit while prototyping this design:

* ``pg_stat_user_tables`` and ``pg_stat_wal`` **lag**: a backend accumulates its stats
  locally and flushes them to shared memory no more often than ``PGSTAT_MIN_INTERVAL``
  (1s). Reading straight after a workload can report zero for mutations that definitely
  happened, and reading straight after a seed can miss the seed -- which then leaks into
  the delta. Postgres force-flushes a backend's pending stats when the backend *exits*,
  so the probe calls :meth:`AsyncEngine.dispose` (which closes every pooled connection)
  immediately before each snapshot. That is deterministic, unlike waiting.
* ``pg_stat_statements`` is **database-wide**. A stray psql session would silently
  corrupt the round-trip count, so :func:`assert_owns_database` fails loudly rather
  than reporting a quietly wrong number.
"""

import contextlib
import dataclasses
from collections.abc import AsyncIterator

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from benchmarks.config import APPLICATION_NAME


# Resets the statement counters and captures every cumulative origin in ONE round-trip,
# so exactly one probe statement lands in pg_stat_statements before the workload runs
# (and it is filtered out again by the NOT ILIKE '%pg_stat%' predicate below).
#
# The table counters MUST be captured here, not assumed zero: pg_stat_user_tables is
# cumulative from table creation, and the consumer scenario seeds its rows *before* the
# probe opens. Treating the end-of-run snapshot as the delta would report the seed's
# 5,000 inserts as workload inserts. The `present` flag lets probe() fail fast on a
# missing table instead of dying with NoResultFound at the end of a long run; the
# coalesce()s keep this statement returning a row either way so that check can run.
#
# :schema is NULL by default and resolves to current_schema(); it is echoed back so the
# end snapshot pins the same schema. pg_stat_user_tables spans every schema, so matching
# on a bare relname could hit two same-named tables.
_START_SQL = text(
    """
    WITH scope AS (SELECT coalesce(CAST(:schema AS text), current_schema()::text) AS schemaname)
    SELECT
      pg_stat_statements_reset() IS NOT NULL AS reset,
      pg_current_wal_lsn() AS lsn,
      scope.schemaname AS schemaname,
      (SELECT w.wal_records FROM pg_stat_wal w) AS wal_records,
      (SELECT w.wal_fpi FROM pg_stat_wal w) AS wal_fpi,
      EXISTS (
        SELECT 1 FROM pg_stat_user_tables t
        WHERE t.relname = :table AND t.schemaname = scope.schemaname
      ) AS present,
      coalesce((SELECT t.n_tup_ins FROM pg_stat_user_tables t
        WHERE t.relname = :table AND t.schemaname = scope.schemaname), 0) AS tup_ins,
      coalesce((SELECT t.n_tup_upd FROM pg_stat_user_tables t
        WHERE t.relname = :table AND t.schemaname = scope.schemaname), 0) AS tup_upd,
      coalesce((SELECT t.n_tup_del FROM pg_stat_user_tables t
        WHERE t.relname = :table AND t.schemaname = scope.schemaname), 0) AS tup_del,
      coalesce((SELECT t.n_tup_hot_upd FROM pg_stat_user_tables t
        WHERE t.relname = :table AND t.schemaname = scope.schemaname), 0) AS tup_hot_upd,
      coalesce((SELECT t.n_tup_newpage_upd FROM pg_stat_user_tables t
        WHERE t.relname = :table AND t.schemaname = scope.schemaname), 0) AS tup_newpage_upd
    FROM scope
    """,
)

# One statement for the whole end-of-run snapshot. Every aggregate excludes the
# probe's own queries by SQL text. schemaname is pinned: pg_stat_user_tables covers
# every schema, so a bare relname match could return two rows.
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
    WHERE t.relname = :table AND t.schemaname = :schema
    """,
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


@contextlib.asynccontextmanager
async def _autocommit(engine: AsyncEngine) -> AsyncIterator[AsyncConnection]:
    """Open a connection that emits no implicit BEGIN/COMMIT/ROLLBACK.

    pg_stat_statements tracks utility statements, and none of BEGIN/COMMIT/ROLLBACK
    contains 'pg_stat', so SQLAlchemy's implicit transaction control would slip past
    the probe's self-exclusion filter and inflate `calls` by a run-dependent amount.
    AUTOCOMMIT removes it at the source.
    """
    async with engine.connect() as conn:
        yield await conn.execution_options(isolation_level="AUTOCOMMIT")


async def ensure_extension(engine: AsyncEngine) -> None:
    """Create pg_stat_statements if the server preloaded it; raise a legible error if not."""
    async with _autocommit(engine) as conn:
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
    round-trip count. Checked once, *before* the broker starts -- at that point the
    only connections are the harness' own (tagged with APPLICATION_NAME).
    """
    async with _autocommit(engine) as conn:
        foreign = (await conn.execute(_OWNERSHIP_SQL, {"app": APPLICATION_NAME})).scalar_one()
    if foreign:
        msg = (
            f"{foreign} foreign backend(s) are connected to the benchmark database. "
            "pg_stat_statements is database-wide, so the round-trip count would be wrong. "
            "Close other sessions (psql, an IDE, a stray app) and re-run."
        )
        raise RuntimeError(msg)


@contextlib.asynccontextmanager
async def probe(engine: AsyncEngine, table_name: str, schema: str | None = None) -> AsyncIterator[list[ProbeResult]]:
    """Snapshot the catalogs around the wrapped workload.

    Yields a list that holds exactly one :class:`ProbeResult` once the block exits.
    A list (rather than a return value) is the only way an async context manager can
    hand a result back to its caller. ``schema`` defaults to ``current_schema()``.

    If the workload raises, the exception propagates and the list stays **empty** --
    the snapshot is deliberately not taken in a ``finally``, because a half-run
    workload's counters are not a measurement. Callers must not assume ``sink[0]``
    without first checking that the block completed.

    ``engine`` is disposed (all pooled connections closed) immediately before each
    snapshot: that is what force-flushes the pending backend stats. The engine stays
    usable afterwards -- SQLAlchemy just opens fresh connections -- so a sweep can keep
    sharing one engine across runs. Connections still *checked out* when the probe exits
    are not disposed by :meth:`AsyncEngine.dispose`, so this raises ``RuntimeError``
    instead of silently under-reporting; the workload must release every connection
    (stop the broker, exit all ``async with engine.begin()`` blocks) before the block ends.

    ``engine`` must also be constructed *before* the probe block opens. SQLAlchemy's
    one-time dialect init (``select pg_catalog.version()``, ``select current_schema()``,
    ``show standard_conforming_strings``, ``show transaction isolation level``, plus a
    BEGIN/ROLLBACK) runs on an engine's first connection and inflates ``calls`` by 6;
    none of those statements contain ``pg_stat``, so the self-exclusion filter cannot
    catch them. It is deterministic, so it will not flap the gate -- it will just bake
    a wrong baseline in.
    """
    sink: list[ProbeResult] = []

    # Flush the seed's stats before reading the origin, or they leak into the delta.
    await engine.dispose()
    async with _autocommit(engine) as conn:
        start = (await conn.execute(_START_SQL, {"table": table_name, "schema": schema})).one()
    if not start.present:
        msg = (
            f"table {start.schemaname}.{table_name} has no pg_stat_user_tables row: it does not exist "
            "(or is not a user table). Create it before opening the probe."
        )
        raise RuntimeError(msg)

    yield sink

    checked_out = engine.pool.checkedout()  # ty: ignore[unresolved-attribute]
    if checked_out:
        msg = (
            f"{checked_out} connection(s) are still checked out of the pool. "
            "AsyncEngine.dispose() only closes checked-in connections, so a checked-out "
            "backend never exits and its stats are never flushed -- the tuple and WAL "
            "counters would silently under-report. Release/close all connections (stop "
            "the broker, exit all `async with engine.begin()` blocks) before leaving "
            "the probe() block."
        )
        raise RuntimeError(msg)

    # Flush the workload's stats before reading the end snapshot.
    await engine.dispose()
    async with _autocommit(engine) as conn:
        row = (
            await conn.execute(
                _SNAPSHOT_SQL,
                {"lsn0": start.lsn, "table": table_name, "schema": start.schemaname},
            )
        ).one()

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
