"""The timer extend workload: heartbeats extend per-identity timers while a subscriber fires the silent ones.

Pings arrive open loop at ``rate`` per second, round-robin over ``timers`` identities, so each identity is
pinged once per ``timers / rate`` seconds and its deadline is pushed to ``ping time + timeout``. At
``silence_at`` a ``silent_fraction`` of identities stop pinging; their timers must fire about ``timeout``
later. Any other identity firing is a false expiry: the pings fell behind their deadlines.

Pings are grouped into transactions of up to ``pings_per_transaction``, sorted by identity so two
transactions extending the same timers lock them in the same order. Each transaction runs in its own
task on a pool of ``pool_size`` connections, so pool waits show up once Postgres falls behind. A point
stops offering load once the backlog passes five seconds of pings and is reported as saturated.

The table carries the recommended autovacuum settings, unlike the gated sweep's tables, because dead
tuples and their cleanup are part of what this measures. Statement, tuple and WAL counters come from
the same probe as the gated sweep. Nothing here is gated: every number depends on timing.
"""

import asyncio
import contextlib
import dataclasses
import datetime as dt
import json
import time
import typing
import uuid

from sqlalchemy import MetaData, Table, insert, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from benchmarks.config import ExtendConfig
from benchmarks.probes import ProbeResult, _autocommit, assert_owns_database, ensure_extension, probe
from benchmarks.workload import _checkpoint, make_engine
from faststream_outbox import OutboxBroker, make_outbox_table, outbox_autovacuum_ddl
from faststream_outbox._time import utcnow
from faststream_outbox.envelope import _encode_payload


_BACKLOG_SECONDS = 5.0
_TICK_SECONDS = 0.001

_STATEMENT_TIMES_SQL = text(
    """
    SELECT lower(split_part(btrim(s.query), ' ', 1)) AS op,
           sum(s.calls) AS calls,
           sum(s.total_exec_time) / nullif(sum(s.calls), 0) AS mean_ms,
           max(s.max_exec_time) AS max_ms
    FROM pg_stat_statements s
    WHERE s.query NOT ILIKE '%pg_stat%'
    GROUP BY 1
    """,
)

_VACUUM_SQL = text(
    """
    SELECT t.autovacuum_count, t.n_live_tup
    FROM pg_stat_user_tables t
    WHERE t.relname = :table AND t.schemaname = current_schema()
    """,
)


@dataclasses.dataclass
class _Ping:
    identity: int
    arrived: float
    deadline: dt.datetime


@dataclasses.dataclass
class _Tally:
    latencies: list[float] = dataclasses.field(default_factory=list)
    pool_waits: list[float] = dataclasses.field(default_factory=list)
    lateness: list[float] = dataclasses.field(default_factory=list)
    offered: int = 0
    committed: int = 0
    transactions: int = 0
    leased: int = 0
    errors: dict[str, int] = dataclasses.field(default_factory=dict)
    expired_silent: int = 0
    false_expiries: int = 0
    saturated: bool = False
    offered_seconds: float = 0.0
    last_commit: float = 0.0
    silent_fired: asyncio.Event = dataclasses.field(default_factory=asyncio.Event)


@dataclasses.dataclass(frozen=True)
class ExtendResult:
    """One extend point: client-side timings, expiry outcomes and the probe's catalog deltas."""

    config: ExtendConfig
    wall_seconds: float
    offered: int
    offered_seconds: float
    committed: int
    transactions: int
    leased: int
    errors: dict[str, int]
    saturated: bool
    latency: dict[str, float]
    pool_wait: dict[str, float]
    lateness: dict[str, float]
    expired_silent: int
    false_expiries: int
    statement_ms: dict[str, tuple[float, float]]
    autovacuum_count: int
    live_tuples: int
    probe: ProbeResult


def percentiles(values: typing.Sequence[float]) -> dict[str, float]:
    """p50, p95, p99 and max of *values* (nearest rank); zeros for an empty sequence."""
    if not values:
        return {"p50": 0.0, "p95": 0.0, "p99": 0.0, "max": 0.0}
    ordered = sorted(values)

    def _rank(q: float) -> float:
        return ordered[min(len(ordered) - 1, max(0, round(q * len(ordered)) - 1))]

    return {"p50": _rank(0.50), "p95": _rank(0.95), "p99": _rank(0.99), "max": ordered[-1]}


def _body(identity: int, deadline: dt.datetime) -> dict[str, typing.Any]:
    return {"identity": identity, "deadline": deadline.isoformat()}


async def _create_table(engine: AsyncEngine, cfg: ExtendConfig) -> tuple[MetaData, Table]:
    metadata = MetaData()
    table = make_outbox_table(metadata, table_name=f"bench_extend_{uuid.uuid4().hex[:10]}")
    async with engine.begin() as conn:
        await conn.run_sync(metadata.create_all)
        await conn.execute(text(outbox_autovacuum_ddl(table.name)))
        start = utcnow()
        rows = []
        for identity in range(cfg.timers):
            deadline = start + dt.timedelta(seconds=cfg.timeout_seconds + identity / cfg.rate)
            payload, headers = _encode_payload(_body(identity, deadline))
            rows.append(
                {
                    "queue": cfg.queue,
                    "payload": payload,
                    "headers": headers,
                    "timer_id": f"t{identity}",
                    "next_attempt_at": deadline,
                },
            )
        await conn.execute(insert(table), rows)
    return metadata, table


async def _flush(
    batch: list[_Ping],
    *,
    engine: AsyncEngine,
    publisher: OutboxBroker,
    cfg: ExtendConfig,
    tally: _Tally,
) -> None:
    started = time.perf_counter()
    try:
        async with AsyncSession(engine) as session, session.begin():
            await session.connection()
            tally.pool_waits.append(time.perf_counter() - started)
            for ping in sorted(batch, key=lambda p: p.identity):
                rid = await publisher.publish(
                    _body(ping.identity, ping.deadline),
                    queue=cfg.queue,
                    session=session,
                    activate_at=ping.deadline,
                    timer_id=f"t{ping.identity}",
                    timer_conflict="extend",
                )
                if rid is None:
                    tally.leased += 1
    except Exception as exc:  # noqa: BLE001  # a failed transaction is a measurement, not a harness bug
        name = type(exc).__name__
        tally.errors[name] = tally.errors.get(name, 0) + len(batch)
        return
    done = time.perf_counter()
    tally.latencies.extend(done - ping.arrived for ping in batch)
    tally.committed += len(batch)
    tally.transactions += 1
    tally.last_commit = done


async def _offer_load(
    *,
    engine: AsyncEngine,
    publisher: OutboxBroker,
    cfg: ExtendConfig,
    tally: _Tally,
) -> float:
    """Generate pings for ``cfg.seconds`` and wait for every transaction; return when the load started."""
    tasks: set[asyncio.Task[None]] = set()
    buffer: list[_Ping] = []
    sequence = 0
    backlog_limit = cfg.rate * _BACKLOG_SECONDS

    def _spawn() -> None:
        task = asyncio.create_task(_flush(list(buffer), engine=engine, publisher=publisher, cfg=cfg, tally=tally))
        tasks.add(task)
        task.add_done_callback(tasks.discard)
        buffer.clear()

    started = time.perf_counter()
    while (elapsed := time.perf_counter() - started) < cfg.seconds:
        if tally.offered - tally.committed - sum(tally.errors.values()) > backlog_limit:
            tally.saturated = True
            break
        while sequence < int(elapsed * cfg.rate):
            identity = sequence % cfg.timers
            sequence += 1
            if identity < cfg.silent_timers and elapsed >= cfg.silence_at:
                continue
            now = time.perf_counter()
            buffer.append(_Ping(identity, now, utcnow() + dt.timedelta(seconds=cfg.timeout_seconds)))
            tally.offered += 1
            if len(buffer) >= cfg.pings_per_transaction:
                _spawn()
        if buffer and time.perf_counter() - buffer[0].arrived >= cfg.flush_interval:
            _spawn()
        await asyncio.sleep(_TICK_SECONDS)
    tally.offered_seconds = time.perf_counter() - started
    if buffer:
        _spawn()
    await asyncio.gather(*tasks)
    return started


async def run_extend(dsn: str, cfg: ExtendConfig) -> ExtendResult:
    """Run one extend point against a fresh table and return its measurements."""
    engine = make_engine(dsn, pool_size=cfg.pool_size)
    subscriber_engine = make_engine(dsn)
    try:
        await ensure_extension(engine)
        metadata, table = await _create_table(engine, cfg)
        try:
            return await _measure(engine, subscriber_engine, table, cfg)
        finally:
            async with engine.begin() as conn:
                await conn.run_sync(metadata.drop_all)
    finally:
        await subscriber_engine.dispose()
        await engine.dispose()


async def _measure(
    engine: AsyncEngine, subscriber_engine: AsyncEngine, table: Table, cfg: ExtendConfig
) -> ExtendResult:
    tally = _Tally()
    publisher = OutboxBroker(outbox_table=table, logger=None)
    expirer = OutboxBroker(subscriber_engine, outbox_table=table, logger=None)

    @expirer.subscriber(cfg.queue, max_fetch_interval=cfg.max_fetch_interval)
    async def _expire(body: dict[str, typing.Any]) -> None:
        tally.lateness.append((utcnow() - dt.datetime.fromisoformat(body["deadline"])).total_seconds())
        if body["identity"] < cfg.silent_timers:
            tally.expired_silent += 1
            if tally.expired_silent >= cfg.silent_timers:
                tally.silent_fired.set()
        else:
            tally.false_expiries += 1

    await _checkpoint(engine)
    await assert_owns_database(engine)
    async with probe(engine, table.name) as sink:
        await expirer.start()
        try:
            started = await _offer_load(engine=engine, publisher=publisher, cfg=cfg, tally=tally)
            await _wait_for_silent_expiries(cfg, tally, started)
        finally:
            await expirer.stop()
            await subscriber_engine.dispose()
        wall = (tally.last_commit or time.perf_counter()) - started

    async with _autocommit(engine) as conn:
        times = (await conn.execute(_STATEMENT_TIMES_SQL)).all()
        vacuum = (await conn.execute(_VACUUM_SQL, {"table": table.name})).one()

    return ExtendResult(
        config=cfg,
        wall_seconds=wall,
        offered=tally.offered,
        offered_seconds=tally.offered_seconds,
        committed=tally.committed,
        transactions=tally.transactions,
        leased=tally.leased,
        errors=dict(tally.errors),
        saturated=tally.saturated,
        latency=percentiles(tally.latencies),
        pool_wait=percentiles(tally.pool_waits),
        lateness=percentiles(tally.lateness),
        expired_silent=tally.expired_silent,
        false_expiries=tally.false_expiries,
        statement_ms={row.op: (float(row.mean_ms or 0.0), float(row.max_ms or 0.0)) for row in times},
        autovacuum_count=int(vacuum.autovacuum_count),
        live_tuples=int(vacuum.n_live_tup),
        probe=sink[0],
    )


async def _wait_for_silent_expiries(cfg: ExtendConfig, tally: _Tally, started: float) -> None:
    """Wait until every silent timer fired, or until it is clearly late."""
    give_up = started + cfg.seconds + cfg.max_fetch_interval + 5.0
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(tally.silent_fired.wait(), timeout=max(0.0, give_up - time.perf_counter()))


def format_extend(results: typing.Sequence[ExtendResult]) -> str:
    """Render the extend points as two markdown tables: the write path, then table and expiry health."""
    write = [
        (
            "| point | offered/s | committed/s | commits/s | ping p50 / p95 / p99 ms | pool wait p99 ms "
            "| extend stmt mean / max ms | errors |"
        ),
        "|---|---|---|---|---|---|---|---|",
    ]
    health = [
        (
            "| point | WAL B/ping | WAL rec/ping | FPI | HOT % | dead tuples | autovacuums | index MB "
            "| fetch mean / max ms | silent fired | false expiries | lateness p50 / max s | leased |"
        ),
        "|---|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for r in results:
        cfg, p = r.config, r.probe
        span = max(r.wall_seconds, 1e-9)
        pings = max(r.committed, 1)
        insert_ms = r.statement_ms.get("insert", (0.0, 0.0))
        fetch_ms = r.statement_ms.get("with", (0.0, 0.0))
        errors = ", ".join(f"{k} {v}" for k, v in sorted(r.errors.items())) or "0"
        label = f"{cfg.name}{' (saturated)' if r.saturated else ''}"
        offered_rate = r.offered / max(r.offered_seconds, 1e-9)
        write.append(
            f"| {label} | {offered_rate:,.0f} | {r.committed / span:,.0f} | {r.transactions / span:,.0f} "
            f"| {r.latency['p50'] * 1e3:.1f} / {r.latency['p95'] * 1e3:.1f} / {r.latency['p99'] * 1e3:.1f} "
            f"| {r.pool_wait['p99'] * 1e3:.1f} | {insert_ms[0]:.3f} / {insert_ms[1]:.1f} | {errors} |",
        )
        hot = 100.0 * p.tup_hot_upd / p.tup_upd if p.tup_upd else 0.0
        health.append(
            f"| {cfg.name} | {p.wal_bytes / pings:,.0f} | {p.wal_records / pings:.2f} | {p.wal_fpi:,} | {hot:.0f} "
            f"| {p.dead_tup:,} | {r.autovacuum_count} | {p.index_bytes / 2**20:.1f} "
            f"| {fetch_ms[0]:.3f} / {fetch_ms[1]:.1f} | {r.expired_silent} / {cfg.silent_timers} "
            f"| {r.false_expiries} | {r.lateness['p50']:.2f} / {r.lateness['max']:.2f} | {r.leased} |",
        )
    return "\n".join([*write, "", *health])


def to_json(results: typing.Sequence[ExtendResult]) -> str:
    """Every field of every point, for anyone who wants more than the tables show."""
    return json.dumps([dataclasses.asdict(r) for r in results], indent=2, default=str)
