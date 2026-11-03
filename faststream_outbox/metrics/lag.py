"""Opt-in Prometheus sampling of outbox queue lag."""

import asyncio
import logging
import typing
from collections.abc import AsyncGenerator, Mapping
from contextlib import asynccontextmanager

from sqlalchemy import bindparam, extract, func, select

from faststream_outbox.client import _claimable_branches, _seconds


if typing.TYPE_CHECKING:
    from prometheus_client import Gauge
    from sqlalchemy import Select, Table
    from sqlalchemy.ext.asyncio import AsyncEngine

_logger = logging.getLogger(__name__)


def _lag_statement(table: "Table") -> "Select[int, int]":
    """Select the oldest claimable row age and sample time from the database clock."""
    now = func.now()
    unleased, expired = _claimable_branches(table, now - _seconds("lease_ttl"))
    due = (
        select(table.c.next_attempt_at)
        .where(table.c.queue == bindparam("queue"), table.c.next_attempt_at <= now)
        .order_by(table.c.next_attempt_at)
        .limit(1)
    )
    pending = due.where(*unleased).scalar_subquery()
    overdue = due.where(*expired, table.c.next_attempt_at <= func.coalesce(pending, now)).scalar_subquery()
    return select(extract("epoch", now - func.least(pending, overdue)), extract("epoch", now))


@asynccontextmanager
async def _sample_queue_lag(
    engine: "AsyncEngine",
    table: "Table",
    *,
    age: "Gauge",
    freshness: "Gauge",
    lease_ttl_by_queue: Mapping[str, float],
    interval_seconds: float,
) -> AsyncGenerator[None, None]:
    """Sample the oldest claimable row for each queue until context exit."""
    statement = _lag_statement(table)
    for queue in lease_ttl_by_queue:
        age.labels(table.fullname, queue).set(float("nan"))
        freshness.labels(table.fullname, queue).set(0)

    async def sample() -> None:
        while True:
            for queue, ttl in lease_ttl_by_queue.items():
                try:
                    async with asyncio.timeout(min(interval_seconds, 5.0)), engine.connect() as connection:
                        delay, sampled_at = (
                            await connection.execute(statement, {"queue": queue, "lease_ttl": ttl})
                        ).one()
                    age.labels(table.fullname, queue).set(float(delay or 0))
                    freshness.labels(table.fullname, queue).set(float(sampled_at))
                except Exception:
                    _logger.warning("Failed to sample outbox lag for queue %r", queue, exc_info=True)
            await asyncio.sleep(interval_seconds)

    task = asyncio.create_task(sample())
    try:
        yield
    finally:
        task.cancel()
        try:
            await asyncio.gather(task, return_exceptions=True)
        finally:
            for queue in lease_ttl_by_queue:
                age.remove(table.fullname, queue)
                freshness.remove(table.fullname, queue)
