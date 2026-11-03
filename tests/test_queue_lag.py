import asyncio
from unittest.mock import patch
from uuid import uuid4

import pytest
from prometheus_client import CollectorRegistry
from sqlalchemy import Table, func, insert, text
from sqlalchemy.ext.asyncio import AsyncEngine

from faststream_outbox import OutboxBroker
from faststream_outbox.metrics.prometheus import PrometheusRecorder


@pytest.mark.asyncio
async def test_queue_lag_samples_only_claimable_rows(pg_engine: AsyncEngine, outbox_table: Table) -> None:
    async with pg_engine.begin() as conn:
        for queue, due, lease in [
            ("work", "-30 seconds", None),
            ("work", "-60 seconds", "-40 seconds"),
            ("work", "-90 seconds", "-1 second"),
            ("future", "1 hour", None),
        ]:
            await conn.execute(
                insert(outbox_table).values(
                    queue=queue,
                    payload=b"",
                    next_attempt_at=func.now() + text(f"interval '{due}'"),
                    acquired_at=func.now() + text(f"interval '{lease}'") if lease else None,
                    acquired_token=uuid4() if lease else None,
                )
            )
    registry = CollectorRegistry()
    age = "faststream_outbox_oldest_due_message_age_seconds"
    freshness = "faststream_outbox_queue_lag_last_success_timestamp_seconds"
    work = {"table": outbox_table.fullname, "queue": "work"}
    future = {"table": outbox_table.fullname, "queue": "future"}

    recorder = PrometheusRecorder(registry=registry, queue_lag_interval_seconds=60)
    broker = OutboxBroker(pg_engine, outbox_table=outbox_table, metrics_recorder=recorder)
    subscriber = broker.subscriber(["work", "future"], lease_ttl_seconds=20)

    with patch.object(subscriber, "start"):
        async with broker:
            await broker.start()
            async with asyncio.timeout(5):
                while not registry.get_sample_value(freshness, future):  # noqa: ASYNC110
                    await asyncio.sleep(0.01)
            sample = registry.get_sample_value(age, work)
            assert sample is not None
            assert 60 <= sample < 90
            assert registry.get_sample_value(age, future) == 0

    assert registry.get_sample_value(age, work) is None


def test_queue_lag_is_disabled_by_default() -> None:
    registry = CollectorRegistry()
    PrometheusRecorder(registry=registry)
    assert all("queue_lag" not in metric.name for metric in registry.collect())
    assert all("oldest_due_message_age" not in metric.name for metric in registry.collect())
