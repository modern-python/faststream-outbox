"""Integration tests against real Postgres. Requires docker-compose postgres up."""

import asyncio
import datetime as _dt
import uuid

import pytest
from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from faststream_outbox import (
    ConstantRetry,
    OutboxBroker,
    OutboxState,
    encode_payload,
    make_outbox_table,
)
from faststream_outbox.client import OutboxClient


pytestmark = pytest.mark.asyncio


async def _wait_until(predicate, *, timeout: float = 5.0) -> None:  # noqa: ASYNC109
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.05)
    msg = "timed out waiting for predicate"
    raise AssertionError(msg)


async def _row_count(engine: AsyncEngine, table) -> int:
    async with engine.connect() as conn:
        result = await conn.execute(select(table))
        return len(result.all())


async def test_validate_schema_passes_for_correct_table(pg_engine, outbox_table) -> None:
    client = OutboxClient(pg_engine, outbox_table)
    await client.validate_schema()  # should not raise


async def test_validate_schema_fails_for_missing_table(pg_engine) -> None:
    from sqlalchemy import MetaData  # noqa: PLC0415

    metadata = MetaData()
    table = make_outbox_table(metadata, table_name="does_not_exist_xyz")
    client = OutboxClient(pg_engine, table)
    with pytest.raises(RuntimeError, match="does not exist"):
        await client.validate_schema()


async def test_ping_succeeds(pg_engine, outbox_table) -> None:
    client = OutboxClient(pg_engine, outbox_table)
    assert await client.ping() is True


async def test_fetch_returns_pending_rows_only(pg_engine, outbox_table) -> None:
    async with pg_engine.begin() as conn:
        for i in range(3):
            await conn.execute(insert(outbox_table).values(queue="orders", payload=f"p-{i}".encode()))
    client = OutboxClient(pg_engine, outbox_table)
    rows = await client.fetch(["orders"], limit=10)
    assert len(rows) == 3
    assert {r.queue for r in rows} == {"orders"}
    assert all(r.acquired_token is not None for r in rows)


async def test_fetch_skips_other_queues(pg_engine, outbox_table) -> None:
    async with pg_engine.begin() as conn:
        await conn.execute(insert(outbox_table).values(queue="orders", payload=b"x"))
        await conn.execute(insert(outbox_table).values(queue="other", payload=b"y"))
    client = OutboxClient(pg_engine, outbox_table)
    rows = await client.fetch(["orders"], limit=10)
    assert len(rows) == 1
    assert rows[0].queue == "orders"


async def test_two_concurrent_fetches_dont_double_claim(pg_engine, outbox_table) -> None:
    async with pg_engine.begin() as conn:
        for i in range(20):
            await conn.execute(insert(outbox_table).values(queue="orders", payload=f"p-{i}".encode()))
    client = OutboxClient(pg_engine, outbox_table)

    async def fetch_n(n: int) -> list[int]:
        rows = await client.fetch(["orders"], limit=n)
        return [r.id for r in rows]

    results = await asyncio.gather(fetch_n(10), fetch_n(10))
    all_ids = sorted(results[0] + results[1])
    assert len(all_ids) == 20
    assert len(set(all_ids)) == 20  # no duplicates


async def test_delete_with_lease_succeeds_with_correct_token(pg_engine, outbox_table) -> None:
    async with pg_engine.begin() as conn:
        await conn.execute(insert(outbox_table).values(queue="orders", payload=b"x"))
    client = OutboxClient(pg_engine, outbox_table)
    rows = await client.fetch(["orders"], limit=1)
    assert len(rows) == 1
    deleted = await client.delete_with_lease(rows[0].id, rows[0].acquired_token)  # ty: ignore[invalid-argument-type]
    assert deleted is True
    assert await _row_count(pg_engine, outbox_table) == 0


async def test_delete_with_wrong_token_is_noop(pg_engine, outbox_table) -> None:
    async with pg_engine.begin() as conn:
        await conn.execute(insert(outbox_table).values(queue="orders", payload=b"x"))
    client = OutboxClient(pg_engine, outbox_table)
    rows = await client.fetch(["orders"], limit=1)
    deleted = await client.delete_with_lease(rows[0].id, uuid.uuid4())  # wrong token
    assert deleted is False
    assert await _row_count(pg_engine, outbox_table) == 1  # row still there


async def test_mark_pending_with_lease(pg_engine, outbox_table) -> None:
    async with pg_engine.begin() as conn:
        await conn.execute(insert(outbox_table).values(queue="orders", payload=b"x"))
    client = OutboxClient(pg_engine, outbox_table)
    rows = await client.fetch(["orders"], limit=1)
    msg = rows[0]
    future = _dt.datetime.now(tz=_dt.UTC) + _dt.timedelta(minutes=10)
    updated = await client.mark_pending_with_lease(
        msg.id,
        msg.acquired_token,  # ty: ignore[invalid-argument-type]
        next_attempt_at=future,
        attempts_count=1,
        first_attempt_at=_dt.datetime.now(tz=_dt.UTC),
        last_attempt_at=_dt.datetime.now(tz=_dt.UTC),
    )
    assert updated is True
    # Refetch — should be empty because next_attempt_at is in the future
    rows2 = await client.fetch(["orders"], limit=10)
    assert rows2 == []


async def test_release_stuck_recovers_old_processing_rows(pg_engine, outbox_table) -> None:
    async with pg_engine.begin() as conn:
        await conn.execute(insert(outbox_table).values(queue="orders", payload=b"x"))
    client = OutboxClient(pg_engine, outbox_table)
    rows = await client.fetch(["orders"], limit=1)
    assert rows
    # Backdate acquired_at so release_stuck picks it up
    backdate_sql = f"UPDATE \"{outbox_table.name}\" SET acquired_at = NOW() - INTERVAL '1 hour'"  # noqa: S608
    async with pg_engine.begin() as conn:
        await conn.exec_driver_sql(backdate_sql)
    released = await client.release_stuck(timeout_seconds=60)
    assert released == 1
    # Row should now be claimable again
    rows2 = await client.fetch(["orders"], limit=10)
    assert len(rows2) == 1


async def test_end_to_end_subscriber_delivers_inserted_row(pg_engine, outbox_table) -> None:
    received: list[dict] = []
    broker = OutboxBroker(pg_engine, outbox_table=outbox_table)

    @broker.subscriber("orders", min_fetch_interval=0.05, max_fetch_interval=0.5)
    async def handle(body: dict) -> None:
        received.append(body)

    session_factory = async_sessionmaker(pg_engine, expire_on_commit=False)
    async with broker:
        async with session_factory() as session, session.begin():
            payload, headers = encode_payload({"order_id": 7})
            await session.execute(insert(outbox_table).values(queue="orders", payload=payload, headers=headers))
        await _wait_until(lambda: received, timeout=5.0)

    assert received == [{"order_id": 7}]
    assert await _row_count(pg_engine, outbox_table) == 0


async def test_end_to_end_failing_handler_with_retry(pg_engine, outbox_table) -> None:
    attempts: list[int] = []
    broker = OutboxBroker(pg_engine, outbox_table=outbox_table)

    @broker.subscriber(
        "orders",
        min_fetch_interval=0.05,
        max_fetch_interval=0.2,
        retry_strategy=ConstantRetry(delay_seconds=0.1, max_attempts=3),
    )
    async def handle(body: dict) -> None:
        del body
        attempts.append(len(attempts))
        if len(attempts) < 3:
            msg = "transient"
            raise RuntimeError(msg)

    session_factory = async_sessionmaker(pg_engine, expire_on_commit=False)
    async with broker:
        async with session_factory() as session, session.begin():
            payload, headers = encode_payload({"x": 1})
            await session.execute(insert(outbox_table).values(queue="orders", payload=payload, headers=headers))
        await _wait_until(lambda: len(attempts) == 3, timeout=10.0)
        await _wait_until(lambda: True, timeout=0.2)
    assert await _row_count(pg_engine, outbox_table) == 0


async def test_subscriber_state_machine_uses_pending_value(outbox_table) -> None:
    """Sanity: the OutboxState constants match the column CHECK constraint."""
    states = list(outbox_table.c.state.constraints)
    assert any("'pending'" in str(c.sqltext) for c in states if hasattr(c, "sqltext"))
    assert OutboxState.PENDING.value == "pending"
    assert OutboxState.PROCESSING.value == "processing"
