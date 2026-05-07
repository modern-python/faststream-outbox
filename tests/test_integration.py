"""Integration tests against real Postgres. Requires docker-compose postgres up."""

import asyncio
import datetime as _dt
import uuid
from unittest import mock

import pytest
from sqlalchemy import insert, select, text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker

from faststream_outbox import (
    ConstantRetry,
    OutboxBroker,
    make_outbox_table,
)
from faststream_outbox.client import OutboxClient
from faststream_outbox.envelope import _encode_payload as encode_payload


pytestmark = pytest.mark.asyncio


async def _wait_until(predicate, *, timeout: float = 5.0) -> None:  # noqa: ASYNC109
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.05)
    msg = "timed out waiting for predicate"  # pragma: no cover
    raise AssertionError(msg)  # pragma: no cover


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
    rows = await client.fetch(["orders"], limit=10, lease_ttl_seconds=60.0)
    assert len(rows) == 3
    assert {r.queue for r in rows} == {"orders"}
    assert all(r.acquired_token is not None for r in rows)


async def test_fetch_skips_other_queues(pg_engine, outbox_table) -> None:
    async with pg_engine.begin() as conn:
        await conn.execute(insert(outbox_table).values(queue="orders", payload=b"x"))
        await conn.execute(insert(outbox_table).values(queue="other", payload=b"y"))
    client = OutboxClient(pg_engine, outbox_table)
    rows = await client.fetch(["orders"], limit=10, lease_ttl_seconds=60.0)
    assert len(rows) == 1
    assert rows[0].queue == "orders"


async def test_two_concurrent_fetches_dont_double_claim(pg_engine, outbox_table) -> None:
    async with pg_engine.begin() as conn:
        for i in range(20):
            await conn.execute(insert(outbox_table).values(queue="orders", payload=f"p-{i}".encode()))
    client = OutboxClient(pg_engine, outbox_table)

    async def fetch_n(n: int) -> list[int]:
        rows = await client.fetch(["orders"], limit=n, lease_ttl_seconds=60.0)
        return [r.id for r in rows]

    results = await asyncio.gather(fetch_n(10), fetch_n(10))
    all_ids = sorted(results[0] + results[1])
    assert len(all_ids) == 20
    assert len(set(all_ids)) == 20  # no duplicates


async def test_delete_with_lease_succeeds_with_correct_token(pg_engine, outbox_table) -> None:
    async with pg_engine.begin() as conn:
        await conn.execute(insert(outbox_table).values(queue="orders", payload=b"x"))
    client = OutboxClient(pg_engine, outbox_table)
    rows = await client.fetch(["orders"], limit=1, lease_ttl_seconds=60.0)
    assert len(rows) == 1
    deleted = await client.delete_with_lease(rows[0].id, rows[0].acquired_token)  # ty: ignore[invalid-argument-type]
    assert deleted is True
    assert await _row_count(pg_engine, outbox_table) == 0


async def test_delete_with_wrong_token_is_noop(pg_engine, outbox_table) -> None:
    async with pg_engine.begin() as conn:
        await conn.execute(insert(outbox_table).values(queue="orders", payload=b"x"))
    client = OutboxClient(pg_engine, outbox_table)
    rows = await client.fetch(["orders"], limit=1, lease_ttl_seconds=60.0)
    deleted = await client.delete_with_lease(rows[0].id, uuid.uuid4())  # wrong token
    assert deleted is False
    assert await _row_count(pg_engine, outbox_table) == 1  # row still there


async def test_mark_pending_with_lease(pg_engine, outbox_table) -> None:
    async with pg_engine.begin() as conn:
        await conn.execute(insert(outbox_table).values(queue="orders", payload=b"x"))
    client = OutboxClient(pg_engine, outbox_table)
    rows = await client.fetch(["orders"], limit=1, lease_ttl_seconds=60.0)
    msg = rows[0]
    updated = await client.mark_pending_with_lease(
        msg.id,
        msg.acquired_token,  # ty: ignore[invalid-argument-type]
        delay_seconds=600.0,  # 10 minutes in the future
        attempts_count=1,
        first_attempt_at=_dt.datetime.now(tz=_dt.UTC),
        last_attempt_at=_dt.datetime.now(tz=_dt.UTC),
    )
    assert updated is True
    # Refetch — should be empty because next_attempt_at is in the future
    rows2 = await client.fetch(["orders"], limit=10, lease_ttl_seconds=60.0)
    assert rows2 == []


async def test_mark_pending_with_lease_uses_db_clock(pg_engine, outbox_table) -> None:
    """next_attempt_at must be computed server-side as now() + delay, not from the worker's clock."""
    async with pg_engine.begin() as conn:
        await conn.execute(insert(outbox_table).values(queue="orders", payload=b"x"))
    client = OutboxClient(pg_engine, outbox_table)
    rows = await client.fetch(["orders"], limit=1, lease_ttl_seconds=60.0)
    msg = rows[0]
    delay = 10.0
    # Use clock_timestamp(), not now(): now() returns transaction start time and
    # would freeze inside the outer connection.
    async with pg_engine.connect() as conn:
        db_before = (await conn.execute(text("SELECT clock_timestamp()"))).scalar()
    await client.mark_pending_with_lease(
        msg.id,
        msg.acquired_token,  # ty: ignore[invalid-argument-type]
        delay_seconds=delay,
        attempts_count=1,
        first_attempt_at=_dt.datetime.now(tz=_dt.UTC),
        last_attempt_at=_dt.datetime.now(tz=_dt.UTC),
    )
    async with pg_engine.connect() as conn:
        db_after = (await conn.execute(text("SELECT clock_timestamp()"))).scalar()
        next_at = (await conn.execute(select(outbox_table.c.next_attempt_at))).scalar_one()
    # next_attempt_at was set inside the mark_pending_with_lease transaction whose
    # now() falls between db_before and db_after.
    assert db_before + _dt.timedelta(seconds=delay) <= next_at <= db_after + _dt.timedelta(seconds=delay)


async def test_expired_lease_is_reclaimed_by_fetch(pg_engine, outbox_table) -> None:
    """A row whose lease has expired must be re-claimed by the next fetch with a fresh token."""
    async with pg_engine.begin() as conn:
        await conn.execute(insert(outbox_table).values(queue="orders", payload=b"x"))
    client = OutboxClient(pg_engine, outbox_table)
    first = await client.fetch(["orders"], limit=1, lease_ttl_seconds=60.0)
    assert first
    original_token = first[0].acquired_token
    # Backdate acquired_at so the lease is now considered expired by a 60s TTL.
    backdate_sql = f"UPDATE \"{outbox_table.name}\" SET acquired_at = NOW() - INTERVAL '1 hour'"  # noqa: S608
    async with pg_engine.begin() as conn:
        await conn.exec_driver_sql(backdate_sql)
    second = await client.fetch(["orders"], limit=1, lease_ttl_seconds=60.0)
    assert len(second) == 1
    assert second[0].id == first[0].id
    assert second[0].acquired_token != original_token  # fresh lease holder


async def test_unexpired_lease_is_not_reclaimed_by_fetch(pg_engine, outbox_table) -> None:
    """A still-valid lease must NOT be reclaimed by another fetch."""
    async with pg_engine.begin() as conn:
        await conn.execute(insert(outbox_table).values(queue="orders", payload=b"x"))
    client = OutboxClient(pg_engine, outbox_table)
    first = await client.fetch(["orders"], limit=1, lease_ttl_seconds=60.0)
    assert first
    # Lease was just set; a fresh fetch with a 60s TTL must find nothing.
    second = await client.fetch(["orders"], limit=1, lease_ttl_seconds=60.0)
    assert second == []


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

        # Wait for the row to be deleted while the broker is still running,
        # otherwise the broker stops and the worker's final DELETE may race
        # with shutdown.
        async def _check_deleted() -> bool:
            return await _row_count(pg_engine, outbox_table) == 0

        deadline = asyncio.get_event_loop().time() + 5.0
        while asyncio.get_event_loop().time() < deadline:
            if await _check_deleted():
                return
            await asyncio.sleep(0.1)  # pragma: no cover
        msg = "row not deleted within timeout"  # pragma: no cover
        raise AssertionError(msg)  # pragma: no cover


async def test_validate_schema_fails_when_columns_missing(pg_engine, outbox_table) -> None:
    """Drop a column the package expects and verify validate_schema reports it."""
    drop_sql = f'ALTER TABLE "{outbox_table.name}" DROP COLUMN headers'
    async with pg_engine.begin() as conn:
        await conn.exec_driver_sql(drop_sql)
    client = OutboxClient(pg_engine, outbox_table)
    with pytest.raises(RuntimeError, match="missing columns"):
        await client.validate_schema()


async def test_publish_inserts_in_caller_transaction(pg_engine, outbox_table) -> None:
    """``broker.publish`` must commit with the caller's transaction, not before."""
    broker = OutboxBroker(pg_engine, outbox_table=outbox_table)
    session_factory = async_sessionmaker(pg_engine, expire_on_commit=False)

    async with session_factory() as session:
        async with session.begin():
            await broker.publish({"order_id": 42}, queue="orders", session=session)
            # Mid-transaction: a separate connection must not see the row.
            count_during = await _row_count(pg_engine, outbox_table)
            assert count_during == 0
        # After commit (exited session.begin()): the row is visible.
        count_after = await _row_count(pg_engine, outbox_table)
        assert count_after == 1


async def test_publish_payload_is_decodable_by_subscriber(pg_engine, outbox_table) -> None:
    received: list[dict] = []
    broker = OutboxBroker(pg_engine, outbox_table=outbox_table)

    @broker.subscriber("orders", min_fetch_interval=0.05, max_fetch_interval=0.5)
    async def handle(body: dict) -> None:
        received.append(body)

    session_factory = async_sessionmaker(pg_engine, expire_on_commit=False)
    async with broker:
        async with session_factory() as session, session.begin():
            await broker.publish({"order_id": 7}, queue="orders", session=session)
        await _wait_until(lambda: received, timeout=5.0)

    assert received == [{"order_id": 7}]


async def test_publish_batch_inserts_all_rows(pg_engine, outbox_table) -> None:
    broker = OutboxBroker(pg_engine, outbox_table=outbox_table)
    session_factory = async_sessionmaker(pg_engine, expire_on_commit=False)

    async with session_factory() as session, session.begin():
        await broker.publish_batch(
            {"id": 1},
            {"id": 2},
            {"id": 3},
            queue="orders",
            session=session,
        )

    assert await _row_count(pg_engine, outbox_table) == 3


async def test_notify_wakes_subscriber_well_before_polling_interval(pg_engine, outbox_table) -> None:
    """LISTEN/NOTIFY must dispatch a freshly-published row long before the polling sleep elapses."""
    received: list[dict] = []
    broker = OutboxBroker(pg_engine, outbox_table=outbox_table)

    # Polling sleep ceiling is 10s; if NOTIFY works, dispatch happens in <500ms.
    @broker.subscriber("orders", min_fetch_interval=10.0, max_fetch_interval=10.0)
    async def handle(body: dict) -> None:
        received.append(body)

    session_factory = async_sessionmaker(pg_engine, expire_on_commit=False)
    async with broker:
        # Let the subscriber settle into its idle wait so we know it's blocked on the
        # polling sleep when NOTIFY arrives — proves the wake-up actually shortcuts.
        await asyncio.sleep(0.5)
        async with session_factory() as session, session.begin():
            await broker.publish({"order_id": 1}, queue="orders", session=session)
        # If NOTIFY wakeup works, this returns in tens of milliseconds. If it doesn't,
        # this would block for ~10s. A 2s budget cleanly distinguishes the two.
        await _wait_until(lambda: received, timeout=2.0)

    assert received == [{"order_id": 1}]


async def test_fetch_uses_persistent_connection(pg_engine, outbox_table) -> None:
    """Every fetch must land on the same backend pid — proves the connection is reused."""
    broker = OutboxBroker(pg_engine, outbox_table=outbox_table)
    received: list[dict] = []
    fetch_pids: list[int] = []
    original_fetch_with_conn = OutboxClient.fetch_with_conn

    async def tracking_fetch_with_conn(self, conn, queues, *, limit, lease_ttl_seconds):
        # Probe the pid in its own transaction so SQLAlchemy's autobegun txn is
        # closed before original_fetch_with_conn opens its own ``async with conn.begin():``.
        async with conn.begin():
            result = await conn.execute(text("SELECT pg_backend_pid()"))
            fetch_pids.append(result.scalar_one())
        return await original_fetch_with_conn(self, conn, queues, limit=limit, lease_ttl_seconds=lease_ttl_seconds)

    @broker.subscriber("orders", min_fetch_interval=0.01, max_fetch_interval=0.05)
    async def handle(body: dict) -> None:
        received.append(body)

    session_factory = async_sessionmaker(pg_engine, expire_on_commit=False)
    with mock.patch.object(OutboxClient, "fetch_with_conn", tracking_fetch_with_conn):
        async with broker:
            for i in range(5):
                async with session_factory() as session, session.begin():
                    await broker.publish({"i": i}, queue="orders", session=session)
            await _wait_until(lambda: len(received) == 5, timeout=5.0)

    assert len(fetch_pids) >= 3, f"fetch_with_conn should run multiple times, got {len(fetch_pids)}"
    assert len(set(fetch_pids)) == 1, f"persistent connection should hold one pid, saw {set(fetch_pids)}"


async def test_publish_with_activate_in_delays_delivery(pg_engine, outbox_table) -> None:
    """Handler must not see the message until activate_in elapses."""
    received: list[dict] = []
    broker = OutboxBroker(pg_engine, outbox_table=outbox_table)

    @broker.subscriber("orders", min_fetch_interval=0.05, max_fetch_interval=0.2)
    async def handle(body: dict) -> None:
        received.append(body)

    session_factory = async_sessionmaker(pg_engine, expire_on_commit=False)
    async with broker:
        async with session_factory() as session, session.begin():
            await broker.publish(
                {"order_id": 1},
                queue="orders",
                session=session,
                activate_in=_dt.timedelta(milliseconds=500),
            )
        # Before the gate opens: nothing delivered.
        await asyncio.sleep(0.2)
        assert received == []
        # After the gate opens: delivered.
        await _wait_until(lambda: received, timeout=3.0)
    assert received == [{"order_id": 1}]


async def test_publish_with_activate_at_delays_delivery(pg_engine, outbox_table) -> None:
    received: list[dict] = []
    broker = OutboxBroker(pg_engine, outbox_table=outbox_table)

    @broker.subscriber("orders", min_fetch_interval=0.05, max_fetch_interval=0.2)
    async def handle(body: dict) -> None:
        received.append(body)

    fire_at = _dt.datetime.now(tz=_dt.UTC) + _dt.timedelta(milliseconds=500)
    session_factory = async_sessionmaker(pg_engine, expire_on_commit=False)
    async with broker:
        async with session_factory() as session, session.begin():
            await broker.publish(
                {"order_id": 2},
                queue="orders",
                session=session,
                activate_at=fire_at,
            )
        await asyncio.sleep(0.2)
        assert received == []
        await _wait_until(lambda: received, timeout=3.0)
    assert received == [{"order_id": 2}]


async def test_publish_rejects_activate_in_and_at_together(pg_engine, outbox_table) -> None:
    broker = OutboxBroker(pg_engine, outbox_table=outbox_table)
    session_factory = async_sessionmaker(pg_engine, expire_on_commit=False)
    async with session_factory() as session, session.begin():
        with pytest.raises(ValueError, match="activate_in / activate_at"):
            await broker.publish(
                {"x": 1},
                queue="orders",
                session=session,
                activate_in=_dt.timedelta(seconds=1),
                activate_at=_dt.datetime.now(tz=_dt.UTC),
            )


async def test_publish_with_timer_id_dedups(pg_engine, outbox_table) -> None:
    """Re-publishing the same (queue, timer_id) is a no-op — handler invoked exactly once."""
    received: list[dict] = []
    broker = OutboxBroker(pg_engine, outbox_table=outbox_table)

    @broker.subscriber("orders", min_fetch_interval=0.05, max_fetch_interval=0.2)
    async def handle(body: dict) -> None:
        received.append(body)

    session_factory = async_sessionmaker(pg_engine, expire_on_commit=False)
    async with broker:
        async with session_factory() as session, session.begin():
            first_id = await broker.publish(
                {"v": 1},
                queue="orders",
                session=session,
                timer_id="email-1",
            )
        async with session_factory() as session, session.begin():
            second_id = await broker.publish(
                {"v": 2},
                queue="orders",
                session=session,
                timer_id="email-1",
            )
        await _wait_until(lambda: received, timeout=3.0)

    assert first_id is not None
    assert second_id is None  # second insert was a no-op
    assert received == [{"v": 1}]  # second body was never delivered


async def test_publish_timer_id_distinct_queues_are_independent(pg_engine, outbox_table) -> None:
    broker = OutboxBroker(pg_engine, outbox_table=outbox_table)
    session_factory = async_sessionmaker(pg_engine, expire_on_commit=False)
    async with session_factory() as session, session.begin():
        a = await broker.publish({"x": 1}, queue="q1", session=session, timer_id="dup")
        b = await broker.publish({"x": 2}, queue="q2", session=session, timer_id="dup")
    assert a is not None
    assert b is not None
    assert await _row_count(pg_engine, outbox_table) == 2


async def test_cancel_timer_before_fire_prevents_delivery(pg_engine, outbox_table) -> None:
    received: list[dict] = []
    broker = OutboxBroker(pg_engine, outbox_table=outbox_table)

    @broker.subscriber("orders", min_fetch_interval=0.05, max_fetch_interval=0.2)
    async def handle(body: dict) -> None:
        received.append(body)  # pragma: no cover  # cancellation must prevent this

    session_factory = async_sessionmaker(pg_engine, expire_on_commit=False)
    async with broker:
        async with session_factory() as session, session.begin():
            await broker.publish(
                {"order_id": 9},
                queue="orders",
                session=session,
                activate_in=_dt.timedelta(seconds=1),
                timer_id="email-cancel",
            )
        # Cancel before activate_in elapses.
        async with session_factory() as session, session.begin():
            cancelled = await broker.cancel_timer(queue="orders", timer_id="email-cancel", session=session)
        assert cancelled is True
        # Wait past the original fire time — handler must never see the row.
        await asyncio.sleep(1.5)
    assert received == []
    assert await _row_count(pg_engine, outbox_table) == 0


async def test_cancel_timer_after_lease_taken_returns_false(pg_engine, outbox_table) -> None:
    """If the row's already in flight, cancel is a no-op and delivery completes."""
    received: list[dict] = []
    broker = OutboxBroker(pg_engine, outbox_table=outbox_table)
    handler_started = asyncio.Event()
    release_handler = asyncio.Event()

    @broker.subscriber("orders", min_fetch_interval=0.01, max_fetch_interval=0.05)
    async def handle(body: dict) -> None:
        handler_started.set()
        await release_handler.wait()
        received.append(body)

    session_factory = async_sessionmaker(pg_engine, expire_on_commit=False)
    async with broker:
        async with session_factory() as session, session.begin():
            await broker.publish(
                {"v": "in-flight"},
                queue="orders",
                session=session,
                timer_id="cant-cancel",
            )
        # Wait for the handler to enter; the row is now leased.
        await asyncio.wait_for(handler_started.wait(), timeout=3.0)
        async with session_factory() as session, session.begin():
            cancelled = await broker.cancel_timer(queue="orders", timer_id="cant-cancel", session=session)
        assert cancelled is False  # lease guard prevented the DELETE
        release_handler.set()
        await _wait_until(lambda: received, timeout=3.0)
    assert received == [{"v": "in-flight"}]


async def test_cancel_timer_unknown_returns_false(pg_engine, outbox_table) -> None:
    broker = OutboxBroker(pg_engine, outbox_table=outbox_table)
    session_factory = async_sessionmaker(pg_engine, expire_on_commit=False)
    async with session_factory() as session, session.begin():
        cancelled = await broker.cancel_timer(queue="orders", timer_id="nope", session=session)
    assert cancelled is False


async def test_publish_returns_inserted_row_id(pg_engine, outbox_table) -> None:
    broker = OutboxBroker(pg_engine, outbox_table=outbox_table)
    session_factory = async_sessionmaker(pg_engine, expire_on_commit=False)
    async with session_factory() as session, session.begin():
        row_id = await broker.publish({"x": 1}, queue="orders", session=session)
    assert isinstance(row_id, int)
    assert row_id > 0


async def test_publish_batch_with_activate_in_delays_all_rows(pg_engine, outbox_table) -> None:
    broker = OutboxBroker(pg_engine, outbox_table=outbox_table)
    session_factory = async_sessionmaker(pg_engine, expire_on_commit=False)
    async with session_factory() as session, session.begin():
        await broker.publish_batch(
            {"i": 1},
            {"i": 2},
            {"i": 3},
            queue="orders",
            session=session,
            activate_in=_dt.timedelta(minutes=10),  # well past any test horizon
        )
    # Rows are inserted but invisible to fetch (next_attempt_at in the future).
    assert await _row_count(pg_engine, outbox_table) == 3
    client = OutboxClient(pg_engine, outbox_table)
    assert await client.fetch(["orders"], limit=10, lease_ttl_seconds=60.0) == []


async def test_notify_payload_carries_queue_name(pg_engine, outbox_table) -> None:
    """The NOTIFY payload Postgres delivers to LISTEN clients must equal the queue name."""
    received_payloads: list[str] = []
    received_event = asyncio.Event()

    # Open a raw asyncpg listener on the same channel the broker NOTIFYs on.
    import asyncpg  # noqa: PLC0415

    dsn = pg_engine.url.set(drivername="postgresql").render_as_string(hide_password=False)
    listener = await asyncpg.connect(dsn)
    try:

        def _cb(_conn, _pid, _channel, payload) -> None:
            received_payloads.append(payload)
            received_event.set()

        await listener.add_listener(f"outbox_{outbox_table.name}", _cb)

        broker = OutboxBroker(pg_engine, outbox_table=outbox_table)
        session_factory = async_sessionmaker(pg_engine, expire_on_commit=False)
        async with session_factory() as session, session.begin():
            await broker.publish({"x": 1}, queue="orders", session=session)
        # Wait for the NOTIFY to land on our listener
        await asyncio.wait_for(received_event.wait(), timeout=2.0)
    finally:
        await listener.close()

    assert received_payloads == ["orders"]
