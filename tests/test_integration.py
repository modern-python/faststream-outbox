"""Integration tests against real Postgres. Requires docker-compose postgres up."""

import asyncio
import datetime as _dt
import json
import uuid
from unittest import mock

import pytest
from sqlalchemy import MetaData, Table, event, insert, select, text
from sqlalchemy.ext.asyncio import AsyncEngine, async_sessionmaker, create_async_engine

from faststream_outbox import (
    ConstantRetry,
    OutboxBroker,
    make_outbox_table,
)
from faststream_outbox.client import OutboxClient
from faststream_outbox.envelope import _encode_payload as encode_payload
from faststream_outbox.testing import FakeOutboxClient


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
    async with pg_engine.connect() as conn:
        rows = await client.fetch(conn, ["orders"], limit=10, lease_ttl_seconds=60.0)
    assert len(rows) == 3
    assert {r.queue for r in rows} == {"orders"}
    assert all(r.acquired_token is not None for r in rows)


async def test_fetch_skips_other_queues(pg_engine, outbox_table) -> None:
    async with pg_engine.begin() as conn:
        await conn.execute(insert(outbox_table).values(queue="orders", payload=b"x"))
        await conn.execute(insert(outbox_table).values(queue="other", payload=b"y"))
    client = OutboxClient(pg_engine, outbox_table)
    async with pg_engine.connect() as conn:
        rows = await client.fetch(conn, ["orders"], limit=10, lease_ttl_seconds=60.0)
    assert len(rows) == 1
    assert rows[0].queue == "orders"


async def test_two_concurrent_fetches_dont_double_claim(pg_engine, outbox_table) -> None:
    async with pg_engine.begin() as conn:
        for i in range(20):
            await conn.execute(insert(outbox_table).values(queue="orders", payload=f"p-{i}".encode()))
    client = OutboxClient(pg_engine, outbox_table)

    async def fetch_n(n: int) -> list[int]:
        async with pg_engine.connect() as conn:
            rows = await client.fetch(conn, ["orders"], limit=n, lease_ttl_seconds=60.0)
        return [r.id for r in rows]

    results = await asyncio.gather(fetch_n(10), fetch_n(10))
    all_ids = sorted(results[0] + results[1])
    assert len(all_ids) == 20
    assert len(set(all_ids)) == 20  # no duplicates


async def test_delete_with_lease_succeeds_with_correct_token(pg_engine: AsyncEngine, outbox_table: Table) -> None:
    async with pg_engine.begin() as conn:
        await conn.execute(insert(outbox_table).values(queue="orders", payload=b"x"))
    client = OutboxClient(pg_engine, outbox_table)
    async with pg_engine.connect() as fetch_conn:
        rows = await client.fetch(fetch_conn, ["orders"], limit=1, lease_ttl_seconds=60.0)
    assert len(rows) == 1
    # delete_with_lease expects an AUTOCOMMIT-configured conn (the production writer
    # conn from _open_worker_resources) — same shape here.
    async with pg_engine.connect() as raw_conn:
        writer_conn = await raw_conn.execution_options(isolation_level="AUTOCOMMIT")
        deleted = await client.delete_with_lease(writer_conn, rows[0].id, rows[0].acquired_token)  # ty: ignore[invalid-argument-type]
    assert deleted is True
    assert await _row_count(pg_engine, outbox_table) == 0


async def test_delete_with_wrong_token_is_noop(pg_engine: AsyncEngine, outbox_table: Table) -> None:
    async with pg_engine.begin() as conn:
        await conn.execute(insert(outbox_table).values(queue="orders", payload=b"x"))
    client = OutboxClient(pg_engine, outbox_table)
    async with pg_engine.connect() as fetch_conn:
        rows = await client.fetch(fetch_conn, ["orders"], limit=1, lease_ttl_seconds=60.0)
    async with pg_engine.connect() as raw_conn:
        writer_conn = await raw_conn.execution_options(isolation_level="AUTOCOMMIT")
        deleted = await client.delete_with_lease(writer_conn, rows[0].id, uuid.uuid4())  # wrong token
    assert deleted is False
    assert await _row_count(pg_engine, outbox_table) == 1  # row still there


async def test_writer_connection_autocommit_round_trip(pg_engine: AsyncEngine, outbox_table: Table) -> None:
    """
    Autocommit-configured writer conn runs ``delete_with_lease`` end-to-end against real Postgres.

    The connection is configured exactly the way ``_open_worker_resources`` configures the
    worker writer (``isolation_level="AUTOCOMMIT"``); ``delete_with_lease`` runs with no
    outer ``conn.begin()`` and the write commits on its own — proves the autocommit setup
    is valid on the asyncpg dialect.
    """
    token = uuid.uuid4()
    now = _dt.datetime.now(tz=_dt.UTC)
    async with pg_engine.begin() as conn:
        result = await conn.execute(
            insert(outbox_table)
            .values(queue="orders", payload=b"x", acquired_at=now, acquired_token=token)
            .returning(outbox_table.c.id),
        )
        row_id = result.scalar_one()

    client = OutboxClient(pg_engine, outbox_table)
    async with pg_engine.connect() as raw_conn:
        writer_conn = await raw_conn.execution_options(isolation_level="AUTOCOMMIT")
        deleted = await client.delete_with_lease(writer_conn, row_id, token)
        # Still inside the autocommit conn — under default isolation the DELETE would
        # be buffered until conn.close(); under AUTOCOMMIT it should already be visible.
        async with pg_engine.connect() as probe_conn:
            remaining = await probe_conn.execute(select(outbox_table).where(outbox_table.c.id == row_id))
            assert remaining.first() is None
    assert deleted is True
    assert await _row_count(pg_engine, outbox_table) == 0


async def test_mark_pending_with_lease(pg_engine: AsyncEngine, outbox_table: Table) -> None:
    async with pg_engine.begin() as conn:
        await conn.execute(insert(outbox_table).values(queue="orders", payload=b"x"))
    client = OutboxClient(pg_engine, outbox_table)
    async with pg_engine.connect() as fetch_conn:
        rows = await client.fetch(fetch_conn, ["orders"], limit=1, lease_ttl_seconds=60.0)
    msg = rows[0]
    # mark_pending_with_lease expects an AUTOCOMMIT-configured conn (production writer conn).
    async with pg_engine.connect() as raw_conn:
        writer_conn = await raw_conn.execution_options(isolation_level="AUTOCOMMIT")
        updated = await client.mark_pending_with_lease(
            writer_conn,
            msg.id,
            msg.acquired_token,  # ty: ignore[invalid-argument-type]
            delay_seconds=600.0,  # 10 minutes in the future
            attempts_count=1,
            first_attempt_at=_dt.datetime.now(tz=_dt.UTC),
            last_attempt_at=_dt.datetime.now(tz=_dt.UTC),
        )
    assert updated is True
    # Refetch — should be empty because next_attempt_at is in the future
    async with pg_engine.connect() as fetch_conn:
        rows2 = await client.fetch(fetch_conn, ["orders"], limit=10, lease_ttl_seconds=60.0)
    assert rows2 == []


async def test_mark_pending_with_lease_uses_db_clock(pg_engine: AsyncEngine, outbox_table: Table) -> None:
    """next_attempt_at must be computed server-side as now() + delay, not from the worker's clock."""
    async with pg_engine.begin() as conn:
        await conn.execute(insert(outbox_table).values(queue="orders", payload=b"x"))
    client = OutboxClient(pg_engine, outbox_table)
    delay = 10.0
    async with pg_engine.connect() as conn:
        rows = await client.fetch(conn, ["orders"], limit=1, lease_ttl_seconds=60.0)
    msg = rows[0]
    # Use clock_timestamp(), not now(): now() returns transaction start time and
    # would freeze inside the outer connection.
    async with pg_engine.connect() as conn:
        db_before = (await conn.execute(text("SELECT clock_timestamp()"))).scalar()
    # mark_pending_with_lease expects an AUTOCOMMIT-configured conn (production writer conn).
    async with pg_engine.connect() as raw_conn:
        writer_conn = await raw_conn.execution_options(isolation_level="AUTOCOMMIT")
        await client.mark_pending_with_lease(
            writer_conn,
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
    # next_attempt_at was set by the autocommit'd UPDATE whose server-side
    # now() falls between db_before and db_after.
    assert db_before + _dt.timedelta(seconds=delay) <= next_at <= db_after + _dt.timedelta(seconds=delay)


async def test_expired_lease_is_reclaimed_by_fetch(pg_engine, outbox_table) -> None:
    """A row whose lease has expired must be re-claimed by the next fetch with a fresh token."""
    async with pg_engine.begin() as conn:
        await conn.execute(insert(outbox_table).values(queue="orders", payload=b"x"))
    client = OutboxClient(pg_engine, outbox_table)
    async with pg_engine.connect() as conn:
        first = await client.fetch(conn, ["orders"], limit=1, lease_ttl_seconds=60.0)
    assert first
    original_token = first[0].acquired_token
    # Backdate acquired_at so the lease is now considered expired by a 60s TTL.
    backdate_sql = f"UPDATE \"{outbox_table.name}\" SET acquired_at = NOW() - INTERVAL '1 hour'"  # noqa: S608
    async with pg_engine.begin() as conn:
        await conn.exec_driver_sql(backdate_sql)
    async with pg_engine.connect() as conn:
        second = await client.fetch(conn, ["orders"], limit=1, lease_ttl_seconds=60.0)
    assert len(second) == 1
    assert second[0].id == first[0].id
    assert second[0].acquired_token != original_token  # fresh lease holder


async def test_unexpired_lease_is_not_reclaimed_by_fetch(pg_engine, outbox_table) -> None:
    """A still-valid lease must NOT be reclaimed by another fetch."""
    async with pg_engine.begin() as conn:
        await conn.execute(insert(outbox_table).values(queue="orders", payload=b"x"))
    client = OutboxClient(pg_engine, outbox_table)
    async with pg_engine.connect() as conn:
        first = await client.fetch(conn, ["orders"], limit=1, lease_ttl_seconds=60.0)
    assert first
    # Lease was just set; a fresh fetch with a 60s TTL must find nothing.
    async with pg_engine.connect() as conn:
        second = await client.fetch(conn, ["orders"], limit=1, lease_ttl_seconds=60.0)
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
            # pragma: empirically unreachable — the worker DELETE lands within
            # the first poll on current CI. The branch exists as a safety valve
            # for slower hardware, not a tested path.
            await asyncio.sleep(0.1)  # pragma: no cover
        msg = "row not deleted within timeout"  # pragma: no cover
        raise AssertionError(msg)  # pragma: no cover


async def test_validate_schema_fails_when_columns_missing(pg_engine, outbox_table) -> None:
    """Drop a column the package expects and verify validate_schema reports it."""
    drop_sql = f'ALTER TABLE "{outbox_table.name}" DROP COLUMN headers'
    async with pg_engine.begin() as conn:
        await conn.exec_driver_sql(drop_sql)
    client = OutboxClient(pg_engine, outbox_table)
    with pytest.raises(RuntimeError, match="missing column 'headers'"):
        await client.validate_schema()


async def test_validate_schema_fails_when_timer_id_unique_index_missing(pg_engine, outbox_table) -> None:
    """
    Missing partial unique index on (queue, timer_id) must be caught before runtime.

    Without it, ``publish(timer_id=…)`` raises ``InvalidColumnReference`` on first call.
    """
    drop_sql = f'DROP INDEX "{outbox_table.name}_timer_id_uq"'
    async with pg_engine.begin() as conn:
        await conn.exec_driver_sql(drop_sql)
    client = OutboxClient(pg_engine, outbox_table)
    with pytest.raises(RuntimeError, match="missing index"):
        await client.validate_schema()


async def test_validate_schema_fails_when_pending_index_missing(pg_engine, outbox_table) -> None:
    """Missing the fetch partial index degrades to seq-scan; validator must surface it."""
    drop_sql = f'DROP INDEX "{outbox_table.name}_pending_idx"'
    async with pg_engine.begin() as conn:
        await conn.exec_driver_sql(drop_sql)
    client = OutboxClient(pg_engine, outbox_table)
    with pytest.raises(RuntimeError, match="missing index"):
        await client.validate_schema()


async def test_validate_schema_fails_when_lease_index_missing(pg_engine, outbox_table) -> None:
    """Missing the expired-lease partial index sends the fetch CTE back to seq-scan; flag it."""
    drop_sql = f'DROP INDEX "{outbox_table.name}_lease_idx"'
    async with pg_engine.begin() as conn:
        await conn.exec_driver_sql(drop_sql)
    client = OutboxClient(pg_engine, outbox_table)
    with pytest.raises(RuntimeError, match="missing index"):
        await client.validate_schema()


async def test_validate_schema_fails_when_column_type_wrong(pg_engine, outbox_table) -> None:
    """``payload`` typed as ``TEXT`` instead of ``BYTEA`` corrupts inserts; catch it early."""
    alter_sql = f"ALTER TABLE \"{outbox_table.name}\" ALTER COLUMN payload TYPE TEXT USING encode(payload, 'escape')"
    async with pg_engine.begin() as conn:
        await conn.exec_driver_sql(alter_sql)
    client = OutboxClient(pg_engine, outbox_table)
    with pytest.raises(RuntimeError, match="type mismatch"):
        await client.validate_schema()


async def test_validate_schema_fails_when_nullability_changed(pg_engine, outbox_table) -> None:
    """ALTER COLUMN ... DROP NOT NULL must be caught by validate_schema()."""
    alter_sql = f'ALTER TABLE "{outbox_table.name}" ALTER COLUMN payload DROP NOT NULL'
    async with pg_engine.begin() as conn:
        await conn.exec_driver_sql(alter_sql)
    client = OutboxClient(pg_engine, outbox_table)
    with pytest.raises(RuntimeError, match="nullability mismatch"):
        await client.validate_schema()


async def test_validate_schema_ignores_user_added_extras(pg_engine, outbox_table) -> None:
    """
    Extra columns / indexes the user adds to their outbox table must NOT fail validation.

    Users may add audit columns or their own indexes; the validator's contract is to flag
    *missing* schema only, not extras.
    """
    async with pg_engine.begin() as conn:
        await conn.exec_driver_sql(f'ALTER TABLE "{outbox_table.name}" ADD COLUMN audit_user_id BIGINT')
        await conn.exec_driver_sql(
            f'CREATE INDEX "{outbox_table.name}_audit_idx" ON "{outbox_table.name}" (audit_user_id)'
        )
    client = OutboxClient(pg_engine, outbox_table)
    await client.validate_schema()  # must not raise


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
    original_fetch = OutboxClient.fetch

    async def tracking_fetch(self, conn, queues, *, limit, lease_ttl_seconds):
        # Read the backend pid synchronously off the underlying asyncpg
        # connection. No await before the append means broker-shutdown
        # cancellation can't interrupt the probe mid-flight, so coverage
        # on this wrapper is deterministic across runners.
        fetch_pids.append(conn.sync_connection.connection.driver_connection.get_server_pid())
        return await original_fetch(self, conn, queues, limit=limit, lease_ttl_seconds=lease_ttl_seconds)

    @broker.subscriber("orders", min_fetch_interval=0.01, max_fetch_interval=0.05)
    async def handle(body: dict) -> None:
        received.append(body)

    session_factory = async_sessionmaker(pg_engine, expire_on_commit=False)
    with mock.patch.object(OutboxClient, "fetch", tracking_fetch):
        async with broker:
            for i in range(5):
                async with session_factory() as session, session.begin():
                    await broker.publish({"i": i}, queue="orders", session=session)
            await _wait_until(lambda: len(received) == 5, timeout=5.0)

    assert len(fetch_pids) >= 3, f"fetch should run multiple times, got {len(fetch_pids)}"
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
    async with pg_engine.connect() as conn:
        assert await client.fetch(conn, ["orders"], limit=10, lease_ttl_seconds=60.0) == []


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


async def test_fetch_unprocessed_returns_all_queues(pg_engine, outbox_table) -> None:
    broker = OutboxBroker(pg_engine, outbox_table=outbox_table)
    session_factory = async_sessionmaker(pg_engine, expire_on_commit=False)
    async with session_factory() as session, session.begin():
        await broker.publish("o-1", queue="orders", session=session)
        await broker.publish("o-2", queue="orders", session=session)
        await broker.publish("s-1", queue="shipments", session=session)

    async with session_factory() as session:
        rows = await broker.fetch_unprocessed(session=session)

    assert [r.queue for r in rows] == ["orders", "orders", "shipments"]
    assert [r.id for r in rows] == sorted(r.id for r in rows)  # ordered by id


async def test_fetch_unprocessed_filters_by_queue(pg_engine, outbox_table) -> None:
    broker = OutboxBroker(pg_engine, outbox_table=outbox_table)
    session_factory = async_sessionmaker(pg_engine, expire_on_commit=False)
    async with session_factory() as session, session.begin():
        await broker.publish("o-1", queue="orders", session=session)
        await broker.publish("s-1", queue="shipments", session=session)

    async with session_factory() as session:
        orders = await broker.fetch_unprocessed(session=session, queue="orders")

    assert len(orders) == 1
    assert orders[0].queue == "orders"


async def test_fetch_unprocessed_includes_future_dated_rows(pg_engine, outbox_table) -> None:
    """Future-dated rows (activate_in) are unprocessed too — fetch_unprocessed must surface them."""
    broker = OutboxBroker(pg_engine, outbox_table=outbox_table)
    session_factory = async_sessionmaker(pg_engine, expire_on_commit=False)
    async with session_factory() as session, session.begin():
        await broker.publish("now", queue="orders", session=session)
        await broker.publish(
            "later",
            queue="orders",
            session=session,
            activate_in=_dt.timedelta(minutes=5),
        )

    async with session_factory() as session:
        rows = await broker.fetch_unprocessed(session=session, queue="orders")

    assert len(rows) == 2
    now = _dt.datetime.now(tz=_dt.UTC)
    future = [r for r in rows if r.next_attempt_at > now + _dt.timedelta(minutes=1)]
    assert len(future) == 1


async def test_fetch_unprocessed_reads_uncommitted_writes_in_same_session(pg_engine, outbox_table) -> None:
    """Same-session contract: a read inside the producer's open transaction sees its own writes."""
    broker = OutboxBroker(pg_engine, outbox_table=outbox_table)
    session_factory = async_sessionmaker(pg_engine, expire_on_commit=False)
    async with session_factory() as session, session.begin():
        await broker.publish("pre-commit", queue="orders", session=session)
        rows = await broker.fetch_unprocessed(session=session)
        assert len(rows) == 1
        assert rows[0].queue == "orders"


async def test_terminal_writes_reuse_writer_conn_under_load(pg_engine, outbox_table) -> None:
    """
    M3 — per-worker cached writer conn drains N rows without N pool checkouts.

    A drain of N rows must trigger O(workers) pool checkouts during the steady-state
    drain, not one per row (the pre-M3 behavior). With ``max_workers=1`` and one fetch
    loop, the broker holds exactly two connections during the drain: one fetch conn
    and one cached writer conn. We count checkouts via SQLAlchemy's ``checkout`` event
    on ``pg_engine.sync_engine`` — ``AsyncEngine.connect`` itself is read-only and
    can't be patched directly.
    """
    broker = OutboxBroker(pg_engine, outbox_table=outbox_table)
    n_rows = 50
    received: list[int] = []

    @broker.subscriber("orders", min_fetch_interval=0.02, max_fetch_interval=0.1)
    async def handle(body: dict) -> None:
        received.append(body["i"])

    session_factory = async_sessionmaker(pg_engine, expire_on_commit=False)
    # Seed BEFORE attaching the listener so seed checkouts don't count.
    async with session_factory() as session, session.begin():
        for i in range(n_rows):
            payload, headers = encode_payload({"i": i})
            await session.execute(insert(outbox_table).values(queue="orders", payload=payload, headers=headers))

    checkouts: list[None] = []

    def _on_checkout(*_args: object) -> None:
        checkouts.append(None)

    # Attach BEFORE entering the broker so the initial fetch + writer conn checkouts
    # register and the listener body executes — otherwise M3 works so well there are
    # zero checkouts during the drain.
    event.listen(pg_engine.sync_engine, "checkout", _on_checkout)
    try:
        async with broker:
            # Pre-M3: ~50+ checkouts (one per terminal DELETE). Post-M3: writer holds
            # a single conn across all rows, so total checkouts ~= 1 fetch + 1 writer +
            # a small constant of health-probe / NOTIFY churn.
            await _wait_until(lambda: len(received) == n_rows, timeout=15.0)
            # Brief settle so any in-flight terminal DELETE finishes before shutdown.
            await asyncio.sleep(0.2)
    finally:
        event.remove(pg_engine.sync_engine, "checkout", _on_checkout)

    # Fetch CTE has no secondary sort key (L2), so rows with identical next_attempt_at
    # claim in non-deterministic order — assert the set, not the sequence.
    assert sorted(received) == list(range(n_rows))
    assert await _row_count(pg_engine, outbox_table) == 0
    # Allow up to 10 to absorb startup churn — the invariant we're asserting is
    # "O(workers), not O(rows)". Pre-M3 this would be 50+.
    assert len(checkouts) <= 10, (
        f"pool checkouts during {n_rows}-row drain: {len(checkouts)}; expected O(workers), not O(rows)"
    )


async def test_fake_and_real_fetch_agree_on_eligibility_predicate(pg_engine, outbox_table) -> None:
    """
    T1 — fake/real predicate parity across the five eligibility states.

    ``OutboxClient.fetch`` (SQL) and ``FakeOutboxClient.fetch`` (Python) compute
    eligibility independently; without this test, drift between them is silent —
    unit tests green, production red. The five states exercised: unleased,
    future-dated, leased-fresh (within TTL), leased-expired (past TTL),
    queue-mismatch.
    """
    lease_ttl = 60.0
    queues_to_fetch = ["orders"]
    # Each spec packs label, queue, next_attempt offset (s), and acquired-age (s) or None.
    specs: list[tuple[str, str, float, float | None]] = [
        ("unleased", "orders", -1.0, None),
        ("future", "orders", 60.0, None),
        ("leased-fresh", "orders", -1.0, 5.0),
        ("leased-expired", "orders", -1.0, 120.0),
        ("queue-mismatch", "other", -1.0, None),
    ]
    expected_eligible = {"unleased", "leased-expired"}

    # Real side — server-side ``now()`` arithmetic keeps the offsets clock-skew-free.
    session_factory = async_sessionmaker(pg_engine, expire_on_commit=False)
    async with session_factory() as session, session.begin():
        for label, queue, offset, acq_age in specs:
            payload, headers = encode_payload({"label": label})
            values: dict[str, object] = {
                "queue": queue,
                "payload": payload,
                "headers": headers,
                "next_attempt_at": text("now() + make_interval(secs => :next_s)").bindparams(next_s=offset),
            }
            if acq_age is not None:
                values["acquired_token"] = uuid.uuid4()
                values["acquired_at"] = text("now() - make_interval(secs => :acq_s)").bindparams(acq_s=acq_age)
            await session.execute(insert(outbox_table).values(**values))
    real_client = OutboxClient(pg_engine, outbox_table)
    async with pg_engine.connect() as conn:
        real_rows = await real_client.fetch(conn, queues_to_fetch, limit=100, lease_ttl_seconds=lease_ttl)
    real_labels = {json.loads(r.payload)["label"] for r in real_rows}

    # Fake side — separate ID space; correlate by payload label. Offsets (>=1s)
    # dwarf any plausible Python/DB clock skew, so the comparison is stable.
    now = _dt.datetime.now(_dt.UTC)
    fake = FakeOutboxClient()
    for label, queue, offset, acq_age in specs:
        payload, headers = encode_payload({"label": label})
        fake.feed(
            queue=queue,
            payload=payload,
            headers=headers,
            next_attempt_at=now + _dt.timedelta(seconds=offset),
        )
        if acq_age is not None:
            fake.rows[-1].acquired_token = uuid.uuid4()
            fake.rows[-1].acquired_at = now - _dt.timedelta(seconds=acq_age)
    fake_rows = await fake.fetch(None, queues_to_fetch, limit=100, lease_ttl_seconds=lease_ttl)
    fake_labels = {json.loads(r.payload)["label"] for r in fake_rows}

    assert real_labels == fake_labels == expected_eligible, (
        f"predicate drift — real={real_labels} fake={fake_labels} expected={expected_eligible}"
    )


async def test_concurrent_drain_with_eight_workers_holds_pool_bounded(pg_engine, outbox_table) -> None:
    """
    T2 — multi-worker drain: 500 rows + max_workers=8 keeps pool checkouts O(workers).

    The M3 baseline (test above) exercises max_workers=1 (2 steady-state pool
    connections: 1 fetch + 1 writer). This test raises the bar to max_workers=8
    (9 steady-state: 1 fetch + 8 writers) and asserts under sustained concurrent
    drain: no duplicate deliveries, no lost rows, all terminal writes land, and
    checkouts stay bounded by O(workers). Uses a locally-tuned engine
    (``pool_size=20``) so the test's connection budget is self-documenting and
    doesn't perturb the conftest fixture used by every other integration test.
    """
    # ``str(url)`` masks the password as ``***``; render with hide_password=False so
    # asyncpg authenticates against the real DSN.
    dsn = pg_engine.url.render_as_string(hide_password=False)
    local_engine = create_async_engine(dsn, future=True, pool_size=20, max_overflow=5)
    try:
        broker = OutboxBroker(local_engine, outbox_table=outbox_table)
        n_rows = 500
        received: list[int] = []

        @broker.subscriber(
            "orders",
            max_workers=8,
            fetch_batch_size=50,
            min_fetch_interval=0.02,
            max_fetch_interval=0.1,
        )
        async def handle(body: dict) -> None:
            received.append(body["i"])

        # Seed BEFORE attaching the checkout listener so seed round-trips don't pollute the count.
        session_factory = async_sessionmaker(local_engine, expire_on_commit=False)
        bodies = [{"i": i} for i in range(n_rows)]
        async with session_factory() as session, session.begin():
            await broker.publish_batch(*bodies, queue="orders", session=session)

        checkouts: list[None] = []

        def _on_checkout(*_args: object) -> None:
            checkouts.append(None)

        event.listen(local_engine.sync_engine, "checkout", _on_checkout)
        try:
            async with broker:
                await _wait_until(lambda: len(received) == n_rows, timeout=30.0)
                # Settle: allow terminal DELETEs from 8 workers to flush before shutdown.
                await asyncio.sleep(0.5)
        finally:
            event.remove(local_engine.sync_engine, "checkout", _on_checkout)

        assert sorted(received) == list(range(n_rows)), "lost or out-of-range rows"
        assert len(received) == len(set(received)), "duplicate deliveries"
        assert await _row_count(local_engine, outbox_table) == 0, "terminal writes did not land"
        # Steady-state: 9 SQLAlchemy connections (1 fetch + 8 writers). Allow ~16
        # of startup/health-probe churn. Pre-M3 with n=500 would be 500+ checkouts.
        assert len(checkouts) <= 25, (
            f"pool checkouts during {n_rows}-row drain at max_workers=8: {len(checkouts)}; "
            f"expected O(workers), not O(rows)"
        )
    finally:
        await local_engine.dispose()


# --- Publisher tests --------------------------------------------------------------------


async def test_publisher_publish_persists_row(pg_engine, outbox_table) -> None:
    """``publisher.publish`` commits with the caller's transaction, just like ``broker.publish``."""
    broker = OutboxBroker(pg_engine, outbox_table=outbox_table)
    pub = broker.publisher("orders", headers={"source": "pub-test"})
    session_factory = async_sessionmaker(pg_engine, expire_on_commit=False)

    async with session_factory() as session:
        async with session.begin():
            row_id = await pub.publish({"order_id": 99}, session=session)
            # Mid-transaction: separate connection must not see the row.
            count_during = await _row_count(pg_engine, outbox_table)
            assert count_during == 0
        count_after = await _row_count(pg_engine, outbox_table)
        assert count_after == 1
        assert isinstance(row_id, int)

    # Verify static headers + queue landed on the row.
    async with pg_engine.connect() as conn:
        result = await conn.execute(select(outbox_table))
        rows = result.mappings().all()
    assert rows[0]["queue"] == "orders"
    assert rows[0]["headers"]["source"] == "pub-test"


async def test_publisher_publish_with_subscriber_end_to_end(pg_engine, outbox_table) -> None:
    received: list[dict] = []
    broker = OutboxBroker(pg_engine, outbox_table=outbox_table)

    @broker.subscriber("orders", min_fetch_interval=0.05, max_fetch_interval=0.5)
    async def handle(body: dict) -> None:
        received.append(body)

    pub = broker.publisher("orders")
    session_factory = async_sessionmaker(pg_engine, expire_on_commit=False)
    async with broker:
        async with session_factory() as session, session.begin():
            await pub.publish({"order_id": 7}, session=session)
        await _wait_until(lambda: received, timeout=5.0)

    assert received == [{"order_id": 7}]


async def test_publisher_with_activate_in_delays_delivery(pg_engine, outbox_table) -> None:
    """``publisher.publish(activate_in=...)`` schedules the row for the future."""
    received: list[dict] = []
    broker = OutboxBroker(pg_engine, outbox_table=outbox_table)

    @broker.subscriber("orders", min_fetch_interval=0.05, max_fetch_interval=0.5)
    async def handle(body: dict) -> None:
        received.append(body)

    pub = broker.publisher("orders")
    session_factory = async_sessionmaker(pg_engine, expire_on_commit=False)
    delay_seconds = 0.7
    start = asyncio.get_event_loop().time()
    async with broker:
        async with session_factory() as session, session.begin():
            await pub.publish({"x": 1}, session=session, activate_in=_dt.timedelta(seconds=delay_seconds))
        await _wait_until(lambda: received, timeout=5.0)

    elapsed = asyncio.get_event_loop().time() - start
    assert received == [{"x": 1}]
    assert elapsed >= delay_seconds, f"delivery fired in {elapsed:.2f}s, expected >= {delay_seconds}s"


async def test_end_to_end_metrics_recorder_fires_for_dispatch_and_publish(pg_engine, outbox_table) -> None:
    """Recorder receives ``published``, ``dispatched``, ``acked``, and ``fetched`` events end-to-end."""
    events: list[tuple[str, dict]] = []

    def recorder(event: str, tags) -> None:
        events.append((event, dict(tags)))

    broker = OutboxBroker(pg_engine, outbox_table=outbox_table, metrics_recorder=recorder)

    @broker.subscriber("orders", min_fetch_interval=0.05, max_fetch_interval=0.5)
    async def handle(body: dict) -> None:
        del body

    session_factory = async_sessionmaker(pg_engine, expire_on_commit=False)
    async with broker:
        async with session_factory() as session, session.begin():
            for i in range(5):
                await broker.publish({"i": i}, queue="orders", session=session)
        await _wait_until(lambda: sum(1 for e, _ in events if e == "acked") >= 5, timeout=10.0)

    names = [e for e, _ in events]
    assert names.count("published") >= 5
    assert names.count("dispatched") >= 5
    assert names.count("acked") >= 5
    assert "fetched" in names  # at least one fetch tick fired


async def test_end_to_end_metrics_recorder_retry_then_terminal(pg_engine, outbox_table) -> None:
    """Handler raises until max_attempts; recorder sees nacked_retried then nacked_terminal."""
    events: list[tuple[str, dict]] = []

    def recorder(event: str, tags) -> None:
        events.append((event, dict(tags)))

    broker = OutboxBroker(pg_engine, outbox_table=outbox_table, metrics_recorder=recorder)

    @broker.subscriber(
        "orders",
        min_fetch_interval=0.05,
        max_fetch_interval=0.2,
        retry_strategy=ConstantRetry(delay_seconds=0.05, max_attempts=2),
    )
    async def handle(body: dict) -> None:
        del body
        msg = "always fails"
        raise RuntimeError(msg)

    session_factory = async_sessionmaker(pg_engine, expire_on_commit=False)
    async with broker:
        async with session_factory() as session, session.begin():
            await broker.publish({"x": 1}, queue="orders", session=session)
        await _wait_until(lambda: any(e == "nacked_terminal" for e, _ in events), timeout=10.0)

    retried = [t for e, t in events if e == "nacked_retried"]
    terminals = [t for e, t in events if e == "nacked_terminal"]
    assert len(retried) >= 1
    assert any(t["reason"] == "retry_terminal" for t in terminals)
    assert all(t["exception_type"] == "RuntimeError" for t in retried)


async def _wait_until_claimed(
    pg_engine: AsyncEngine,
    outbox_table: Table,
    *,
    timeout: float,  # noqa: ASYNC109
) -> None:
    deadline = asyncio.get_event_loop().time() + timeout
    stmt = select(outbox_table.c.id).where(outbox_table.c.acquired_token.is_(None))
    while asyncio.get_event_loop().time() < deadline:
        async with pg_engine.connect() as conn:
            unclaimed = (await conn.execute(stmt)).fetchall()
        if not unclaimed:
            return
        await asyncio.sleep(0.02)
    msg = "fetch never claimed every row"  # pragma: no cover
    raise AssertionError(msg)  # pragma: no cover


async def test_drain_finishes_inflight_rows_before_returning(
    pg_engine: AsyncEngine,
    outbox_table: Table,
) -> None:
    """Rows claimed by fetch must run to completion when broker.stop() is called."""
    broker = OutboxBroker(pg_engine, outbox_table=outbox_table, graceful_timeout=5.0)
    handled: list[int] = []

    @broker.subscriber(
        "orders",
        min_fetch_interval=0.02,
        max_fetch_interval=0.05,
        max_workers=4,
        fetch_batch_size=20,
    )
    async def handle(body: dict) -> None:
        await asyncio.sleep(0.1)
        handled.append(body["i"])

    session_factory = async_sessionmaker(pg_engine, expire_on_commit=False)
    async with broker:
        async with session_factory() as session, session.begin():
            for i in range(20):
                await broker.publish({"i": i}, queue="orders", session=session)
        await _wait_until_claimed(pg_engine, outbox_table, timeout=3.0)
        await broker.stop()

    assert sorted(handled) == list(range(20))
    assert await _row_count(pg_engine, outbox_table) == 0


async def test_drain_returns_within_graceful_timeout_when_handler_wedges(
    pg_engine: AsyncEngine,
    outbox_table: Table,
) -> None:
    """Wedged handlers must be cancelled within graceful_timeout (no 2x wait)."""
    broker = OutboxBroker(pg_engine, outbox_table=outbox_table, graceful_timeout=0.3)
    started = asyncio.Event()

    @broker.subscriber("orders", min_fetch_interval=0.02, max_fetch_interval=0.05)
    async def handle(body: dict) -> None:
        del body
        started.set()
        await asyncio.sleep(60.0)  # never returns voluntarily

    session_factory = async_sessionmaker(pg_engine, expire_on_commit=False)
    async with broker:
        async with session_factory() as session, session.begin():
            await broker.publish({"i": 0}, queue="orders", session=session)
        await asyncio.wait_for(started.wait(), timeout=3.0)
        start = asyncio.get_event_loop().time()
        await broker.stop()
        elapsed = asyncio.get_event_loop().time() - start

    # Strict bound: ~graceful_timeout for drain + cancellation propagation slack.
    # The 2x-regression failure mode (re-waiting MultiLock inside super().stop())
    # would push this past 0.6s; 0.7s is the safe upper guard.
    assert elapsed < 0.7, f"broker.stop() took {elapsed:.3f}s — strict-bound regression"
    # Row preserved with lease set: another replica reclaims after lease_ttl.
    assert await _row_count(pg_engine, outbox_table) == 1


async def test_broker_stop_runs_subscribers_concurrently(
    pg_engine: AsyncEngine,
    outbox_table: Table,
) -> None:
    """``OutboxBroker.stop``'s gather collapses N x graceful_timeout to ~max(per-sub)."""
    broker = OutboxBroker(pg_engine, outbox_table=outbox_table, graceful_timeout=2.0)
    started = [asyncio.Event() for _ in range(3)]
    finished: list[str] = []

    def register(queue: str, idx: int) -> None:
        @broker.subscriber(queue, min_fetch_interval=0.02, max_fetch_interval=0.05)
        async def handle(body: dict) -> None:
            del body
            started[idx].set()
            await asyncio.sleep(0.3)
            finished.append(queue)

    for idx, queue in enumerate(("q-a", "q-b", "q-c")):
        register(queue, idx)

    session_factory = async_sessionmaker(pg_engine, expire_on_commit=False)
    async with broker:
        async with session_factory() as session, session.begin():
            for queue in ("q-a", "q-b", "q-c"):
                await broker.publish({"q": queue}, queue=queue, session=session)
        await asyncio.gather(*(asyncio.wait_for(e.wait(), timeout=3.0) for e in started))
        start = asyncio.get_event_loop().time()
        await broker.stop()
        elapsed = asyncio.get_event_loop().time() - start

    assert sorted(finished) == ["q-a", "q-b", "q-c"]
    # Three 0.3s handlers in parallel ~ 0.3s + slack. Sequential would be >= 0.9s.
    # 0.7s upper guard catches a regression to sequential broker.stop.
    assert elapsed < 0.7, f"broker.stop() took {elapsed:.3f}s — looks sequential"
