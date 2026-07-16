"""Integration tests against real Postgres. Requires docker-compose postgres up."""

import asyncio
import contextlib
import datetime as _dt
import logging
import uuid
from collections.abc import Mapping
from typing import Any
from unittest import mock

import pytest
from faststream.kafka import KafkaBroker, TestKafkaBroker
from sqlalchemy import MetaData, Table, event, insert, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from faststream_outbox import (
    ConstantRetry,
    NoRetry,
    OutboxBroker,
    OutboxResponse,
    make_dlq_table,
    make_outbox_table,
    outbox_autovacuum_ddl,
)
from faststream_outbox.client import OutboxClient
from faststream_outbox.envelope import _encode_payload as encode_payload
from faststream_outbox.publisher.fake import OutboxFakePublisher


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


async def test_validate_schema_detects_wrong_partial_index_predicate(
    pg_engine: AsyncEngine,
    outbox_table: Table,
) -> None:
    """S2: a partial index built with the wrong WHERE predicate is caught (alembic's diff ignores it)."""
    idx = f"{outbox_table.name}_timer_id_uq"
    async with pg_engine.begin() as conn:
        await conn.execute(text(f'DROP INDEX "{idx}"'))
        # Recreate with the wrong predicate: should be ``timer_id IS NOT NULL``.
        await conn.execute(
            text(f'CREATE UNIQUE INDEX "{idx}" ON {outbox_table.name} (queue, timer_id) WHERE timer_id IS NULL'),
        )
    client = OutboxClient(pg_engine, outbox_table)
    with pytest.raises(RuntimeError, match="wrong partial predicate"):
        await client.validate_schema()


async def test_validate_schema_detects_non_partial_index_on_present_index(
    pg_engine: AsyncEngine,
    outbox_table: Table,
) -> None:
    """S2 (review #1): an expected partial index recreated NON-partial breaks ON CONFLICT too, and is caught.

    Alembic's diff can't distinguish a plain UNIQUE (queue, timer_id) from the partial
    form (it ignores postgresql_where), and the indpred-NULL row would otherwise be
    silently skipped by the probe.
    """
    idx = f"{outbox_table.name}_timer_id_uq"
    async with pg_engine.begin() as conn:
        await conn.execute(text(f'DROP INDEX "{idx}"'))
        await conn.execute(text(f'CREATE UNIQUE INDEX "{idx}" ON {outbox_table.name} (queue, timer_id)'))
    client = OutboxClient(pg_engine, outbox_table)
    with pytest.raises(RuntimeError, match="not a partial index"):
        await client.validate_schema()


async def test_validate_schema_detects_non_unique_timer_id_index(
    pg_engine: AsyncEngine,
    outbox_table: Table,
) -> None:
    """F2-10: a same-named timer_id index recreated NON-unique (right predicate) breaks ON CONFLICT — caught."""
    idx = f"{outbox_table.name}_timer_id_uq"
    async with pg_engine.begin() as conn:
        await conn.execute(text(f'DROP INDEX "{idx}"'))
        # Correct partial predicate but NOT unique → the producer's ON CONFLICT (queue, timer_id)
        # arbiter has no unique index to bind to and raises at publish time.
        await conn.execute(
            text(f'CREATE INDEX "{idx}" ON {outbox_table.name} (queue, timer_id) WHERE timer_id IS NOT NULL'),
        )
    client = OutboxClient(pg_engine, outbox_table)
    with pytest.raises(RuntimeError, match="not UNIQUE"):
        await client.validate_schema()


async def test_validate_schema_passes_for_correct_table(pg_engine, outbox_table) -> None:
    client = OutboxClient(pg_engine, outbox_table)
    await client.validate_schema()  # should not raise


async def test_validate_schema_passes_for_table_in_named_schema(pg_engine: AsyncEngine) -> None:
    """A correct outbox table in a non-default ``MetaData(schema=...)`` must validate.

    ``_run_validate`` configures Alembic with ``include_schemas=True`` so its reflection
    reaches beyond the default schema; without it, a named-schema table is invisible to
    ``compare_metadata`` and falsely reads as ``table 'outbox' does not exist``.
    """
    schema = f"sch_vs_{uuid.uuid4().hex[:8]}"
    metadata = MetaData(schema=schema)
    table = make_outbox_table(metadata, table_name="outbox")
    async with pg_engine.begin() as conn:
        await conn.execute(text(f'CREATE SCHEMA "{schema}"'))
        await conn.run_sync(metadata.create_all)
    try:
        client = OutboxClient(pg_engine, table)
        await client.validate_schema()  # must NOT raise
    finally:
        async with pg_engine.begin() as conn:
            await conn.run_sync(metadata.drop_all)
            await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))


async def test_validate_schema_passes_for_explicitly_named_default_schema(pg_engine: AsyncEngine) -> None:
    """A table whose ``MetaData(schema=...)`` explicitly names the connection's DEFAULT schema validates.

    With ``include_schemas=True`` Alembic reports the default schema to ``include_name`` as
    ``None``, so a naive ``name == table.schema`` excludes a table declared with the literal
    default-schema name (e.g. ``MetaData(schema="public")``) — the table never reflects and a
    CORRECT table falsely raises "table 'outbox' does not exist". ``_run_validate`` normalizes
    the explicitly-named default schema to ``None`` before comparing.
    """
    metadata = MetaData(schema="public")
    table = make_outbox_table(metadata, table_name=f"outbox_pub_{uuid.uuid4().hex[:8]}")
    async with pg_engine.begin() as conn:
        await conn.run_sync(metadata.create_all)
    try:
        client = OutboxClient(pg_engine, table)
        await client.validate_schema()  # must NOT raise
    finally:
        async with pg_engine.begin() as conn:
            await conn.run_sync(metadata.drop_all)


async def test_validate_schema_fails_for_missing_table(pg_engine) -> None:
    metadata = MetaData()
    table = make_outbox_table(metadata, table_name="does_not_exist_xyz")
    client = OutboxClient(pg_engine, table)
    with pytest.raises(RuntimeError, match="does not exist"):
        await client.validate_schema()


async def test_ping_succeeds(pg_engine, outbox_table) -> None:
    client = OutboxClient(pg_engine, outbox_table)
    assert await client.ping() is True


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


async def test_fetch_skips_rows_locked_by_another_transaction(pg_engine, outbox_table) -> None:
    """F7-04: fetch uses FOR UPDATE SKIP LOCKED — it skips rows another transaction holds, not blocks.

    Seeds 30 rows, locks 20 in an open transaction, then asserts a concurrent fetch promptly
    claims the disjoint 10. A regression to plain ``FOR UPDATE`` would block on the locked rows
    until the holder commits, so the bounded ``wait_for`` fails via timeout instead of hanging.
    (The sibling test above guards no-double-claim but would pass without SKIP LOCKED.)
    """
    async with pg_engine.begin() as conn:
        for i in range(30):
            await conn.execute(insert(outbox_table).values(queue="orders", payload=f"p-{i}".encode()))
    client = OutboxClient(pg_engine, outbox_table)
    t = outbox_table

    async with pg_engine.connect() as holder, holder.begin():
        locked = (
            (
                await holder.execute(
                    select(t.c.id)
                    .where(t.c.queue == "orders")
                    .order_by(t.c.id)
                    .limit(20)
                    .with_for_update(skip_locked=True),
                )
            )
            .scalars()
            .all()
        )
        assert len(locked) == 20

        async with pg_engine.connect() as conn_b:
            claimed = await asyncio.wait_for(
                client.fetch(conn_b, ["orders"], limit=20, lease_ttl_seconds=60.0),
                timeout=5.0,
            )
        claimed_ids = {r.id for r in claimed}
        assert claimed_ids.isdisjoint(set(locked))  # skipped the holder's rows
        assert len(claimed_ids) == 10  # exactly the rows the holder didn't lock


async def test_writer_connection_autocommit_round_trip(pg_engine: AsyncEngine, outbox_table: Table) -> None:
    """Autocommit-configured writer conn runs ``delete_with_lease`` end-to-end against real Postgres.

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
    with pytest.raises(RuntimeError, match="missing column 'headers'") as excinfo:
        await client.validate_schema()
    assert "fixing-drift-autogenerate-cant-see" not in str(excinfo.value)


async def test_validate_schema_fails_when_timer_id_unique_index_missing(pg_engine, outbox_table) -> None:
    """Missing partial unique index on (queue, timer_id) must be caught before runtime.

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
    """Extra columns / indexes the user adds to their outbox table must NOT fail validation.

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


async def test_outbox_response_followon_row_commits_with_handler_transaction(pg_engine, outbox_table) -> None:
    """F7-02: a returned OutboxResponse's follow-on row commits with the handler's transaction.

    The worker loop publishes a returned OutboxResponse through ``OutboxFakePublisher``
    (``result_msg.as_publish_command()`` → ``producer.publish`` on the response's own
    session). This drives that exact path on real Postgres and pins that the resulting
    INSERT participates in the caller's open transaction: invisible to a separate
    connection until commit, then committed atomically with the handler's writes —
    mirroring ``test_publish_inserts_in_caller_transaction`` for the direct path. A
    regression that published the follow-on row on a fresh connection would make the
    mid-transaction count non-zero.
    """
    broker = OutboxBroker(pg_engine, outbox_table=outbox_table)
    response_publisher = OutboxFakePublisher(broker.config.producer)
    session_factory = async_sessionmaker(pg_engine, expire_on_commit=False)

    async with session_factory() as session:
        async with session.begin():
            response = OutboxResponse(body={"chained": True}, queue="downstream", session=session)
            await response_publisher._publish(response.as_publish_command(), _extra_middlewares=[])  # noqa: SLF001
            # Mid-transaction: a separate connection must not see the follow-on row.
            assert await _row_count(pg_engine, outbox_table) == 0
        # After the handler's transaction commits, exactly the chained row is visible.
        assert await _row_count(pg_engine, outbox_table) == 1

    async with session_factory() as session:
        rows = await broker.fetch_unprocessed(session=session, queue="downstream")
    assert [r.queue for r in rows] == ["downstream"]


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
    """M3 — per-worker cached writer conn drains N rows without N pool checkouts.

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


async def test_concurrent_drain_with_eight_workers_holds_pool_bounded(pg_engine, outbox_table) -> None:
    """T2 — multi-worker drain: 500 rows + max_workers=8 keeps pool checkouts O(workers).

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


async def test_batched_flush_deletes_all_rows(pg_engine: AsyncEngine, outbox_table: Table) -> None:
    """terminal_flush_batch_size>1 coalesces terminal deletes; every row still lands.

    A no-op handler acks each row; the worker buffers the batchable terminal deletes and
    flushes them in one ``DELETE ... RETURNING`` (on queue-idle, since the buffer never
    reaches 100). After drain the table must be empty and exactly N ``acked`` metrics fire
    (one per deleted row, emitted from the batch flush).
    """
    n_rows = 50
    received: list[int] = []
    events: list[tuple[str, dict[str, Any]]] = []

    def recorder(event: str, tags: Mapping[str, Any]) -> None:
        events.append((event, dict(tags)))

    broker = OutboxBroker(pg_engine, outbox_table=outbox_table, metrics_recorder=recorder)

    @broker.subscriber(
        "orders",
        min_fetch_interval=0.02,
        max_fetch_interval=0.1,
        fetch_batch_size=50,
        terminal_flush_batch_size=100,
    )
    async def handle(body: dict) -> None:
        received.append(body["i"])

    session_factory = async_sessionmaker(pg_engine, expire_on_commit=False)
    async with broker:
        async with session_factory() as session, session.begin():
            for i in range(n_rows):
                payload, headers = encode_payload({"i": i})
                await session.execute(insert(outbox_table).values(queue="orders", payload=payload, headers=headers))
        await _wait_until(lambda: len(received) == n_rows, timeout=15.0)

        # The batched DELETE lands on queue-idle after the last dispatch; wait for the acked
        # metrics (emitted only once the flush's DELETE lands) while the broker is still
        # running so the flush isn't racing shutdown. Waiting on the event count instead of
        # polling row_count keeps coverage deterministic -- an inline row-count poll skips its
        # sleep whenever the table is already empty on the first check.
        await _wait_until(lambda: len([t for e, t in events if e == "acked"]) == n_rows)

    assert sorted(received) == list(range(n_rows))
    assert await _row_count(pg_engine, outbox_table) == 0
    acked = [t for e, t in events if e == "acked"]
    assert len(acked) == n_rows, f"one acked metric per deleted row expected, got {len(acked)}"


async def test_batched_flush_lease_lost_row_redelivers(pg_engine: AsyncEngine, outbox_table: Table) -> None:
    """A buffered row whose lease is stolen before its batch flush emits lease_lost, not acked.

    While its handler runs, one row's ``acquired_token`` is overwritten from a separate
    connection (a fresh, still-valid lease). When the batched ``DELETE`` fires, that row's
    ``(id, token)`` pair no longer matches, so it is absent from the ``RETURNING`` set: the
    worker emits ``lease_lost(phase=terminal)`` for exactly that row and leaves it in place;
    every other buffered row is deleted. This proves the lease-token invariant holds through
    the batched DELETE's ``RETURNING`` set.
    """
    n_rows = 5
    received: list[Any] = []
    events: list[tuple[str, dict[str, Any]]] = []

    def recorder(event: str, tags: Mapping[str, Any]) -> None:
        events.append((event, dict(tags)))

    # Capture the marker row's id up front so we can both steal its lease and assert on it.
    marker_payload, marker_headers = encode_payload({"marker": True})
    async with pg_engine.begin() as conn:
        marker_id = (
            await conn.execute(
                insert(outbox_table)
                .values(queue="orders", payload=marker_payload, headers=marker_headers)
                .returning(outbox_table.c.id),
            )
        ).scalar_one()
        for i in range(n_rows - 1):
            payload, headers = encode_payload({"i": i})
            await conn.execute(insert(outbox_table).values(queue="orders", payload=payload, headers=headers))

    broker = OutboxBroker(pg_engine, outbox_table=outbox_table, metrics_recorder=recorder)

    # The marker handler blocks (row leased, mid-flight) until the main task steals its lease,
    # then returns so its row is buffered with the now-stale fetch-time token. Stealing from the
    # main task -- not inside the worker-task handler -- both makes the ordering deterministic and
    # keeps the steal's ``async with`` traceable (an await in a worker task cancelled at stop()
    # phantom-drops its coverage).
    marker_leased = asyncio.Event()
    lease_stolen = asyncio.Event()

    @broker.subscriber(
        "orders",
        max_workers=1,
        min_fetch_interval=0.02,
        max_fetch_interval=0.1,
        fetch_batch_size=10,
        terminal_flush_batch_size=100,
        lease_ttl_seconds=30.0,  # long: the stolen (still-valid) lease must not be re-fetched
    )
    async def handle(body: dict) -> None:
        received.append(body)
        if body.get("marker"):
            marker_leased.set()
            await lease_stolen.wait()

    async with broker:
        await marker_leased.wait()  # marker handler is in-flight; its row holds the fetch-time lease
        steal = outbox_table.update().where(outbox_table.c.id == marker_id).values(acquired_token=uuid.uuid4())
        # pragma: no cover -- this steal executes (the assertions below prove it: the marker
        # survives, the other four are deleted), but coverage cannot trace a SQLAlchemy-async
        # await that runs concurrently with the live broker's own greenlet DB work; the source
        # invariant it exercises is covered deterministically by the _flush_buffer unit test.
        async with pg_engine.begin() as steal_conn:  # pragma: no cover
            await steal_conn.execute(steal)
        lease_stolen.set()  # release the marker handler; its row buffers with the stale token
        await _wait_until(lambda: len(received) == n_rows, timeout=15.0)
        await _wait_until(
            lambda: any(e == "lease_lost" and t.get("phase") == "terminal" for e, t in events),
            timeout=15.0,
        )

    lease_lost = [t for e, t in events if e == "lease_lost" and t.get("phase") == "terminal"]
    assert len(lease_lost) == 1, f"exactly one row's batched DELETE must miss, got {len(lease_lost)}"
    assert lease_lost[0]["row_id"] == marker_id, "the lease_lost row must be the one whose token was stolen"
    acked = [t for e, t in events if e == "acked"]
    assert len(acked) == n_rows - 1, f"every other buffered row must be deleted, got {len(acked)} acked"
    # The marker survives (its stolen lease still holds it); the other four are gone.
    async with pg_engine.connect() as conn:
        remaining = (await conn.execute(select(outbox_table.c.id))).scalars().all()
    assert remaining == [marker_id], f"only the lease-lost marker row must survive, got {remaining}"


async def test_batch_size_one_matches_per_row(pg_engine: AsyncEngine, outbox_table: Table) -> None:
    """terminal_flush_batch_size=1 (the default) keeps every row on the inline per-row path.

    N seeded rows drain to empty and emit exactly N ``acked`` metrics -- identical to the
    pre-batching behavior. This guards the default-identity axis: batching must not leak
    into the ``==1`` path.
    """
    n_rows = 20
    received: list[int] = []
    events: list[tuple[str, dict[str, Any]]] = []

    def recorder(event: str, tags: Mapping[str, Any]) -> None:
        events.append((event, dict(tags)))

    broker = OutboxBroker(pg_engine, outbox_table=outbox_table, metrics_recorder=recorder)

    @broker.subscriber(
        "orders",
        min_fetch_interval=0.02,
        max_fetch_interval=0.1,
        fetch_batch_size=20,
        terminal_flush_batch_size=1,
    )
    async def handle(body: dict) -> None:
        received.append(body["i"])

    session_factory = async_sessionmaker(pg_engine, expire_on_commit=False)
    async with broker:
        async with session_factory() as session, session.begin():
            for i in range(n_rows):
                payload, headers = encode_payload({"i": i})
                await session.execute(insert(outbox_table).values(queue="orders", payload=payload, headers=headers))
        await _wait_until(lambda: len(received) == n_rows, timeout=15.0)
        # Wait on the acked count (emitted when each per-row DELETE lands), not an inline
        # row-count poll -- see test_batched_flush_deletes_all_rows for why.
        await _wait_until(lambda: len([t for e, t in events if e == "acked"]) == n_rows)

    assert sorted(received) == list(range(n_rows))
    assert await _row_count(pg_engine, outbox_table) == 0
    acked = [t for e, t in events if e == "acked"]
    assert len(acked) == n_rows, f"batch_size=1 must emit one acked per row, got {len(acked)}"


async def test_batched_flush_size_trigger_deletes_all(pg_engine: AsyncEngine, outbox_table: Table) -> None:
    """``len(buffer) >= batch_size`` (the primary under-load path) actually fires and drains.

    The other batched tests use ``terminal_flush_batch_size=100`` with <=50 rows, so the
    buffer never reaches its cap and every flush goes through the idle/``QueueEmpty`` path.
    Here ``fetch_batch_size >= n_rows`` keeps the inflight queue populated so the worker's
    buffer fills to ``batch_size`` while more rows remain queued -- the size-trigger branch,
    not idle. A spy on ``delete_batch_with_lease`` confirms at least one flush carried
    exactly ``batch_size`` pairs, then every row must still land.
    """
    n_rows = 30
    batch_size = 3
    received: list[int] = []
    events: list[tuple[str, dict[str, Any]]] = []

    def recorder(event: str, tags: Mapping[str, Any]) -> None:
        events.append((event, dict(tags)))

    broker = OutboxBroker(pg_engine, outbox_table=outbox_table, metrics_recorder=recorder)

    @broker.subscriber(
        "orders",
        max_workers=1,
        min_fetch_interval=0.02,
        max_fetch_interval=0.1,
        fetch_batch_size=n_rows,
        terminal_flush_batch_size=batch_size,
    )
    async def handle(body: dict) -> None:
        received.append(body["i"])

    client = broker.client
    original_batch_delete = client.delete_batch_with_lease
    batch_sizes: list[int] = []

    async def spy_delete_batch(*args: Any, **kwargs: Any) -> Any:
        pairs = args[1] if len(args) > 1 else kwargs["pairs"]
        batch_sizes.append(len(pairs))
        return await original_batch_delete(*args, **kwargs)

    session_factory = async_sessionmaker(pg_engine, expire_on_commit=False)
    with mock.patch.object(client, "delete_batch_with_lease", side_effect=spy_delete_batch):
        async with broker:
            async with session_factory() as session, session.begin():
                for i in range(n_rows):
                    payload, headers = encode_payload({"i": i})
                    await session.execute(
                        insert(outbox_table).values(queue="orders", payload=payload, headers=headers),
                    )
            await _wait_until(lambda: len(received) == n_rows, timeout=15.0)
            # Wait on the acked count (emitted when the batched DELETE lands), not an inline
            # row-count poll -- see test_batched_flush_deletes_all_rows for why.
            await _wait_until(lambda: len([t for e, t in events if e == "acked"]) == n_rows)

    assert sorted(received) == list(range(n_rows))
    assert await _row_count(pg_engine, outbox_table) == 0
    acked = [t for e, t in events if e == "acked"]
    assert len(acked) == n_rows, f"one acked metric per deleted row expected, got {len(acked)}"
    assert batch_size in batch_sizes, (
        f"expected at least one flush with exactly batch_size={batch_size} pairs (the size "
        f"trigger, not idle), got flush sizes {batch_sizes}"
    )


async def test_batched_flush_dlq_row_stays_inline(
    pg_engine: AsyncEngine,
    outbox_table: Table,
    dlq_table: Table,
) -> None:
    """A DLQ-writing terminal row stays on the inline CTE path even with batching enabled.

    ``_terminal_has_dlq`` excludes a failure-terminal row from the batchable buffer whenever
    ``dlq_table`` is configured -- its terminal write is a DELETE+INSERT CTE, which a plain
    batched DELETE cannot express. This seeds one row that fails terminally alongside several
    rows that succeed, with ``terminal_flush_batch_size>1``: the failing row's DLQ audit must
    land (not be silently dropped by a batched delete), and the successful rows must still be
    batch-deleted normally -- proving the mixed inline-DLQ + batched-success paths coexist.
    """
    n_success = 4
    received: list[dict[str, Any]] = []
    events: list[tuple[str, dict[str, Any]]] = []

    def recorder(event: str, tags: Mapping[str, Any]) -> None:
        events.append((event, dict(tags)))

    broker = OutboxBroker(pg_engine, outbox_table=outbox_table, dlq_table=dlq_table, metrics_recorder=recorder)

    @broker.subscriber(
        "orders",
        retry_strategy=NoRetry(),
        max_workers=1,
        min_fetch_interval=0.02,
        max_fetch_interval=0.1,
        fetch_batch_size=10,
        terminal_flush_batch_size=5,
    )
    async def handle(body: dict) -> None:
        received.append(body)
        if body.get("fail"):
            msg = "always fails"
            raise RuntimeError(msg)

    session_factory = async_sessionmaker(pg_engine, expire_on_commit=False)
    async with broker:
        async with session_factory() as session, session.begin():
            await broker.publish({"fail": True}, queue="orders", session=session, correlation_id="trace-dlq")
            for i in range(n_success):
                await broker.publish({"i": i}, queue="orders", session=session)
        await _wait_until(lambda: len(received) == n_success + 1, timeout=15.0)
        await _wait_until(lambda: any(e == "dlq_written" for e, _ in events), timeout=15.0)
        # Wait on the success rows' acked count (the inline DLQ row is handled above), not an
        # inline row-count poll -- see test_batched_flush_deletes_all_rows for why.
        await _wait_until(lambda: len([t for e, t in events if e == "acked"]) == n_success)

    assert await _row_count(pg_engine, outbox_table) == 0
    rows = await _dlq_rows(pg_engine, dlq_table)
    assert len(rows) == 1, "exactly the failing row must land in the DLQ"
    assert rows[0]["failure_reason"] == "retry_terminal"
    acked = [t for e, t in events if e == "acked"]
    assert len(acked) == n_success, "the success rows must still be batch-deleted alongside the inline DLQ row"


async def test_graceful_stop_leaves_no_undeleted_rows_via_join_barrier(
    pg_engine: AsyncEngine,
    outbox_table: Table,
) -> None:
    """A graceful stop() leaves zero undeleted rows -- no redelivery, no leak.

    This proves the *graceful* guarantee, and names the mechanism honestly: the buffer is
    cleared by the queue-idle flush inside ``_worker_inner`` and the ``_inflight.join()``
    drain barrier in ``stop()`` (every buffered row's deferred ``task_done`` fires only after
    its ``DELETE`` lands, so ``join()`` returns only once the buffer is empty). It does NOT
    exercise ``_worker_inner``'s exit-flush ``finally`` block -- that path is reachable only
    on a drain *timeout*, and here the idle flush empties the buffer before ``stop()`` even
    calls ``join()``. Seeds FEWER rows than ``terminal_flush_batch_size`` (50 rows, batch
    1000) so the size cap never fires: only the idle/join barrier can clear the buffer. No
    polling-for-empty happens before ``stop()`` -- the assertion is that the graceful stop
    itself left zero rows. A survivor means the join barrier leaked a completed-but-undeleted
    row and Task 3's batching is broken.
    """
    n_rows = 50
    received: list[int] = []

    broker = OutboxBroker(pg_engine, outbox_table=outbox_table)

    @broker.subscriber(
        "orders",
        max_workers=1,
        min_fetch_interval=0.02,
        max_fetch_interval=0.1,
        fetch_batch_size=50,
        terminal_flush_batch_size=1000,
    )
    async def handle(body: dict) -> None:
        received.append(body["i"])

    session_factory = async_sessionmaker(pg_engine, expire_on_commit=False)
    await broker.start()
    try:
        async with session_factory() as session, session.begin():
            for i in range(n_rows):
                payload, headers = encode_payload({"i": i})
                await session.execute(insert(outbox_table).values(queue="orders", payload=payload, headers=headers))
        await _wait_until(lambda: len(received) == n_rows, timeout=15.0)
    finally:
        # Graceful stop must flush the partial buffer as its final drain step.
        await broker.stop()

    assert sorted(received) == list(range(n_rows))
    assert await _row_count(pg_engine, outbox_table) == 0, (
        "graceful stop() must flush the partial terminal buffer; surviving rows mean the drain leaked"
    )


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


async def test_drain_finishes_inflight_rows_before_returning(
    pg_engine: AsyncEngine,
    outbox_table: Table,
) -> None:
    """Rows claimed by fetch must run to completion when broker.stop() is called."""
    fetched_total = 0

    def recorder(event: str, fields: Mapping[str, Any]) -> None:
        nonlocal fetched_total
        if event == "fetched":
            fetched_total += fields["count"]

    broker = OutboxBroker(
        pg_engine,
        outbox_table=outbox_table,
        graceful_timeout=5.0,
        metrics_recorder=recorder,
    )
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
        await _wait_until(lambda: fetched_total >= 20, timeout=3.0)
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


# --- DLQ (issue #26) -----------------------------------------------------------


async def _dlq_rows(engine: AsyncEngine, table: Table) -> list[dict]:
    async with engine.connect() as conn:
        result = await conn.execute(select(table).order_by(table.c.id))
        return [dict(row) for row in result.mappings().all()]


async def test_validate_schema_passes_for_correct_dlq_table(
    pg_engine: AsyncEngine,
    outbox_table: Table,
    dlq_table: Table,
) -> None:
    client = OutboxClient(pg_engine, outbox_table, dlq_table=dlq_table)
    await client.validate_schema()


async def test_validate_schema_fails_for_missing_dlq_table(
    pg_engine: AsyncEngine,
    outbox_table: Table,
) -> None:
    """When ``dlq_table`` is configured, validate_schema also checks the DLQ table."""
    metadata = MetaData()
    missing_dlq = make_dlq_table(metadata, table_name="does_not_exist_dlq_xyz")
    client = OutboxClient(pg_engine, outbox_table, dlq_table=missing_dlq)
    with pytest.raises(RuntimeError, match="does_not_exist_dlq_xyz"):
        await client.validate_schema()


async def test_dlq_atomic_insert_with_delete(
    pg_engine: AsyncEngine,
    outbox_table: Table,
    dlq_table: Table,
) -> None:
    """End-to-end: a terminal-failure handler triggers DELETE+INSERT in one CTE."""
    broker = OutboxBroker(pg_engine, outbox_table=outbox_table, dlq_table=dlq_table)
    handled = asyncio.Event()

    @broker.subscriber("orders", retry_strategy=NoRetry(), min_fetch_interval=0.02)
    async def handle(body: dict) -> None:
        del body
        handled.set()
        msg = "always fails"
        raise RuntimeError(msg)

    session_factory = async_sessionmaker(pg_engine, expire_on_commit=False)
    async with broker:
        async with session_factory() as session, session.begin():
            await broker.publish({"audit": True}, queue="orders", session=session, correlation_id="trace-dlq")
        await asyncio.wait_for(handled.wait(), timeout=5.0)
        # Exit ``async with broker`` triggers stop() → graceful drain, so the
        # worker's terminal flush (the CTE) has fully committed before we assert.

    assert await _row_count(pg_engine, outbox_table) == 0
    rows = await _dlq_rows(pg_engine, dlq_table)
    assert len(rows) == 1
    row = rows[0]
    assert row["queue"] == "orders"
    assert row["failure_reason"] == "retry_terminal"
    assert row["last_exception"] is not None
    assert "RuntimeError" in row["last_exception"]
    assert row["headers"] is not None
    assert row["headers"].get("correlation_id") == "trace-dlq"
    assert row["original_id"] is not None
    assert row["payload"] is not None


async def test_dlq_writes_to_schema_qualified_tables(pg_engine: AsyncEngine) -> None:
    """B10: with a non-default ``MetaData(schema=...)`` the DLQ CTE must target ``schema.table``.

    The buggy ``quote(table.name)`` dropped the schema, so the raw DELETE+INSERT CTE
    referenced a bare ``outbox`` / ``outbox_dlq`` not on the search_path → ``UndefinedTable``
    on every terminal failure and no audit row ever landed.
    """
    schema = f"sch_{uuid.uuid4().hex[:8]}"
    metadata = MetaData(schema=schema)
    outbox = make_outbox_table(metadata, table_name="outbox")
    dlq = make_dlq_table(metadata, table_name="outbox_dlq")
    async with pg_engine.begin() as conn:
        await conn.execute(text(f'CREATE SCHEMA "{schema}"'))
        await conn.run_sync(metadata.create_all)
    try:
        broker = OutboxBroker(pg_engine, outbox_table=outbox, dlq_table=dlq)
        handled = asyncio.Event()

        @broker.subscriber("orders", retry_strategy=NoRetry(), min_fetch_interval=0.02)
        async def handle(body: dict) -> None:
            del body
            handled.set()
            boom = "always fails"
            raise RuntimeError(boom)

        session_factory = async_sessionmaker(pg_engine, expire_on_commit=False)
        async with broker:
            async with session_factory() as session, session.begin():
                await broker.publish({"audit": True}, queue="orders", session=session)
            await asyncio.wait_for(handled.wait(), timeout=5.0)

        assert await _row_count(pg_engine, outbox) == 0
        rows = await _dlq_rows(pg_engine, dlq)
        assert len(rows) == 1
        assert rows[0]["failure_reason"] == "retry_terminal"
    finally:
        async with pg_engine.begin() as conn:
            await conn.run_sync(metadata.drop_all)
            await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))


async def test_lease_pairing_check_rejects_half_set_lease(pg_engine: AsyncEngine, outbox_table: Table) -> None:
    """P8: a half-set lease (acquired_at set, acquired_token NULL) is unrepresentable via the CHECK."""
    with pytest.raises(IntegrityError):
        async with pg_engine.begin() as conn:
            await conn.execute(
                insert(outbox_table).values(
                    queue="orders",
                    payload=b"x",
                    acquired_at=_dt.datetime.now(tz=_dt.UTC),  # acquired_token left NULL
                ),
            )


async def test_dlq_preserves_timer_id(
    pg_engine: AsyncEngine,
    outbox_table: Table,
    dlq_table: Table,
) -> None:
    """P9: a terminally-failed timer keeps its timer_id in the DLQ audit trail."""
    broker = OutboxBroker(pg_engine, outbox_table=outbox_table, dlq_table=dlq_table)
    handled = asyncio.Event()

    @broker.subscriber("orders", retry_strategy=NoRetry(), min_fetch_interval=0.02)
    async def handle(body: dict) -> None:
        del body
        handled.set()
        boom = "always fails"
        raise RuntimeError(boom)

    session_factory = async_sessionmaker(pg_engine, expire_on_commit=False)
    async with broker:
        async with session_factory() as session, session.begin():
            await broker.publish({"x": 1}, queue="orders", session=session, timer_id="email-42")
        await asyncio.wait_for(handled.wait(), timeout=5.0)

    rows = await _dlq_rows(pg_engine, dlq_table)
    assert len(rows) == 1
    assert rows[0]["timer_id"] == "email-42"


async def test_dlq_insert_failure_rolls_back_delete(
    pg_engine: AsyncEngine,
    outbox_table: Table,
) -> None:
    """A DLQ-write failure rolls back the whole CTE — the outbox row stays leased."""
    # Configure the broker with a DLQ table that doesn't actually exist in the DB
    # (we create the canonical schema but skip create_all so the INSERT fails).
    metadata = MetaData()
    missing_dlq = make_dlq_table(metadata, table_name=f"does_not_exist_dlq_{uuid.uuid4().hex[:8]}")
    client = OutboxClient(pg_engine, outbox_table, dlq_table=missing_dlq)

    # Seed an outbox row and acquire its lease so we can drive ``delete_with_lease``.
    async with pg_engine.begin() as conn:
        await conn.execute(insert(outbox_table).values(queue="orders", payload=b"poison"))
    async with pg_engine.connect() as fetch_conn:
        rows = await client.fetch(fetch_conn, ["orders"], limit=1, lease_ttl_seconds=60.0)
    assert len(rows) == 1
    row = rows[0]

    async with pg_engine.connect() as raw_conn:
        writer_conn = await raw_conn.execution_options(isolation_level="AUTOCOMMIT")
        with pytest.raises(Exception, match="does_not_exist_dlq"):
            await client.delete_with_lease(
                writer_conn,
                row.id,
                row.acquired_token,  # ty: ignore[invalid-argument-type]
                dlq_payload={"failure_reason": "retry_terminal", "last_exception": "RuntimeError('x')"},
            )

    # Outbox row still present — the CTE was atomic, INSERT failure rolled back DELETE.
    assert await _row_count(pg_engine, outbox_table) == 1


async def test_dlq_cte_returns_false_when_lease_already_lost(
    pg_engine: AsyncEngine,
    outbox_table: Table,
    dlq_table: Table,
) -> None:
    """When the lease was reclaimed by another worker, the CTE deletes nothing AND writes no DLQ row."""
    client = OutboxClient(pg_engine, outbox_table, dlq_table=dlq_table)
    async with pg_engine.begin() as conn:
        await conn.execute(insert(outbox_table).values(queue="orders", payload=b"x"))
    async with pg_engine.connect() as fetch_conn:
        rows = await client.fetch(fetch_conn, ["orders"], limit=1, lease_ttl_seconds=60.0)

    async with pg_engine.connect() as raw_conn:
        writer_conn = await raw_conn.execution_options(isolation_level="AUTOCOMMIT")
        # Wrong token = lease lost.
        deleted = await client.delete_with_lease(
            writer_conn,
            rows[0].id,
            uuid.uuid4(),
            dlq_payload={"failure_reason": "retry_terminal", "last_exception": "RuntimeError('x')"},
        )
    assert deleted is False
    assert await _row_count(pg_engine, outbox_table) == 1  # outbox row still leased
    assert await _row_count(pg_engine, dlq_table) == 0  # no DLQ row from a no-op CTE


async def test_relay_at_least_once_under_foreign_publish_failure(
    pg_engine: AsyncEngine,
    outbox_table: Table,
) -> None:
    """Assert at-least-once delivery to a foreign broker under simulated publish failure.

    Foreign publish that fails on the first attempt is retried via the
    outbox's retry_strategy; the row eventually clears after a successful
    second attempt.
    """
    broker_outbox = OutboxBroker(pg_engine, outbox_table=outbox_table)
    broker_kafka = KafkaBroker("kafka://test:9092")
    publisher_kafka = broker_kafka.publisher("relay_topic")

    call_count = 0
    delivered_bodies: list[Any] = []
    original_publish = publisher_kafka._publish  # noqa: SLF001

    async def flaky_publish(cmd: Any, **kwargs: Any) -> Any:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            msg = "simulated foreign-publish failure"
            raise RuntimeError(msg)
        delivered_bodies.append(cmd.body)  # F7-11: capture what the successful retry actually relayed
        return await original_publish(cmd, **kwargs)

    @publisher_kafka
    @broker_outbox.subscriber(
        "relay_queue",
        max_workers=1,
        min_fetch_interval=0.05,
        max_fetch_interval=0.2,
        lease_ttl_seconds=2.0,
        retry_strategy=ConstantRetry(delay_seconds=0.1, max_attempts=5),
    )
    async def relay(body: dict[str, Any]) -> dict[str, Any]:
        return body

    session_factory = async_sessionmaker(pg_engine, expire_on_commit=False)

    with mock.patch.object(publisher_kafka, "_publish", side_effect=flaky_publish):
        async with TestKafkaBroker(broker_kafka), broker_outbox:
            async with session_factory() as session, session.begin():
                await broker_outbox.publish(
                    {"body": "first"},
                    queue="relay_queue",
                    session=session,
                )
            await _wait_until(lambda: call_count >= 2, timeout=10.0)

    assert call_count >= 2, (
        f"Expected at-least-once delivery via retry, but foreign _publish was called {call_count} time(s)."
    )
    # F7-11: the successful retry must carry the original body — a retry that relayed an
    # empty/wrong payload would pass the call-count + row-deletion checks alone.
    assert delivered_bodies
    assert delivered_bodies[-1] == {"body": "first"}
    assert await _row_count(pg_engine, outbox_table) == 0, "Row should be deleted after successful retry."


# --- live worker-loop coverage of the two load-bearing concurrency paths ------
# (audit 2026-06-14, MEDIUM test gaps: lease-expiry mid-handler; relay config-error)


async def test_lease_expiry_during_inflight_handler_redelivers_without_clobber(
    pg_engine: AsyncEngine,
    outbox_table: Table,
) -> None:
    """The lease-token invariant, driven end-to-end through the live worker loop.

    A handler that outlives ``lease_ttl_seconds`` has its row reclaimed and
    redelivered to a second worker mid-flight. The slow holder's terminal DELETE
    must find ``rowcount == 0`` (the new holder already owns the row) and be
    dropped — emitting ``lease_lost(phase=terminal)`` instead of ``acked`` — so it
    cannot clobber the new lease holder. The row is deleted exactly once.

    Prior coverage only fed ``delete_with_lease`` a synthetic wrong token and
    reclaimed *idle* rows; this is the first test that races a real running
    handler against its own lease expiry through ``_fetch_loop``/``_worker_loop``.
    """
    events: list[tuple[str, dict[str, Any]]] = []

    def recorder(event: str, tags: Mapping[str, Any]) -> None:
        events.append((event, dict(tags)))

    release = asyncio.Event()
    deliveries: list[int] = []

    broker = OutboxBroker(pg_engine, outbox_table=outbox_table, metrics_recorder=recorder)

    @broker.subscriber(
        "orders",
        min_fetch_interval=0.02,
        max_fetch_interval=0.05,
        max_workers=2,  # worker A blocks; worker B must be free to take the reclaim
        lease_ttl_seconds=0.3,
    )
    async def handle(body: dict[str, Any]) -> None:
        del body
        idx = len(deliveries)
        deliveries.append(idx)
        if idx == 0:
            # First (slow) delivery: block past lease_ttl_seconds so the row's
            # lease expires and a second fetch reclaims + redelivers it.
            await release.wait()

    session_factory = async_sessionmaker(pg_engine, expire_on_commit=False)
    async with broker:
        async with session_factory() as session, session.begin():
            await broker.publish({"i": 0}, queue="orders", session=session)
        # Worker B reclaims the expired lease, is redelivered the row, and deletes it
        # (``acked``) — all while worker A is still blocked in its slow handler. The
        # ``acked`` event is exactly the moment B's terminal DELETE lands, so waiting on
        # it deterministically orders B's success ahead of A's release below.
        await _wait_until(lambda: len(deliveries) >= 2, timeout=10.0)
        await _wait_until(lambda: any(e == "acked" for e, _ in events), timeout=10.0)
        # Now unblock the slow holder; its terminal DELETE must be a no-op (lease lost).
        release.set()
        await _wait_until(
            lambda: any(e == "lease_lost" and t.get("phase") == "terminal" for e, t in events),
            timeout=10.0,
        )

    assert len(deliveries) >= 2, "row was never redelivered — lease expiry did not reclaim it"
    acked = [t for e, t in events if e == "acked"]
    lease_lost_terminal = [t for e, t in events if e == "lease_lost" and t.get("phase") == "terminal"]
    assert len(acked) == 1, f"exactly one terminal DELETE must land (the new holder), got {len(acked)}"
    assert lease_lost_terminal, "the stale holder's DELETE must be dropped with lease_lost(phase=terminal)"
    assert await _row_count(pg_engine, outbox_table) == 0, "row must be deleted exactly once"


async def test_relay_dual_fire_guard_through_worker_loop_leaves_row_and_logs(
    pg_engine: AsyncEngine,
    outbox_table: Table,
) -> None:
    """The OutboxResponse + foreign-publisher dual-fire guard, through the live worker loop.

    Every existing relay-chain test runs in ``TestOutboxBroker(run_loops=False)``,
    where the handler runs synchronously inside ``publish`` and the guard raises out
    of ``dispatch_one``. The *worker-loop* contract is different: ``_worker_inner``
    catches the ``_OutboxConfigError``, logs it at ERROR, and leaves the row for
    lease-expiry retry (it must NOT delete it or fire the foreign publish). This is
    the first test that exercises that catch path end-to-end against real Postgres.
    """
    broker_outbox = OutboxBroker(pg_engine, outbox_table=outbox_table)
    broker_kafka = KafkaBroker("kafka://test:9092")
    publisher_kafka = broker_kafka.publisher("relay_topic")

    @publisher_kafka
    @broker_outbox.subscriber(
        "relay_queue",
        max_workers=1,
        min_fetch_interval=0.05,
        max_fetch_interval=0.2,
        lease_ttl_seconds=30.0,  # long: keep redelivery noise out of the assertion window
    )
    async def relay(body: dict[str, Any]) -> OutboxResponse:
        # Valid session so eager validation passes; the dual-fire guard fires before it is used.
        return OutboxResponse(body=body, queue="next_queue", session=mock.AsyncMock(spec=AsyncSession))

    errors: list[str] = []
    session_factory = async_sessionmaker(pg_engine, expire_on_commit=False)
    # TestKafkaBroker first so the foreign producer is wired before the outbox start() probe.
    async with TestKafkaBroker(broker_kafka), broker_outbox:
        sub = next(iter(broker_outbox._subscribers))  # noqa: SLF001
        original_log = sub._log  # noqa: SLF001

        def spy_log(*args: Any, **kwargs: Any) -> Any:
            if kwargs.get("log_level") == logging.ERROR and "configuration error" in str(kwargs.get("message", "")):
                errors.append(str(kwargs["message"]))
            return original_log(*args, **kwargs)

        with mock.patch.object(sub, "_log", side_effect=spy_log):
            async with session_factory() as session, session.begin():
                await broker_outbox.publish({"x": 1}, queue="relay_queue", session=session)
            await _wait_until(lambda: bool(errors), timeout=10.0)

    # Guard fired before the chain → the foreign Kafka publish never happened.
    publisher_kafka.mock.assert_not_called()
    # Row left in place (lease held, not deleted) for lease-expiry retry.
    assert await _row_count(pg_engine, outbox_table) == 1, "config-error row must be left for lease-expiry, not deleted"
    # And the _OutboxConfigError was logged at ERROR by the worker loop.
    assert errors, "worker loop must log the _OutboxConfigError at ERROR"


async def test_listen_failure_falls_back_to_polling_against_real_postgres(
    pg_engine: AsyncEngine,
    outbox_table: Table,
) -> None:
    """With the LISTEN connection unavailable, delivery still happens via polling.

    Prior coverage stubbed ``asyncpg.connect``/``add_listener`` to fail in unit tests;
    this drives a real subscriber against live Postgres whose ``_open_listen_connection``
    returns None (the documented fallback) and asserts the row is still delivered within
    the polling interval — proving the fetch loop does not wedge without LISTEN.
    """
    received: list[dict[str, Any]] = []
    broker = OutboxBroker(pg_engine, outbox_table=outbox_table)

    @broker.subscriber("orders", min_fetch_interval=0.05, max_fetch_interval=0.3)
    async def handle(body: dict[str, Any]) -> None:
        received.append(body)

    sub = next(iter(broker._subscribers))  # noqa: SLF001
    session_factory = async_sessionmaker(pg_engine, expire_on_commit=False)
    # Force the LISTEN setup to report failure → polling-only path.
    with mock.patch.object(sub, "_open_listen_connection", new=mock.AsyncMock(return_value=None)):
        async with broker:
            async with session_factory() as session, session.begin():
                await broker.publish({"order_id": 7}, queue="orders", session=session)
            await _wait_until(lambda: bool(received), timeout=5.0)

    assert received == [{"order_id": 7}]
    assert await _row_count(pg_engine, outbox_table) == 0


async def test_worker_rebuilds_writer_connection_after_flush_failure(
    pg_engine: AsyncEngine,
    outbox_table: Table,
) -> None:
    """A terminal-write failure poisons the worker's writer connection; it must rebuild and recover.

    Prior coverage only drove this with MagicMock engines (asserting ``connect`` was called
    twice). Here a real terminal ``delete_with_lease`` raises once against live Postgres,
    forcing ``_run_with_reconnect`` to tear down and reopen the AUTOCOMMIT writer connection.
    The row then redelivers via lease expiry and clears on the second, successful attempt —
    proving the rebuilt connection actually drains rows.
    """
    received: list[int] = []
    broker = OutboxBroker(pg_engine, outbox_table=outbox_table)

    @broker.subscriber(
        "orders",
        min_fetch_interval=0.05,
        max_fetch_interval=0.2,
        max_workers=1,
        lease_ttl_seconds=1.0,  # short so the un-deleted row is reclaimed quickly
    )
    async def handle(body: dict[str, Any]) -> None:
        received.append(body["i"])

    client = broker.client
    original_delete = client.delete_with_lease
    calls = {"n": 0}

    async def flaky_delete(*args: Any, **kwargs: Any) -> bool:
        calls["n"] += 1
        if calls["n"] == 1:
            msg = "simulated writer-connection flush failure"
            raise RuntimeError(msg)
        return await original_delete(*args, **kwargs)

    session_factory = async_sessionmaker(pg_engine, expire_on_commit=False)
    with mock.patch.object(client, "delete_with_lease", side_effect=flaky_delete):
        async with broker:
            async with session_factory() as session, session.begin():
                await broker.publish({"i": 0}, queue="orders", session=session)
            # First delete raises (poisons the writer conn); after reconnect + lease-expiry
            # reclaim, the second delete lands and the row clears.
            await _wait_until(lambda: calls["n"] >= 2, timeout=15.0)

            async def _deleted() -> bool:
                return await _row_count(pg_engine, outbox_table) == 0

            deadline = asyncio.get_event_loop().time() + 15.0
            while asyncio.get_event_loop().time() < deadline:
                if await _deleted():
                    break
                await asyncio.sleep(0.1)  # pragma: no cover  # slow-hardware safety valve
            else:  # pragma: no cover
                msg = "row not cleared after writer-connection rebuild"
                raise AssertionError(msg)

    assert calls["n"] >= 2, "the terminal write must be retried after the connection rebuild"
    assert received  # handler ran (at least once; redelivery may run it again)


# --- behavior-change Lows (audit 2026-06-14) ----------------------------------


async def test_graceful_timeout_none_still_bounds_drain(
    pg_engine: AsyncEngine,
    outbox_table: Table,
) -> None:
    """``graceful_timeout=None`` must not hang ``stop()`` on a wedged handler.

    None stays "unbounded" for ``ping()``, but the drain clamps it to a finite fallback so
    a single stuck handler can't make ``stop()`` (hence pod shutdown) hang forever. Without
    the clamp ``anyio.move_on_after(None)`` has deadline=inf and this test would hang. The
    module fallback is patched down so the test stays fast.
    """
    broker = OutboxBroker(pg_engine, outbox_table=outbox_table, graceful_timeout=None)
    started = asyncio.Event()

    @broker.subscriber("orders", min_fetch_interval=0.02, max_fetch_interval=0.05)
    async def handle(body: dict[str, Any]) -> None:
        del body
        started.set()
        await asyncio.sleep(60.0)  # wedged — never returns voluntarily

    session_factory = async_sessionmaker(pg_engine, expire_on_commit=False)
    with mock.patch("faststream_outbox.subscriber.usecase._DEFAULT_DRAIN_TIMEOUT_SECONDS", 0.3):
        async with broker:
            async with session_factory() as session, session.begin():
                await broker.publish({"i": 0}, queue="orders", session=session)
            await asyncio.wait_for(started.wait(), timeout=3.0)
            start = asyncio.get_event_loop().time()
            await broker.stop()
            elapsed = asyncio.get_event_loop().time() - start

    assert elapsed < 0.7, f"broker.stop() took {elapsed:.3f}s — graceful_timeout=None did not clamp the drain"
    # Row preserved with its lease set; another replica reclaims after lease_ttl.
    assert await _row_count(pg_engine, outbox_table) == 1


async def test_validate_schema_fails_when_lease_check_constraint_missing(
    pg_engine: AsyncEngine,
    outbox_table: Table,
) -> None:
    """A dropped ``<table>_lease_ck`` CHECK must be caught — alembic's diff can't see it (audit 2026-06-14)."""
    drop_sql = f'ALTER TABLE "{outbox_table.name}" DROP CONSTRAINT "{outbox_table.name}_lease_ck"'
    async with pg_engine.begin() as conn:
        await conn.exec_driver_sql(drop_sql)
    client = OutboxClient(pg_engine, outbox_table)
    with pytest.raises(RuntimeError, match="missing CHECK constraint") as excinfo:
        await client.validate_schema()
    assert "operations/alembic/#fixing-drift-autogenerate-cant-see" in str(excinfo.value)


async def test_validate_schema_passes_under_ck_naming_convention(
    pg_engine: AsyncEngine,
) -> None:
    """A MetaData with a ``ck`` naming convention renames the lease CHECK to ``ck_<t>_<t>_lease_ck``.

    The probe must look it up under that resolved name (carried on the constraint object), not the
    literal ``<t>_lease_ck`` — otherwise a perfectly valid schema falsely raises "missing CHECK
    constraint" for every deployment using the SQLAlchemy/Alembic-recommended convention.
    """
    convention = {"ck": "ck_%(table_name)s_%(constraint_name)s"}
    metadata = MetaData(naming_convention=convention)
    table_name = f"test_outbox_{uuid.uuid4().hex[:12]}"
    table = make_outbox_table(metadata, table_name=table_name)
    async with pg_engine.begin() as conn:
        await conn.run_sync(metadata.create_all)
    try:
        client = OutboxClient(pg_engine, table)
        await client.validate_schema()  # must NOT raise
    finally:
        async with pg_engine.begin() as conn:
            await conn.run_sync(metadata.drop_all)


async def test_validate_schema_fails_when_lease_check_constraint_predicate_wrong(
    pg_engine: AsyncEngine,
    outbox_table: Table,
) -> None:
    """A drifted lease-CHECK predicate no longer enforces the invariant, so it reads as missing."""
    name = outbox_table.name
    async with pg_engine.begin() as conn:
        await conn.exec_driver_sql(f'ALTER TABLE "{name}" DROP CONSTRAINT "{name}_lease_ck"')
        # Re-add under the same name with a different (wrong) predicate.
        await conn.exec_driver_sql(
            f'ALTER TABLE "{name}" ADD CONSTRAINT "{name}_lease_ck" '
            f"CHECK (acquired_token IS NOT NULL OR acquired_at IS NULL)",
        )
    client = OutboxClient(pg_engine, outbox_table)
    with pytest.raises(RuntimeError, match="missing CHECK constraint enforcing") as excinfo:
        await client.validate_schema()
    # The predicate probe is Alembic-blind too, so the remediation pointer must fire.
    assert "operations/alembic/#fixing-drift-autogenerate-cant-see" in str(excinfo.value)


async def test_validate_schema_passes_under_ck_convention_with_literally_named_constraint(
    pg_engine: AsyncEngine,
) -> None:
    """Convention metadata + a literally-named lease CHECK must validate (the reported case).

    A ``MetaData`` carries a ``ck`` naming convention, but the lease CHECK was created by a
    hand-written migration under the **literal** ``<table>_lease_ck`` name (Alembic op functions
    don't apply the convention). The in-memory Table's convention-doubled name (``ck_<t>_<t>_lease_ck``)
    differs from the live literal name — yet the predicate matches, so validate_schema must pass.
    """
    convention = {"ck": "ck_%(table_name)s_%(constraint_name)s"}
    metadata = MetaData(naming_convention=convention)
    table_name = f"test_outbox_{uuid.uuid4().hex[:12]}"
    table = make_outbox_table(metadata, table_name=table_name)
    async with pg_engine.begin() as conn:
        await conn.run_sync(metadata.create_all)
        # Replace the convention-doubled constraint with one under the literal name a
        # ``op.create_check_constraint('<table>_lease_ck', ...)`` migration would create.
        await conn.exec_driver_sql(
            f'ALTER TABLE "{table_name}" DROP CONSTRAINT "ck_{table_name}_{table_name}_lease_ck"',
        )
        await conn.exec_driver_sql(
            f'ALTER TABLE "{table_name}" ADD CONSTRAINT "{table_name}_lease_ck" '
            f"CHECK ((acquired_token IS NULL) = (acquired_at IS NULL))",
        )
    try:
        client = OutboxClient(pg_engine, table)
        await client.validate_schema()  # must NOT raise
    finally:
        async with pg_engine.begin() as conn:
            await conn.run_sync(metadata.drop_all)


async def test_validate_schema_autovacuum_raises_when_untuned(pg_engine: AsyncEngine, outbox_table: Table) -> None:
    """Fresh table (the fixture applies no reloptions) -> distinctly-labeled raise, not a schema mismatch."""
    client = OutboxClient(pg_engine, outbox_table)
    with pytest.raises(RuntimeError, match="autovacuum not tuned") as excinfo:
        await client.validate_schema(check_autovacuum=True)
    assert "schema mismatch" not in str(excinfo.value)


async def test_validate_schema_autovacuum_ok_when_applied(pg_engine: AsyncEngine, outbox_table: Table) -> None:
    async with pg_engine.begin() as conn:
        await conn.execute(text(outbox_autovacuum_ddl(outbox_table.name)))
    client = OutboxClient(pg_engine, outbox_table)
    await client.validate_schema(check_autovacuum=True)  # must NOT raise


async def test_validate_schema_autovacuum_ok_with_custom_threshold(
    pg_engine: AsyncEngine,
    outbox_table: Table,
) -> None:
    # scale_factor still 0, but a deliberately different threshold -> no false raise.
    async with pg_engine.begin() as conn:
        await conn.execute(text(outbox_autovacuum_ddl(outbox_table.name, vacuum_threshold=5000)))
    client = OutboxClient(pg_engine, outbox_table)
    await client.validate_schema(check_autovacuum=True)  # must NOT raise


async def test_validate_schema_autovacuum_raises_when_scale_factor_nonzero(
    pg_engine: AsyncEngine,
    outbox_table: Table,
) -> None:
    # A user who set a nonzero scale factor is still exposed to the death-spiral.
    async with pg_engine.begin() as conn:
        await conn.execute(
            text(f'ALTER TABLE "{outbox_table.name}" SET (autovacuum_vacuum_scale_factor = 0.1)'),
        )
    client = OutboxClient(pg_engine, outbox_table)
    with pytest.raises(RuntimeError, match="autovacuum not tuned"):
        await client.validate_schema(check_autovacuum=True)


async def test_validate_schema_autovacuum_false_does_not_check(pg_engine: AsyncEngine, outbox_table: Table) -> None:
    """``check_autovacuum=False`` (the default) never raises for autovacuum, even on an untuned table."""
    client = OutboxClient(pg_engine, outbox_table)
    await client.validate_schema(check_autovacuum=False)  # must NOT raise
    await client.validate_schema()  # default also must NOT raise


async def test_validate_schema_autovacuum_ok_when_applied_in_named_schema(pg_engine: AsyncEngine) -> None:
    """Full round-trip: ``validate_schema(check_autovacuum=True)`` on a named-schema table does not raise.

    The DDL helper's ``schema=`` must target the same table the schema-aware reloptions query
    reads: ``outbox_autovacuum_ddl(name, schema=table.schema)`` applies the reloptions, and the
    check (schema-aware via ``COALESCE(:schema, current_schema())``, plus the schema-reflecting
    ``validate_schema`` diff) confirms them — neither the schema probe nor the autovacuum probe
    falsely fires for a correct table in a non-default ``MetaData(schema=...)``.
    """
    schema = f"sch_av_{uuid.uuid4().hex[:8]}"
    metadata = MetaData(schema=schema)
    table = make_outbox_table(metadata, table_name="outbox")
    async with pg_engine.begin() as conn:
        await conn.execute(text(f'CREATE SCHEMA "{schema}"'))
        await conn.run_sync(metadata.create_all)
    try:
        async with pg_engine.begin() as conn:
            await conn.execute(text(outbox_autovacuum_ddl(table.name, schema=table.schema)))
        client = OutboxClient(pg_engine, table)
        await client.validate_schema(check_autovacuum=True)  # must NOT raise
    finally:
        async with pg_engine.begin() as conn:
            await conn.run_sync(metadata.drop_all)
            await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))


@contextlib.contextmanager
def _count_pg_notify(engine: AsyncEngine):
    """Count executed ``pg_notify`` statements on *engine* via a cursor-execute listener.

    Returns a one-element mutable list so a test can also reset it mid-transaction.
    Listens on ``engine.sync_engine`` (the AsyncEngine routes DBAPI execution through it).
    """
    count = [0]

    def _listener(conn, cursor, statement, parameters, context, executemany) -> None:  # noqa: ARG001
        if "pg_notify" in statement.lower():
            count[0] += 1

    event.listen(engine.sync_engine, "before_cursor_execute", _listener)
    try:
        yield count
    finally:
        event.remove(engine.sync_engine, "before_cursor_execute", _listener)


async def test_notify_deduped_within_transaction_same_queue(pg_engine, outbox_table) -> None:
    broker = OutboxBroker(pg_engine, outbox_table=outbox_table)
    session_factory = async_sessionmaker(pg_engine, expire_on_commit=False)
    with _count_pg_notify(pg_engine) as count:
        async with session_factory() as session, session.begin():
            for _ in range(5):
                await broker.publish({"n": 1}, queue="orders", session=session)
    assert count[0] == 1  # 5 publishes, one queue, one txn -> one pg_notify


async def test_notify_one_per_distinct_queue_within_transaction(pg_engine, outbox_table) -> None:
    broker = OutboxBroker(pg_engine, outbox_table=outbox_table)
    session_factory = async_sessionmaker(pg_engine, expire_on_commit=False)
    with _count_pg_notify(pg_engine) as count:
        async with session_factory() as session, session.begin():
            await broker.publish({"n": 1}, queue="a", session=session)
            await broker.publish({"n": 2}, queue="b", session=session)
            await broker.publish({"n": 3}, queue="a", session=session)  # deduped
    assert count[0] == 2  # queues a, b -> two pg_notify


async def test_notify_re_emits_in_separate_transaction(pg_engine, outbox_table) -> None:
    broker = OutboxBroker(pg_engine, outbox_table=outbox_table)
    session_factory = async_sessionmaker(pg_engine, expire_on_commit=False)
    with _count_pg_notify(pg_engine) as count:
        async with session_factory() as session:
            async with session.begin():
                await broker.publish({"n": 1}, queue="orders", session=session)
                await broker.publish({"n": 2}, queue="orders", session=session)  # deduped
            async with session.begin():  # new transaction, same session
                await broker.publish({"n": 3}, queue="orders", session=session)  # must re-emit
    assert count[0] == 2  # one per transaction (the per-transaction reset)


async def test_notify_re_emits_after_savepoint_rollback(pg_engine, outbox_table) -> None:
    # Innermost-transaction key: a publish inside a rolled-back savepoint must NOT
    # suppress a later publish of the same queue in the outer transaction.
    broker = OutboxBroker(pg_engine, outbox_table=outbox_table)
    session_factory = async_sessionmaker(pg_engine, expire_on_commit=False)

    class _RollbackError(Exception):
        pass

    with _count_pg_notify(pg_engine) as count:
        async with session_factory() as session, session.begin():
            with contextlib.suppress(_RollbackError):
                async with session.begin_nested():
                    await broker.publish({"n": 1}, queue="orders", session=session)
                    raise _RollbackError  # roll the savepoint back
            count[0] = 0  # reset AFTER the savepoint: isolate the outer publish
            await broker.publish({"n": 2}, queue="orders", session=session)  # must emit
    assert count[0] == 1  # the outer republish emitted despite the rolled-back savepoint's publish


async def test_notify_dedup_spans_publish_batch_and_publish(pg_engine, outbox_table) -> None:
    broker = OutboxBroker(pg_engine, outbox_table=outbox_table)
    session_factory = async_sessionmaker(pg_engine, expire_on_commit=False)
    with _count_pg_notify(pg_engine) as count:
        async with session_factory() as session, session.begin():
            await broker.publish_batch({"n": 1}, {"n": 2}, queue="orders", session=session)  # 1 pg_notify
            await broker.publish({"n": 3}, queue="orders", session=session)  # deduped
    assert count[0] == 1


async def test_notify_still_skipped_for_future_dated(pg_engine, outbox_table) -> None:
    broker = OutboxBroker(pg_engine, outbox_table=outbox_table)
    session_factory = async_sessionmaker(pg_engine, expire_on_commit=False)
    with _count_pg_notify(pg_engine) as count:
        async with session_factory() as session, session.begin():
            await broker.publish({"n": 1}, queue="orders", session=session, activate_in=_dt.timedelta(minutes=5))
    assert count[0] == 0  # future-dated -> no NOTIFY (unchanged), dedup did not break the skip
