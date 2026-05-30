import os
import uuid
from collections.abc import AsyncIterator

import pytest
from sqlalchemy import MetaData, Table
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from faststream_outbox import make_dlq_table, make_outbox_table


PG_DSN = os.environ.get("POSTGRES_DSN", "postgresql+asyncpg://outbox:outbox@localhost:5432/outbox")


@pytest.fixture
async def pg_engine() -> AsyncIterator[AsyncEngine]:
    engine = create_async_engine(PG_DSN, future=True)
    try:
        async with engine.connect() as conn:
            await conn.exec_driver_sql("SELECT 1")
    except Exception as exc:  # noqa: BLE001  # pragma: no cover
        await engine.dispose()
        pytest.skip(f"Postgres not available at {PG_DSN}: {exc}")
    yield engine
    await engine.dispose()


@pytest.fixture
async def outbox_table(pg_engine: AsyncEngine) -> AsyncIterator[Table]:
    """
    Per-test outbox table.

    The partial fetch index is declared on the Table itself, so ``create_all``
    brings it up alongside the table.
    """
    metadata = MetaData()
    table_name = f"test_outbox_{uuid.uuid4().hex[:12]}"
    table = make_outbox_table(metadata, table_name=table_name)
    async with pg_engine.begin() as conn:
        await conn.run_sync(metadata.create_all)
    yield table
    async with pg_engine.begin() as conn:
        await conn.run_sync(metadata.drop_all)


@pytest.fixture
async def dlq_table(pg_engine: AsyncEngine) -> AsyncIterator[Table]:
    """Per-test DLQ table on its own MetaData so it lives independently of the outbox fixture."""
    metadata = MetaData()
    table_name = f"test_dlq_{uuid.uuid4().hex[:12]}"
    table = make_dlq_table(metadata, table_name=table_name)
    async with pg_engine.begin() as conn:
        await conn.run_sync(metadata.create_all)
    yield table
    async with pg_engine.begin() as conn:
        await conn.run_sync(metadata.drop_all)
