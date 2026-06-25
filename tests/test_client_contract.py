"""Cross-adapter contract for ``AbstractOutboxClient``.

The two adapters — ``OutboxClient`` (SQL/Postgres) and ``FakeOutboxClient``
(in-memory) — implement the same rules in different substrates: the real client runs
eligibility / lease cutoff / retry timing *inside Postgres*; the fake runs the
equivalents in Python. Those implementations cannot share code, so this module pins
them to one behavioural contract instead. Every scenario runs against **both** the
fake (everywhere) and real Postgres (auto-skipped when unreachable).

Scope is exactly the shared ``AbstractOutboxClient`` surface: ``fetch``,
``delete_with_lease``, ``mark_pending_with_lease`` (+ the DLQ side-effect).
``cancel_timer`` and ``timer_id`` insert-dedup live on the broker / producer, not on
the client interface, so they are covered by ``test_integration.py`` /
``test_fake.py``, not here.

A per-adapter harness hides the substrate differences (how a row is seeded, which
connection a terminal write needs) so the scenarios read adapter-agnostically.
Scheduling is expressed as **offsets from now** seeded server-side on the real path
(``now() + make_interval(...)``) — the same clock-skew-free idiom the existing
fake/real predicate-parity test uses. Expectations are hand-specified, never computed
from a shared helper, so neither adapter can pass trivially against itself.

The one drift this cannot catch: an in-process test can't manufacture cross-host
DB-vs-worker clock skew, so the real client's server-side ``make_interval`` clock
authority stays a documented invariant (CLAUDE.md), not an assertion here.
"""

import datetime as _dt
import os
import uuid
from collections.abc import AsyncIterator

import pytest
from sqlalchemy import MetaData, Table, insert, select, text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from faststream_outbox import make_dlq_table, make_outbox_table
from faststream_outbox._time import utcnow
from faststream_outbox.client import OutboxClient
from faststream_outbox.message import OutboxInnerMessage
from faststream_outbox.testing import FakeOutboxClient


pytestmark = pytest.mark.asyncio

PG_DSN = os.environ.get("POSTGRES_DSN", "postgresql+asyncpg://outbox:outbox@localhost:5432/outbox")


class _FakeHarness:
    """Adapter-agnostic harness backed by an in-memory ``FakeOutboxClient``."""

    kind = "fake"

    def __init__(self) -> None:
        self._client = FakeOutboxClient()

    async def seed(
        self,
        *,
        queue: str = "q",
        payload: bytes = b"x",
        next_attempt_offset: float = -1.0,
        timer_id: str | None = None,
        leased_token: uuid.UUID | None = None,
        acquired_age: float | None = None,
        deliveries_count: int = 0,
    ) -> int:
        rid = self._client.feed(
            queue=queue,
            payload=payload,
            next_attempt_at=utcnow() + _dt.timedelta(seconds=next_attempt_offset),
            timer_id=timer_id,
        )
        assert rid is not None
        row = next(r for r in self._client.rows if r.id == rid)
        row.deliveries_count = deliveries_count
        if leased_token is not None:
            row.acquired_token = leased_token
            row.acquired_at = utcnow() - _dt.timedelta(seconds=acquired_age or 0.0)
        return rid

    async def fetch(self, queues: list[str], *, limit: int, lease_ttl_seconds: float) -> list[OutboxInnerMessage]:
        return await self._client.fetch(None, queues, limit=limit, lease_ttl_seconds=lease_ttl_seconds)

    async def delete(
        self,
        message_id: int,
        token: uuid.UUID,
        *,
        dlq_payload: dict[str, object] | None = None,
    ) -> bool:
        return await self._client.delete_with_lease(None, message_id, token, dlq_payload=dlq_payload)

    async def mark_pending(
        self,
        message_id: int,
        token: uuid.UUID,
        *,
        delay_seconds: float,
        attempts_count: int,
        first_attempt_at: _dt.datetime,
        last_attempt_at: _dt.datetime,
    ) -> bool:
        return await self._client.mark_pending_with_lease(
            None,
            message_id,
            token,
            delay_seconds=delay_seconds,
            attempts_count=attempts_count,
            first_attempt_at=first_attempt_at,
            last_attempt_at=last_attempt_at,
        )

    async def get_row(self, message_id: int) -> dict[str, object] | None:
        row = next((r for r in self._client.rows if r.id == message_id), None)
        if row is None:
            return None
        return {
            "id": row.id,
            "queue": row.queue,
            "payload": bytes(row.payload),
            "acquired_token": row.acquired_token,
            "acquired_at": row.acquired_at,
            "attempts_count": row.attempts_count,
            "deliveries_count": row.deliveries_count,
            "next_attempt_at": row.next_attempt_at,
            "timer_id": row.timer_id,
        }

    async def dlq_rows(self) -> list[dict[str, object]]:
        return [dict(r) for r in self._client.dlq_rows]


class _RealHarness:
    """Adapter-agnostic harness backed by a real ``OutboxClient`` against Postgres."""

    kind = "real"

    def __init__(self, engine: AsyncEngine, table: Table, dlq: Table) -> None:
        self._engine = engine
        self._table = table
        self._dlq = dlq
        self._client = OutboxClient(engine, table, dlq_table=dlq)

    async def seed(
        self,
        *,
        queue: str = "q",
        payload: bytes = b"x",
        next_attempt_offset: float = -1.0,
        timer_id: str | None = None,
        leased_token: uuid.UUID | None = None,
        acquired_age: float | None = None,
        deliveries_count: int = 0,
    ) -> int:
        # Server-side ``now()`` arithmetic (not a Python literal) keeps the offsets
        # clock-skew-free and lets the column default never sneak in.
        values: dict[str, object] = {
            "queue": queue,
            "payload": payload,
            "next_attempt_at": text("now() + make_interval(secs => :nas)").bindparams(nas=next_attempt_offset),
            "deliveries_count": deliveries_count,
        }
        if timer_id is not None:
            values["timer_id"] = timer_id
        if leased_token is not None:
            values["acquired_token"] = leased_token
            values["acquired_at"] = text("now() - make_interval(secs => :aas)").bindparams(aas=acquired_age or 0.0)
        async with self._engine.begin() as conn:
            result = await conn.execute(insert(self._table).values(**values).returning(self._table.c.id))
            return result.scalar_one()

    async def fetch(self, queues: list[str], *, limit: int, lease_ttl_seconds: float) -> list[OutboxInnerMessage]:
        async with self._engine.connect() as conn:
            return await self._client.fetch(conn, queues, limit=limit, lease_ttl_seconds=lease_ttl_seconds)

    async def delete(
        self,
        message_id: int,
        token: uuid.UUID,
        *,
        dlq_payload: dict[str, object] | None = None,
    ) -> bool:
        async with self._engine.connect() as raw:
            writer = await raw.execution_options(isolation_level="AUTOCOMMIT")
            return await self._client.delete_with_lease(writer, message_id, token, dlq_payload=dlq_payload)

    async def mark_pending(
        self,
        message_id: int,
        token: uuid.UUID,
        *,
        delay_seconds: float,
        attempts_count: int,
        first_attempt_at: _dt.datetime,
        last_attempt_at: _dt.datetime,
    ) -> bool:
        async with self._engine.connect() as raw:
            writer = await raw.execution_options(isolation_level="AUTOCOMMIT")
            return await self._client.mark_pending_with_lease(
                writer,
                message_id,
                token,
                delay_seconds=delay_seconds,
                attempts_count=attempts_count,
                first_attempt_at=first_attempt_at,
                last_attempt_at=last_attempt_at,
            )

    async def get_row(self, message_id: int) -> dict[str, object] | None:
        async with self._engine.connect() as conn:
            result = await conn.execute(select(self._table).where(self._table.c.id == message_id))
            mapping = result.mappings().first()
            if mapping is None:
                return None
            row = dict(mapping)
            row["payload"] = bytes(row["payload"])
            return row

    async def dlq_rows(self) -> list[dict[str, object]]:
        async with self._engine.connect() as conn:
            result = await conn.execute(select(self._dlq))
            rows = [dict(m) for m in result.mappings().all()]
        for row in rows:
            row["payload"] = bytes(row["payload"])
        return rows


_Harness = _FakeHarness | _RealHarness


@pytest.fixture(params=["fake", "real"])
async def contract(request: pytest.FixtureRequest) -> AsyncIterator[_Harness]:
    """Yield a harness over each adapter. The ``real`` param auto-skips without Postgres."""
    if request.param == "fake":
        yield _FakeHarness()
        return
    engine = create_async_engine(PG_DSN, future=True)
    try:
        async with engine.connect() as conn:
            await conn.exec_driver_sql("SELECT 1")
    except Exception as exc:  # noqa: BLE001  # pragma: no cover
        await engine.dispose()
        pytest.skip(f"Postgres not available at {PG_DSN}: {exc}")
    metadata = MetaData()
    suffix = uuid.uuid4().hex[:12]
    table = make_outbox_table(metadata, table_name=f"test_ctr_outbox_{suffix}")
    dlq = make_dlq_table(metadata, table_name=f"test_ctr_dlq_{suffix}")
    async with engine.begin() as conn:
        await conn.run_sync(metadata.create_all)
    try:
        yield _RealHarness(engine, table, dlq)
    finally:
        async with engine.begin() as conn:
            await conn.run_sync(metadata.drop_all)
        await engine.dispose()


# --- fetch -----------------------------------------------------------------


async def test_fetch_claims_unleased_row(contract: _Harness) -> None:
    rid = await contract.seed(queue="orders", payload=b"a")
    msgs = await contract.fetch(["orders"], limit=10, lease_ttl_seconds=60.0)
    assert [m.id for m in msgs] == [rid]
    assert msgs[0].acquired_token is not None
    assert msgs[0].acquired_at is not None
    assert msgs[0].deliveries_count == 1


async def test_fetch_skips_future_dated(contract: _Harness) -> None:
    await contract.seed(queue="orders", next_attempt_offset=3600.0)
    msgs = await contract.fetch(["orders"], limit=10, lease_ttl_seconds=60.0)
    assert msgs == []


async def test_fetch_reclaims_expired_lease(contract: _Harness) -> None:
    old_token = uuid.uuid4()
    rid = await contract.seed(queue="orders", leased_token=old_token, acquired_age=120.0, deliveries_count=1)
    msgs = await contract.fetch(["orders"], limit=10, lease_ttl_seconds=60.0)
    assert [m.id for m in msgs] == [rid]
    assert msgs[0].acquired_token != old_token
    assert msgs[0].deliveries_count == 2


async def test_fetch_skips_fresh_lease(contract: _Harness) -> None:
    await contract.seed(queue="orders", leased_token=uuid.uuid4(), acquired_age=1.0, deliveries_count=1)
    msgs = await contract.fetch(["orders"], limit=10, lease_ttl_seconds=60.0)
    assert msgs == []


async def test_fetch_selects_oldest_under_limit(contract: _Harness) -> None:
    # FIFO *selection* under contention is the contract; within-batch *return order*
    # is unspecified for the real client (F2-09 — the outer UPDATE…RETURNING is
    # unordered), so assert on the chosen set, not its order.
    rid_a = await contract.seed(queue="orders", next_attempt_offset=-10.0)
    rid_b = await contract.seed(queue="orders", next_attempt_offset=-20.0)
    rid_c = await contract.seed(queue="orders", next_attempt_offset=-30.0)
    msgs = await contract.fetch(["orders"], limit=2, lease_ttl_seconds=60.0)
    assert {m.id for m in msgs} == {rid_c, rid_b}
    assert rid_a not in {m.id for m in msgs}


async def test_fetch_respects_limit(contract: _Harness) -> None:
    for _ in range(3):
        await contract.seed(queue="orders")
    msgs = await contract.fetch(["orders"], limit=2, lease_ttl_seconds=60.0)
    assert len(msgs) == 2


async def test_fetch_filters_by_queue(contract: _Harness) -> None:
    await contract.seed(queue="orders")
    await contract.seed(queue="other")
    msgs = await contract.fetch(["orders"], limit=10, lease_ttl_seconds=60.0)
    assert {m.queue for m in msgs} == {"orders"}


# --- delete_with_lease -----------------------------------------------------


async def test_delete_deletes_on_token_match(contract: _Harness) -> None:
    token = uuid.uuid4()
    rid = await contract.seed(leased_token=token, acquired_age=1.0)
    assert await contract.delete(rid, token) is True
    assert await contract.get_row(rid) is None


async def test_delete_noop_on_token_mismatch(contract: _Harness) -> None:
    rid = await contract.seed(leased_token=uuid.uuid4(), acquired_age=1.0)
    assert await contract.delete(rid, uuid.uuid4()) is False
    assert await contract.get_row(rid) is not None


async def test_delete_noop_on_unleased_row(contract: _Harness) -> None:
    rid = await contract.seed()
    assert await contract.delete(rid, uuid.uuid4()) is False
    assert await contract.get_row(rid) is not None


async def test_delete_with_dlq_materializes_audit_row(contract: _Harness) -> None:
    token = uuid.uuid4()
    rid = await contract.seed(queue="orders", payload=b"body", timer_id="t-1", leased_token=token, acquired_age=1.0)
    deleted = await contract.delete(
        rid,
        token,
        dlq_payload={"failure_reason": "boom", "last_exception": "Traceback..."},
    )
    assert deleted is True
    assert await contract.get_row(rid) is None
    dlq = await contract.dlq_rows()
    assert len(dlq) == 1
    assert dlq[0]["original_id"] == rid
    assert dlq[0]["queue"] == "orders"
    assert dlq[0]["payload"] == b"body"
    assert dlq[0]["failure_reason"] == "boom"
    assert dlq[0]["last_exception"] == "Traceback..."
    assert dlq[0]["timer_id"] == "t-1"


# --- mark_pending_with_lease -----------------------------------------------


async def test_mark_pending_reschedules_on_token_match(contract: _Harness) -> None:
    token = uuid.uuid4()
    now = utcnow()
    rid = await contract.seed(next_attempt_offset=-5.0, leased_token=token, acquired_age=1.0)
    ok = await contract.mark_pending(
        rid,
        token,
        delay_seconds=60.0,
        attempts_count=1,
        first_attempt_at=now,
        last_attempt_at=now,
    )
    assert ok is True
    row = await contract.get_row(rid)
    assert row is not None
    assert row["acquired_token"] is None
    assert row["acquired_at"] is None
    assert row["attempts_count"] == 1
    next_attempt_at = row["next_attempt_at"]
    assert isinstance(next_attempt_at, _dt.datetime)
    assert next_attempt_at > utcnow()


async def test_mark_pending_noop_on_token_mismatch(contract: _Harness) -> None:
    token = uuid.uuid4()
    now = utcnow()
    rid = await contract.seed(leased_token=token, acquired_age=1.0)
    ok = await contract.mark_pending(
        rid,
        uuid.uuid4(),
        delay_seconds=60.0,
        attempts_count=1,
        first_attempt_at=now,
        last_attempt_at=now,
    )
    assert ok is False
    row = await contract.get_row(rid)
    assert row is not None
    assert row["acquired_token"] == token
