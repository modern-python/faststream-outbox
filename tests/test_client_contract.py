"""Cross-adapter contract for ``AbstractOutboxClient``.

The two adapters — ``OutboxClient`` (SQL/Postgres) and ``FakeOutboxClient``
(in-memory) — implement the same rules in different substrates: the real client runs
eligibility / lease cutoff / retry timing *inside Postgres*; the fake runs the
equivalents in Python. Those implementations cannot share code, so this module pins
them to one behavioural contract instead. Every scenario runs against **both** the
fake (everywhere) and real Postgres (auto-skipped when unreachable).

Scope is exactly the shared ``AbstractOutboxClient`` surface: ``insert`` /
``insert_batch`` (+ ``timer_id`` dedup and headers JSONB rejects), ``fetch``,
``delete_with_lease``, ``mark_pending_with_lease`` (+ the DLQ side-effect).
``cancel_timer`` lives on the broker, so it is covered by ``test_integration.py`` /
``test_fake.py``, not here.

A per-adapter harness hides the substrate differences (how a row is seeded, which
connection a terminal write needs) so the scenarios read adapter-agnostically.
Scheduling is expressed as **offsets from now** seeded server-side on the real path
(``now() + make_interval(...)``) — the same clock-skew-free idiom the existing
fake/real predicate-parity test uses. Expectations are hand-specified, never computed
from a shared helper, so neither adapter can pass trivially against itself.

The one drift this cannot catch: an in-process test can't manufacture cross-host
DB-vs-worker clock skew, so the real client's server-side ``make_interval`` clock
authority stays a documented invariant (AGENTS.md), not an assertion here.
"""

import contextlib
import datetime as dt
import os
import uuid
from collections.abc import AsyncGenerator, AsyncIterator, Sequence
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import MetaData, Table, insert, select, text
from sqlalchemy.exc import StatementError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

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
    rejects: type[Exception] = ValueError

    def __init__(self, *, with_dlq: bool = True) -> None:
        self._client = FakeOutboxClient(dlq_enabled=with_dlq)

    async def insert(
        self,
        *,
        queue: str = "q",
        payload: bytes = b"x",
        headers: dict[str, str] | None = None,
        activate_in: dt.timedelta | None = None,
        activate_at: dt.datetime | None = None,
        timer_id: str | None = None,
    ) -> int | None:
        return await self._client.insert(
            AsyncMock(spec=AsyncSession),
            queue=queue,
            payload=payload,
            headers=headers,
            activate_in=activate_in,
            activate_at=activate_at,
            timer_id=timer_id,
        )

    async def insert_batch(
        self,
        rows: Sequence[tuple[bytes, dict[str, str] | None]],
        *,
        queue: str = "q",
        activate_in: dt.timedelta | None = None,
    ) -> None:
        await self._client.insert_batch(AsyncMock(spec=AsyncSession), queue=queue, rows=rows, activate_in=activate_in)

    async def queue_ids(self, queue: str) -> list[int]:
        return sorted(r.id for r in self._client.rows if r.queue == queue)

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
            next_attempt_at=utcnow() + dt.timedelta(seconds=next_attempt_offset),
            timer_id=timer_id,
        )
        assert rid is not None
        row = next(r for r in self._client.rows if r.id == rid)
        row.deliveries_count = deliveries_count
        if leased_token is not None:
            row.acquired_token = leased_token
            row.acquired_at = utcnow() - dt.timedelta(seconds=acquired_age or 0.0)
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

    async def delete_batch(self, pairs: list[tuple[int, uuid.UUID]]) -> set[tuple[int, uuid.UUID]]:
        return await self._client.delete_batch_with_lease(None, pairs)

    async def mark_pending(
        self,
        message_id: int,
        token: uuid.UUID,
        *,
        delay_seconds: float,
        attempts_count: int,
        first_attempt_at: dt.datetime,
        last_attempt_at: dt.datetime,
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
    rejects: type[Exception] = StatementError

    def __init__(self, engine: AsyncEngine, table: Table, dlq: Table, *, with_dlq: bool = True) -> None:
        self._engine = engine
        self._table = table
        self._dlq = dlq
        self._client = OutboxClient(engine, table, dlq_table=dlq if with_dlq else None)
        self._sessions = async_sessionmaker(engine, expire_on_commit=False)

    async def insert(
        self,
        *,
        queue: str = "q",
        payload: bytes = b"x",
        headers: dict[str, str] | None = None,
        activate_in: dt.timedelta | None = None,
        activate_at: dt.datetime | None = None,
        timer_id: str | None = None,
    ) -> int | None:
        async with self._sessions() as session, session.begin():
            return await self._client.insert(
                session,
                queue=queue,
                payload=payload,
                headers=headers,
                activate_in=activate_in,
                activate_at=activate_at,
                timer_id=timer_id,
            )

    async def insert_batch(
        self,
        rows: Sequence[tuple[bytes, dict[str, str] | None]],
        *,
        queue: str = "q",
        activate_in: dt.timedelta | None = None,
    ) -> None:
        async with self._sessions() as session, session.begin():
            await self._client.insert_batch(session, queue=queue, rows=rows, activate_in=activate_in)

    async def queue_ids(self, queue: str) -> list[int]:
        async with self._engine.connect() as conn:
            result = await conn.execute(select(self._table.c.id).where(self._table.c.queue == queue))
            return sorted(result.scalars().all())

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

    async def delete_batch(self, pairs: list[tuple[int, uuid.UUID]]) -> set[tuple[int, uuid.UUID]]:
        async with self._engine.connect() as raw:
            writer = await raw.execution_options(isolation_level="AUTOCOMMIT")
            return await self._client.delete_batch_with_lease(writer, pairs)

    async def mark_pending(
        self,
        message_id: int,
        token: uuid.UUID,
        *,
        delay_seconds: float,
        attempts_count: int,
        first_attempt_at: dt.datetime,
        last_attempt_at: dt.datetime,
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


@contextlib.asynccontextmanager
async def _harness(kind: str, *, with_dlq: bool) -> AsyncGenerator[_Harness]:
    if kind == "fake":
        yield _FakeHarness(with_dlq=with_dlq)
        return
    engine = create_async_engine(PG_DSN, future=True)
    try:
        async with engine.connect() as conn:
            await conn.exec_driver_sql("SELECT 1")
    except Exception as exc:  # noqa: BLE001  # pragma: no cover - only when Postgres is unreachable; the test then skips
        await engine.dispose()
        pytest.skip(f"Postgres not available at {PG_DSN}: {exc}")
    metadata = MetaData()
    suffix = uuid.uuid4().hex[:12]
    table = make_outbox_table(metadata, table_name=f"test_ctr_outbox_{suffix}")
    dlq = make_dlq_table(metadata, table_name=f"test_ctr_dlq_{suffix}")
    async with engine.begin() as conn:
        await conn.run_sync(metadata.create_all)
    try:
        yield _RealHarness(engine, table, dlq, with_dlq=with_dlq)
    finally:
        async with engine.begin() as conn:
            await conn.run_sync(metadata.drop_all)
        await engine.dispose()


@pytest.fixture(params=["fake", "real"])
async def contract(request: pytest.FixtureRequest) -> AsyncIterator[_Harness]:
    """Yield a harness over each adapter. The ``real`` param auto-skips without Postgres."""
    async with _harness(request.param, with_dlq=True) as harness:
        yield harness


@pytest.fixture(params=["fake", "real"])
async def contract_without_dlq(request: pytest.FixtureRequest) -> AsyncIterator[_Harness]:
    """Like ``contract``, but the client has no DLQ table."""
    async with _harness(request.param, with_dlq=False) as harness:
        yield harness


# --- insert ----------------------------------------------------------------


async def test_insert_stores_an_eligible_row(contract: _Harness) -> None:
    rid = await contract.insert(queue="orders", payload=b"a", headers={"k": "v"})
    assert rid is not None
    row = await contract.get_row(rid)
    assert row is not None
    assert (row["queue"], row["payload"], row["timer_id"]) == ("orders", b"a", None)
    msgs = await contract.fetch(["orders"], limit=10, lease_ttl_seconds=60.0)
    assert [(m.id, m.headers) for m in msgs] == [(rid, {"k": "v"})]


async def test_insert_returns_none_on_live_timer_conflict(contract: _Harness) -> None:
    first = await contract.insert(queue="orders", timer_id="t1")
    assert first is not None
    assert await contract.insert(queue="orders", timer_id="t1") is None
    assert await contract.queue_ids("orders") == [first]


async def test_insert_same_timer_id_on_another_queue_lands(contract: _Harness) -> None:
    assert await contract.insert(queue="orders", timer_id="t1") is not None
    assert await contract.insert(queue="audit", timer_id="t1") is not None


async def test_insert_reuses_timer_id_after_delivery(contract: _Harness) -> None:
    first = await contract.insert(queue="orders", timer_id="t1")
    assert first is not None
    [msg] = await contract.fetch(["orders"], limit=10, lease_ttl_seconds=60.0)
    assert msg.acquired_token is not None
    assert await contract.delete(msg.id, msg.acquired_token)
    second = await contract.insert(queue="orders", timer_id="t1")
    assert second is not None
    assert second != first


async def test_insert_activate_in_gates_the_row(contract: _Harness) -> None:
    before = utcnow()
    rid = await contract.insert(queue="orders", activate_in=dt.timedelta(hours=1))
    assert rid is not None
    assert await contract.fetch(["orders"], limit=10, lease_ttl_seconds=60.0) == []
    row = await contract.get_row(rid)
    assert row is not None
    next_at = row["next_attempt_at"]
    assert isinstance(next_at, dt.datetime)
    assert before + dt.timedelta(minutes=59) < next_at < before + dt.timedelta(minutes=61)


async def test_insert_activate_at_stores_that_time(contract: _Harness) -> None:
    at = utcnow() + dt.timedelta(hours=2)
    rid = await contract.insert(queue="orders", activate_at=at)
    assert rid is not None
    row = await contract.get_row(rid)
    assert row is not None
    assert row["next_attempt_at"] == at


async def test_insert_past_activate_at_is_eligible(contract: _Harness) -> None:
    rid = await contract.insert(queue="orders", activate_at=utcnow() - dt.timedelta(minutes=5))
    msgs = await contract.fetch(["orders"], limit=10, lease_ttl_seconds=60.0)
    assert [m.id for m in msgs] == [rid]


async def test_insert_rejects_headers_jsonb_cannot_hold(contract: _Harness) -> None:
    with pytest.raises(contract.rejects):
        await contract.insert(queue="orders", headers={"k": object()})  # ty: ignore[invalid-argument-type]
    assert await contract.queue_ids("orders") == []


async def test_insert_batch_stores_every_row_in_order(contract: _Harness) -> None:
    await contract.insert_batch([(b"a", None), (b"b", {"k": "v"})], queue="orders")
    msgs = await contract.fetch(["orders"], limit=10, lease_ttl_seconds=60.0)
    assert [(m.payload, m.headers) for m in sorted(msgs, key=lambda m: m.id)] == [(b"a", None), (b"b", {"k": "v"})]


async def test_insert_batch_activate_in_gates_every_row(contract: _Harness) -> None:
    await contract.insert_batch([(b"a", None), (b"b", None)], queue="orders", activate_in=dt.timedelta(hours=1))
    assert len(await contract.queue_ids("orders")) == 2
    assert await contract.fetch(["orders"], limit=10, lease_ttl_seconds=60.0) == []


async def test_insert_batch_with_one_rejected_row_stores_nothing(contract: _Harness) -> None:
    rows = [(b"a", None), (b"b", {"k": object()})]
    with pytest.raises(contract.rejects):
        await contract.insert_batch(rows, queue="orders")  # ty: ignore[invalid-argument-type]
    assert await contract.queue_ids("orders") == []


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
    """INVARIANT: an expired lease is reclaimed by the ordinary fetch, in both adapters.

    There is no reaper: the fetch CTE's WHERE is the only thing that recovers an abandoned row, so
    expiry-based redelivery is what makes every "leave it to lease expiry" path in the subscriber
    safe. A fake that skipped leased rows unconditionally would make retry, drain, and shutdown
    tests pass while production quietly stalled on rows nothing ever returns to.
    """
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


async def test_fetch_returns_rows_oldest_first(contract: _Harness) -> None:
    rids = [await contract.seed(queue="orders", next_attempt_offset=-float(i)) for i in range(1, 21)]
    rids += [await contract.seed(queue="audit", next_attempt_offset=-float(i) - 0.5) for i in range(1, 21)]
    rids.append(
        await contract.seed(
            queue="orders", next_attempt_offset=-0.25, leased_token=uuid.uuid4(), acquired_age=120.0, deliveries_count=1
        )
    )
    msgs = await contract.fetch(["orders", "audit"], limit=41, lease_ttl_seconds=60.0)
    assert sorted(m.id for m in msgs) == sorted(rids)
    assert msgs == sorted(msgs, key=lambda m: (m.next_attempt_at, m.id))


async def test_fetch_selects_oldest_under_limit(contract: _Harness) -> None:
    rid_a = await contract.seed(queue="orders", next_attempt_offset=-10.0)
    rid_b = await contract.seed(queue="orders", next_attempt_offset=-20.0)
    rid_c = await contract.seed(queue="orders", next_attempt_offset=-30.0)
    msgs = await contract.fetch(["orders"], limit=2, lease_ttl_seconds=60.0)
    assert {m.id for m in msgs} == {rid_c, rid_b}
    assert rid_a not in {m.id for m in msgs}


async def test_fetch_selects_oldest_across_queues_under_limit(contract: _Harness) -> None:
    rid_a_old = await contract.seed(queue="a", next_attempt_offset=-30.0)
    rid_b_mid = await contract.seed(queue="b", next_attempt_offset=-20.0)
    await contract.seed(queue="a", next_attempt_offset=-10.0)
    await contract.seed(queue="b", next_attempt_offset=-5.0)
    msgs = await contract.fetch(["a", "b"], limit=2, lease_ttl_seconds=60.0)
    assert {m.id for m in msgs} == {rid_a_old, rid_b_mid}


async def test_fetch_selects_oldest_across_pending_and_expired_leases(contract: _Harness) -> None:
    rid_expired = await contract.seed(
        queue="orders", next_attempt_offset=-30.0, leased_token=uuid.uuid4(), acquired_age=120.0, deliveries_count=1
    )
    rid_pending = await contract.seed(queue="orders", next_attempt_offset=-20.0)
    await contract.seed(queue="orders", next_attempt_offset=-10.0)
    msgs = await contract.fetch(["orders"], limit=2, lease_ttl_seconds=60.0)
    assert {m.id for m in msgs} == {rid_expired, rid_pending}


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


async def test_fetch_empty_queues_returns_empty(contract: _Harness) -> None:
    # Both adapters short-circuit an empty queue list before touching storage.
    await contract.seed(queue="orders")
    msgs = await contract.fetch([], limit=10, lease_ttl_seconds=60.0)
    assert msgs == []


# --- delete_with_lease -----------------------------------------------------


async def test_delete_deletes_on_token_match(contract: _Harness) -> None:
    token = uuid.uuid4()
    rid = await contract.seed(leased_token=token, acquired_age=1.0)
    assert await contract.delete(rid, token) is True
    assert await contract.get_row(rid) is None


async def test_delete_noop_on_token_mismatch(contract: _Harness) -> None:
    """INVARIANT: a terminal delete whose token no longer matches is a no-op, in both adapters.

    This is the lease-token invariant itself: a slow handler whose lease expired and was reclaimed
    must not clobber the new holder's row. It is asserted against both substrates because the rule
    exists twice -- as SQL in ``OutboxClient`` and as Python in ``FakeOutboxClient`` -- and a fake
    that quietly deleted regardless would make every test built on the test broker agree with a
    production path that does not.
    """
    rid = await contract.seed(leased_token=uuid.uuid4(), acquired_age=1.0)
    assert await contract.delete(rid, uuid.uuid4()) is False
    assert await contract.get_row(rid) is not None


async def test_delete_noop_on_unleased_row(contract: _Harness) -> None:
    rid = await contract.seed()
    assert await contract.delete(rid, uuid.uuid4()) is False
    assert await contract.get_row(rid) is not None


async def test_delete_with_dlq_payload_but_no_dlq_table_refuses(contract_without_dlq: _Harness) -> None:
    token = uuid.uuid4()
    rid = await contract_without_dlq.seed(queue="orders", leased_token=token, acquired_age=1.0, deliveries_count=1)
    with pytest.raises(RuntimeError, match="no dlq_table"):
        await contract_without_dlq.delete(rid, token, dlq_payload={"failure_reason": "boom", "last_exception": None})
    assert await contract_without_dlq.get_row(rid) is not None
    assert await contract_without_dlq.dlq_rows() == []


async def test_delete_with_dlq_materializes_audit_row(contract: _Harness) -> None:
    """INVARIANT: the DLQ archive is written atomically with the delete, in both adapters.

    The real client does this as one CTE so an INSERT failure rolls the DELETE back and the row is
    reclaimed on lease expiry -- misconfiguration surfaces as outbox growth and ``lease_lost``
    spikes, never as silent audit loss. Splitting it into two statements, or letting the fake build
    its audit dict from a hand-written column list rather than the shared projection, breaks that
    guarantee on one substrate while the other still passes.
    """
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


# --- delete_batch_with_lease -----------------------------------------------


async def test_delete_batch_deletes_only_token_matches(contract: _Harness) -> None:
    a = await contract.seed(queue="q")
    b = await contract.seed(queue="q")
    c = await contract.seed(queue="q")
    leased = await contract.fetch(["q"], limit=10, lease_ttl_seconds=60.0)
    tokens: dict[int, uuid.UUID] = {}
    for row in leased:
        assert row.acquired_token is not None
        tokens[row.id] = row.acquired_token
    # a and b with correct tokens; c with a wrong token -> only a and b delete.
    wrong = uuid.uuid4()
    deleted = await contract.delete_batch([(a, tokens[a]), (b, tokens[b]), (c, wrong)])
    assert deleted == {(a, tokens[a]), (b, tokens[b])}
    assert await contract.get_row(a) is None
    assert await contract.get_row(b) is None
    assert await contract.get_row(c) is not None


async def test_delete_batch_returns_only_the_pair_holding_the_lease(contract: _Harness) -> None:
    """INVARIANT: the batched delete reports deleted rows as ``(id, token)`` pairs, in both adapters.

    One flush can carry a stale and a live copy of the same id. Returning bare ids would let the
    caller ack the stale copy too, because its id matches a row that really was deleted.
    """
    a = await contract.seed(queue="q")
    [leased] = await contract.fetch(["q"], limit=10, lease_ttl_seconds=60.0)
    assert leased.acquired_token is not None
    deleted = await contract.delete_batch([(a, uuid.uuid4()), (a, leased.acquired_token)])
    assert deleted == {(a, leased.acquired_token)}
    assert await contract.get_row(a) is None


async def test_delete_batch_empty_is_noop(contract: _Harness) -> None:
    assert await contract.delete_batch([]) == set()


async def test_delete_batch_ignores_unleased_row(contract: _Harness) -> None:
    a = await contract.seed(queue="q")  # never fetched -> acquired_token is None
    deleted = await contract.delete_batch([(a, uuid.uuid4())])
    assert deleted == set()
    assert await contract.get_row(a) is not None


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
    assert isinstance(next_attempt_at, dt.datetime)
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


async def test_mark_pending_noop_on_unleased_row(contract: _Harness) -> None:
    # The NULL = NULL token guard on the retry path: an unleased row (acquired_token
    # None) is never reschedulable by any claim token. Parallels the delete twin.
    now = utcnow()
    rid = await contract.seed()  # never leased -> acquired_token is None
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
    assert row["acquired_token"] is None
    assert row["attempts_count"] == 0
