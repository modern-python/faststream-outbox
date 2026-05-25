import asyncio
import datetime as _dt
import json
import logging
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import faststream.asgi.factories.asyncapi.try_it_out
import pytest
from faststream.exceptions import IncorrectState
from pydantic import BaseModel
from sqlalchemy import MetaData
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncSession

from faststream_outbox import (
    ConstantRetry,
    ExponentialRetry,
    LinearRetry,
    NoRetry,
    OutboxBroker,
    OutboxRouter,
    TestOutboxBroker,
    make_outbox_table,
)
from faststream_outbox.client import OutboxClient, _validate_schema_sync
from faststream_outbox.envelope import _encode_payload
from faststream_outbox.message import OutboxInnerMessage, OutboxMessage
from faststream_outbox.parser.parser import OutboxParser
from faststream_outbox.subscriber.usecase import OutboxSubscriber, _compute_backoff
from faststream_outbox.testing import FakeOutboxClient


def test_outbox_broker_registered_in_try_it_out_registry() -> None:
    registry = faststream.asgi.factories.asyncapi.try_it_out._get_broker_registry()  # noqa: SLF001
    assert registry[OutboxBroker] is TestOutboxBroker  # ty: ignore[invalid-argument-type]


def _make_broker(engine: object | None = None, table_name: str = "outbox") -> OutboxBroker:
    metadata = MetaData()
    table = make_outbox_table(metadata, table_name=table_name)
    if engine is not None:
        return OutboxBroker(engine, outbox_table=table)  # ty: ignore[invalid-argument-type]
    return OutboxBroker(outbox_table=table)


def _make_session_mock(*, scalar_return: object = 42) -> AsyncMock:
    """
    Build an AsyncSession mock whose ``execute()`` returns a sync MagicMock.

    AsyncMock(spec=AsyncSession) makes the return_value of execute() default to an
    AsyncMock — so ``result.scalar()`` would itself return a coroutine. The broker's
    real CursorResult.scalar() is sync, so override the return_value to a MagicMock.
    """
    session = AsyncMock(spec=AsyncSession)
    session.execute.return_value = MagicMock()
    session.execute.return_value.scalar.return_value = scalar_return
    return session


# --- make_outbox_table ---


def test_make_outbox_table_columns_present() -> None:
    metadata = MetaData()
    t = make_outbox_table(metadata, table_name="my_outbox")
    expected = {
        "id",
        "queue",
        "payload",
        "headers",
        "attempts_count",
        "deliveries_count",
        "created_at",
        "next_attempt_at",
        "first_attempt_at",
        "last_attempt_at",
        "acquired_at",
        "acquired_token",
        "timer_id",
    }
    assert {c.name for c in t.columns} == expected
    assert t.name == "my_outbox"


def test_make_outbox_table_declares_timer_unique_index() -> None:
    metadata = MetaData()
    t = make_outbox_table(metadata, table_name="my_outbox")
    timer_idx = next(idx for idx in t.indexes if idx.name == "my_outbox_timer_id_uq")
    assert timer_idx.unique is True
    assert [c.name for c in timer_idx.columns] == ["queue", "timer_id"]
    # Partial-index predicate ensures non-timer rows aren't constrained
    assert timer_idx.dialect_options["postgresql"]["where"] is not None


def test_make_outbox_table_declares_lease_idx() -> None:
    metadata = MetaData()
    t = make_outbox_table(metadata, table_name="my_outbox")
    lease_idx = next(idx for idx in t.indexes if idx.name == "my_outbox_lease_idx")
    assert lease_idx.unique is False
    assert [c.name for c in lease_idx.columns] == ["queue", "acquired_at"]
    # Partial-index predicate `acquired_token IS NOT NULL` — the fetch CTE's
    # Branch B (expired-lease reclaim) relies on this; without it, the OR
    # disjunct degrades to seq-scan even with `_pending_idx` covering Branch A.
    assert lease_idx.dialect_options["postgresql"]["where"] is not None


def test_make_outbox_table_attaches_to_metadata() -> None:
    metadata = MetaData()
    t = make_outbox_table(metadata, table_name="outbox")
    assert "outbox" in metadata.tables
    assert metadata.tables["outbox"] is t


def test_make_outbox_table_accepts_max_length_name() -> None:
    # 56 ASCII bytes + "outbox_" (7) = 63 — exactly the Postgres limit.
    metadata = MetaData()
    name = "a" * 56
    t = make_outbox_table(metadata, table_name=name)
    assert t.name == name


def test_make_outbox_table_rejects_oversize_name() -> None:
    metadata = MetaData()
    with pytest.raises(ValueError, match="63 bytes"):
        make_outbox_table(metadata, table_name="a" * 57)


def test_make_outbox_table_rejects_oversize_multibyte_name() -> None:
    # 30x "é" = 60 UTF-8 bytes (each "é" is 2 bytes), char count is 30 — well under
    # any naive char-based check, but channel byte length is 7 + 60 = 67 > 63.
    metadata = MetaData()
    with pytest.raises(ValueError, match="63 bytes"):
        make_outbox_table(metadata, table_name="é" * 30)


# --- _encode_payload ---


def test_encode_payload_dict_sets_content_type_and_correlation() -> None:
    payload, headers = _encode_payload({"order_id": 1})
    assert payload == b'{"order_id": 1}'
    assert headers["content-type"] == "application/json"
    assert headers["correlation_id"]


def test_encode_payload_preserves_user_correlation_id() -> None:
    _, headers = _encode_payload({"x": 1}, correlation_id="trace-abc")
    assert headers["correlation_id"] == "trace-abc"


def test_encode_payload_passes_through_bytes() -> None:
    payload, headers = _encode_payload(b"raw bytes here")
    assert payload == b"raw bytes here"
    # No content-type for raw bytes
    assert "content-type" not in headers
    assert headers["correlation_id"]


def test_encode_payload_merges_user_headers() -> None:
    _, headers = _encode_payload({"x": 1}, headers={"x-tenant": "acme"})
    assert headers["x-tenant"] == "acme"
    assert headers["content-type"] == "application/json"


def test_encode_payload_raises_when_user_content_type_conflicts() -> None:
    """User-supplied content-type that mismatches the encoder is a foot-gun (L1)."""
    with pytest.raises(ValueError, match="content-type"):
        _encode_payload({"x": 1}, headers={"content-type": "text/plain"})


def test_encode_payload_accepts_user_content_type_when_it_matches() -> None:
    """Idempotent re-publish with the encoder's own content-type is allowed."""
    _, headers = _encode_payload({"x": 1}, headers={"content-type": "application/json"})
    assert headers["content-type"] == "application/json"


def test_encode_payload_allows_user_content_type_for_bytes_body() -> None:
    """Plain bytes have no encoder content-type, so the user's label wins with no conflict."""
    payload, headers = _encode_payload(b"raw", headers={"content-type": "application/octet-stream"})
    assert payload == b"raw"
    assert headers["content-type"] == "application/octet-stream"


class _PydanticBody(BaseModel):
    order_id: int
    name: str


def test_encode_payload_serializes_pydantic_model_with_default_serializer() -> None:
    """Default broker resolves PydanticSerializer so BaseModel encodes as JSON."""
    broker = _make_broker()
    serializer = broker.config.broker_config.fd_config._serializer  # noqa: SLF001
    body = _PydanticBody(order_id=1, name="x")
    payload, headers = _encode_payload(body, serializer=serializer)
    assert json.loads(payload) == body.model_dump()
    assert headers["content-type"] == "application/json"


# --- retry strategies ---


def _make_times() -> tuple[_dt.datetime, _dt.datetime]:
    first = _dt.datetime(2026, 1, 1, tzinfo=_dt.UTC)
    last = first + _dt.timedelta(seconds=10)
    return first, last


def test_no_retry_always_terminal() -> None:
    first, last = _make_times()
    assert (
        NoRetry().get_next_attempt_delay(
            first_attempt_at=first,
            last_attempt_at=last,
            attempts_count=1,
        )
        is None
    )


def test_constant_retry_returns_delay() -> None:
    first, last = _make_times()
    delay = ConstantRetry(delay_seconds=30).get_next_attempt_delay(
        first_attempt_at=first,
        last_attempt_at=last,
        attempts_count=1,
    )
    assert delay == 30.0


def test_constant_retry_max_attempts_reached() -> None:
    first, last = _make_times()
    s = ConstantRetry(delay_seconds=1, max_attempts=3)
    assert s.get_next_attempt_delay(first_attempt_at=first, last_attempt_at=last, attempts_count=3) is None
    assert s.get_next_attempt_delay(first_attempt_at=first, last_attempt_at=last, attempts_count=2) is not None


def test_constant_retry_max_total_delay_exceeded() -> None:
    first, last = _make_times()
    s = ConstantRetry(delay_seconds=100, max_total_delay_seconds=50)
    assert s.get_next_attempt_delay(first_attempt_at=first, last_attempt_at=last, attempts_count=1) is None


def test_linear_retry_grows_with_attempts() -> None:
    first, last = _make_times()
    s = LinearRetry(initial_delay_seconds=10, step_seconds=5)
    d1 = s.get_next_attempt_delay(first_attempt_at=first, last_attempt_at=last, attempts_count=1)
    d2 = s.get_next_attempt_delay(first_attempt_at=first, last_attempt_at=last, attempts_count=3)
    assert d1 is not None
    assert d2 is not None
    assert d2 > d1


def test_exponential_retry_caps_at_max_delay() -> None:
    first, last = _make_times()
    s = ExponentialRetry(initial_delay_seconds=1, multiplier=2, max_delay_seconds=10)
    delay = s.get_next_attempt_delay(first_attempt_at=first, last_attempt_at=last, attempts_count=10)
    assert delay == 10.0


def test_exponential_retry_with_jitter_within_bounds() -> None:
    # Symmetric jitter ±jitter_factor/2 of the base delay.
    first, last = _make_times()
    s = ExponentialRetry(initial_delay_seconds=10, multiplier=1.0, jitter_factor=0.5)
    delay = s.get_next_attempt_delay(first_attempt_at=first, last_attempt_at=last, attempts_count=1)
    assert delay is not None
    assert 7.5 <= delay <= 12.5


def test_exponential_retry_jitter_respects_max_delay() -> None:
    # Jitter must be applied before the clamp — otherwise max_delay_seconds is leaky.
    first, last = _make_times()
    s = ExponentialRetry(
        initial_delay_seconds=100,
        multiplier=1.0,
        max_delay_seconds=10.0,
        jitter_factor=0.5,
    )
    for _ in range(200):
        delay = s.get_next_attempt_delay(first_attempt_at=first, last_attempt_at=last, attempts_count=1)
        assert delay is not None
        assert delay <= 10.0


# --- OutboxInnerMessage state machine ---


def _make_msg(**overrides: object) -> OutboxInnerMessage:
    base: dict = {
        "id": 1,
        "queue": "q",
        "payload": b"p",
        "headers": None,
        "attempts_count": 0,
        "deliveries_count": 1,
        "created_at": _dt.datetime.now(tz=_dt.UTC),
        "next_attempt_at": _dt.datetime.now(tz=_dt.UTC),
        "first_attempt_at": None,
        "last_attempt_at": None,
        "acquired_at": _dt.datetime.now(tz=_dt.UTC),
        "acquired_token": uuid.uuid4(),
    }
    base.update(overrides)
    return OutboxInnerMessage(**base)


async def test_inner_message_ack_marks_for_delete() -> None:
    msg = _make_msg()
    await msg.ack()
    assert msg.to_delete
    assert msg.state_set


async def test_inner_message_nack_with_no_strategy_is_terminal() -> None:
    msg = _make_msg()
    await msg.nack()
    assert msg.to_delete


async def test_inner_message_nack_with_strategy_schedules_retry() -> None:
    msg = _make_msg(retry_strategy=ConstantRetry(delay_seconds=60))
    await msg.nack()
    assert not msg.to_delete
    assert msg.last_attempt_at is not None
    assert msg.pending_delay_seconds == 60.0


async def test_inner_message_reject_is_terminal() -> None:
    msg = _make_msg()
    await msg.reject()
    assert msg.to_delete


async def test_inner_message_double_ack_is_noop() -> None:
    msg = _make_msg()
    await msg.ack()
    initial = msg.attempts_count
    await msg.ack()  # second call must not double-record
    assert msg.attempts_count == initial


def test_allow_delivery_under_cap() -> None:
    msg = _make_msg(deliveries_count=3)
    assert msg.allow_delivery(max_deliveries=5, logger=None) is True
    assert not msg.to_delete


def test_allow_delivery_exceeds_cap_marks_for_delete() -> None:
    msg = _make_msg(deliveries_count=10)
    assert msg.allow_delivery(max_deliveries=5, logger=None) is False
    assert msg.to_delete


def test_allow_delivery_no_cap_is_always_true() -> None:
    msg = _make_msg(deliveries_count=10_000)
    assert msg.allow_delivery(max_deliveries=None, logger=None) is True


async def test_assert_state_set_rejects_when_not_set() -> None:
    msg = _make_msg()
    await msg.assert_state_set(logger=None)
    assert msg.state_set
    assert msg.to_delete  # reject path → terminal


# --- parser ---


async def test_parser_round_trip() -> None:
    msg = _make_msg(
        payload=b'{"x": 1}',
        headers={"content-type": "application/json", "correlation_id": "trace-1"},
    )
    parser = OutboxParser()
    stream_msg = await parser.parse_message(msg)
    assert isinstance(stream_msg, OutboxMessage)
    assert stream_msg.correlation_id == "trace-1"
    decoded = await parser.decode_message(stream_msg)
    assert decoded == {"x": 1}


# --- broker construction ---


def test_broker_constructs_without_engine() -> None:
    broker = _make_broker()
    assert broker.config.broker_config.client is None


def test_broker_with_engine_has_client() -> None:
    engine = AsyncMock()
    broker = _make_broker(engine)
    assert broker.config.broker_config.client is not None


async def test_broker_publish_rejects_non_async_session() -> None:
    broker = _make_broker()
    with pytest.raises(TypeError, match="AsyncSession"):
        await broker.publish(b"x", queue="orders", session=object())  # ty: ignore[invalid-argument-type]


async def test_broker_publish_executes_insert_then_pg_notify_on_session() -> None:
    broker = _make_broker()
    session = _make_session_mock()
    await broker.publish({"order_id": 1}, queue="orders", session=session)
    # Two execute calls: the INSERT, then SELECT pg_notify(...).
    assert session.execute.await_count == 2
    insert_stmt = session.execute.await_args_list[0].args[0]
    assert "INSERT INTO" in str(insert_stmt)
    params = insert_stmt.compile().params
    assert params["queue"] == "orders"
    assert json.loads(params["payload"]) == {"order_id": 1}
    assert params["headers"]["content-type"] == "application/json"
    notify_stmt, notify_params = session.execute.await_args_list[1].args
    assert "pg_notify" in str(notify_stmt)
    assert notify_params["channel"] == "outbox_outbox"
    assert notify_params["payload"] == "orders"


async def test_broker_publish_encodes_pydantic_model() -> None:
    broker = _make_broker()
    session = _make_session_mock()
    body = _PydanticBody(order_id=7, name="alpha")
    await broker.publish(body, queue="orders", session=session)
    insert_stmt = session.execute.await_args_list[0].args[0]
    params = insert_stmt.compile().params
    assert json.loads(params["payload"]) == body.model_dump()
    assert params["headers"]["content-type"] == "application/json"


async def test_broker_publish_batch_encodes_pydantic_models() -> None:
    broker = _make_broker()
    session = _make_session_mock()
    bodies = [_PydanticBody(order_id=1, name="a"), _PydanticBody(order_id=2, name="b")]
    await broker.publish_batch(*bodies, queue="orders", session=session)
    # First execute is the INSERT (executemany), second is pg_notify.
    insert_call = session.execute.await_args_list[0]
    rows = insert_call.args[1]
    assert [json.loads(row["payload"]) for row in rows] == [b.model_dump() for b in bodies]
    for row in rows:
        assert row["headers"]["content-type"] == "application/json"


async def test_broker_publish_does_not_commit() -> None:
    broker = _make_broker()
    session = _make_session_mock()
    await broker.publish(b"x", queue="orders", session=session)
    session.commit.assert_not_called()
    session.flush.assert_not_called()


async def test_broker_request_raises() -> None:
    broker = _make_broker()
    with pytest.raises(NotImplementedError):
        await broker.request(b"x")


async def test_broker_ping_no_client_returns_false() -> None:
    broker = _make_broker()
    assert await broker.ping() is False


async def test_broker_ping_when_engine_query_fails() -> None:
    engine = AsyncMock()
    engine.connect.return_value.__aenter__.side_effect = ConnectionError("nope")
    broker = _make_broker(engine)
    assert await broker.ping() is False


# --- registrator validation ---


def test_subscriber_empty_queue_list_raises() -> None:
    broker = _make_broker()
    with pytest.raises(ValueError, match="at least one queue"):
        broker.subscriber([])


def test_publisher_raises_not_implemented() -> None:
    broker = _make_broker()
    with pytest.raises(NotImplementedError, match="no publisher"):
        broker.publisher("orders")


def test_duplicate_subscriber_warns() -> None:
    broker = _make_broker()

    @broker.subscriber("orders")
    async def first(body: dict) -> None: ...

    with pytest.warns(UserWarning, match="Duplicate subscriber"):

        @broker.subscriber("orders")
        async def second(body: dict) -> None: ...


def test_router_can_be_constructed() -> None:
    router = OutboxRouter()
    assert router is not None


# --- broker error paths and _NoProducer stubs ---


async def test_broker_publish_batch_rejects_non_async_session() -> None:
    broker = _make_broker()
    with pytest.raises(TypeError, match="AsyncSession"):
        await broker.publish_batch(b"x", queue="orders", session=object())  # ty: ignore[invalid-argument-type]


async def test_broker_publish_batch_no_bodies_is_noop() -> None:
    from sqlalchemy.ext.asyncio import AsyncSession  # noqa: PLC0415

    broker = _make_broker()
    session = AsyncMock(spec=AsyncSession)
    await broker.publish_batch(queue="orders", session=session)
    session.execute.assert_not_called()


async def test_broker_publish_rejects_activate_in_and_at_together() -> None:
    broker = _make_broker()
    session = _make_session_mock()
    with pytest.raises(ValueError, match="activate_in / activate_at"):
        await broker.publish(
            b"x",
            queue="orders",
            session=session,
            activate_in=_dt.timedelta(seconds=1),
            activate_at=_dt.datetime.now(tz=_dt.UTC) + _dt.timedelta(seconds=1),
        )


async def test_broker_publish_with_activate_in_skips_notify() -> None:
    broker = _make_broker()
    session = _make_session_mock()
    await broker.publish(b"x", queue="orders", session=session, activate_in=_dt.timedelta(seconds=30))
    # Only the INSERT — no NOTIFY for future-dated rows.
    assert session.execute.await_count == 1
    insert_stmt = session.execute.await_args_list[0].args[0]
    assert "INSERT INTO" in str(insert_stmt)
    assert "next_attempt_at" in str(insert_stmt)


async def test_broker_publish_with_activate_at_skips_notify() -> None:
    broker = _make_broker()
    session = _make_session_mock()
    fire = _dt.datetime.now(tz=_dt.UTC) + _dt.timedelta(minutes=5)
    await broker.publish(b"x", queue="orders", session=session, activate_at=fire)
    assert session.execute.await_count == 1
    params = session.execute.await_args_list[0].args[0].compile().params
    assert params["next_attempt_at"] == fire


async def test_broker_publish_emits_notify_when_activate_at_is_past() -> None:
    # Past activate_at means the row is immediately eligible — NOTIFY must fire so
    # listeners wake without waiting for the next poll tick.
    broker = _make_broker()
    session = _make_session_mock()
    past = _dt.datetime.now(tz=_dt.UTC) - _dt.timedelta(seconds=5)
    await broker.publish(b"x", queue="orders", session=session, activate_at=past)
    assert session.execute.await_count == 2
    notify_stmt, notify_params = session.execute.await_args_list[1].args
    assert "pg_notify" in str(notify_stmt)
    assert notify_params["payload"] == "orders"


async def test_broker_publish_emits_notify_when_activate_in_is_zero() -> None:
    broker = _make_broker()
    session = _make_session_mock()
    await broker.publish(b"x", queue="orders", session=session, activate_in=_dt.timedelta(0))
    assert session.execute.await_count == 2
    notify_stmt, _params = session.execute.await_args_list[1].args
    assert "pg_notify" in str(notify_stmt)


async def test_broker_publish_rejects_naive_activate_at() -> None:
    broker = _make_broker()
    session = _make_session_mock()
    naive = _dt.datetime(2026, 5, 23, 12, 0, 0)  # noqa: DTZ001
    with pytest.raises(ValueError, match="timezone-aware"):
        await broker.publish(b"x", queue="orders", session=session, activate_at=naive)


async def test_broker_publish_with_timer_id_uses_on_conflict() -> None:
    broker = _make_broker()
    session = _make_session_mock()
    await broker.publish(b"x", queue="orders", session=session, timer_id="email-123")
    insert_stmt = session.execute.await_args_list[0].args[0]
    compiled = insert_stmt.compile(dialect=postgresql.dialect())
    sql = str(compiled)
    assert "INSERT INTO" in sql
    assert "ON CONFLICT" in sql
    assert "DO NOTHING" in sql
    assert compiled.params["timer_id"] == "email-123"


async def test_broker_publish_returns_none_on_timer_id_conflict() -> None:
    broker = _make_broker()
    # Simulate ON CONFLICT DO NOTHING returning no rows: scalar() → None
    session = _make_session_mock(scalar_return=None)
    result = await broker.publish(b"x", queue="orders", session=session, timer_id="dup")
    assert result is None
    # NOTIFY skipped when nothing was inserted.
    assert session.execute.await_count == 1


async def test_broker_publish_batch_rejects_activate_in_and_at_together() -> None:
    broker = _make_broker()
    session = AsyncMock(spec=AsyncSession)
    with pytest.raises(ValueError, match="activate_in / activate_at"):
        await broker.publish_batch(
            b"a",
            queue="orders",
            session=session,
            activate_in=_dt.timedelta(seconds=1),
            activate_at=_dt.datetime.now(tz=_dt.UTC) + _dt.timedelta(seconds=1),
        )


async def test_broker_publish_batch_with_activate_in_skips_notify() -> None:
    broker = _make_broker()
    session = AsyncMock(spec=AsyncSession)
    await broker.publish_batch(
        b"a",
        b"b",
        queue="orders",
        session=session,
        activate_in=_dt.timedelta(seconds=30),
    )
    # Insert only — no NOTIFY for future-dated batch.
    assert session.execute.await_count == 1
    rows = session.execute.await_args_list[0].args[1]
    assert all("next_attempt_at" in r for r in rows)


async def test_broker_publish_batch_with_activate_at_skips_notify() -> None:
    broker = _make_broker()
    session = AsyncMock(spec=AsyncSession)
    fire = _dt.datetime.now(tz=_dt.UTC) + _dt.timedelta(minutes=5)
    await broker.publish_batch(b"a", b"b", queue="orders", session=session, activate_at=fire)
    # No NOTIFY: future-dated rows.
    assert session.execute.await_count == 1
    rows = session.execute.await_args_list[0].args[1]
    assert all(r["next_attempt_at"] == fire for r in rows)


async def test_broker_publish_batch_emits_notify_when_activate_at_is_past() -> None:
    broker = _make_broker()
    session = AsyncMock(spec=AsyncSession)
    past = _dt.datetime.now(tz=_dt.UTC) - _dt.timedelta(seconds=5)
    await broker.publish_batch(b"a", b"b", queue="orders", session=session, activate_at=past)
    # INSERT + NOTIFY: past activate_at is immediately eligible.
    assert session.execute.await_count == 2
    notify_stmt, notify_params = session.execute.await_args_list[1].args
    assert "pg_notify" in str(notify_stmt)
    assert notify_params["payload"] == "orders"


async def test_broker_publish_batch_rejects_naive_activate_at() -> None:
    broker = _make_broker()
    session = AsyncMock(spec=AsyncSession)
    naive = _dt.datetime(2026, 5, 23, 12, 0, 0)  # noqa: DTZ001
    with pytest.raises(ValueError, match="timezone-aware"):
        await broker.publish_batch(b"a", queue="orders", session=session, activate_at=naive)


async def test_broker_publish_batch_does_not_accept_timer_id() -> None:
    """publish_batch must not expose per-row dedup; timer_id is a publish()-only kwarg."""
    broker = _make_broker()
    session = AsyncMock(spec=AsyncSession)
    with pytest.raises(TypeError, match="timer_id"):
        await broker.publish_batch(
            b"a",
            queue="orders",
            session=session,
            timer_id="x",  # ty: ignore[unknown-argument]
        )


async def test_broker_cancel_timer_rejects_non_async_session() -> None:
    broker = _make_broker()
    with pytest.raises(TypeError, match="AsyncSession"):
        await broker.cancel_timer(queue="orders", timer_id="x", session=object())  # ty: ignore[invalid-argument-type]


async def test_broker_cancel_timer_emits_delete_with_lease_guard() -> None:
    broker = _make_broker()
    session = AsyncMock(spec=AsyncSession)
    session.execute.return_value.rowcount = 1
    deleted = await broker.cancel_timer(queue="orders", timer_id="email-1", session=session)
    assert deleted is True
    delete_stmt = session.execute.await_args_list[0].args[0]
    sql = str(delete_stmt)
    assert "DELETE" in sql
    assert "acquired_token IS NULL" in sql
    params = delete_stmt.compile().params
    assert params["queue_1"] == "orders"
    assert params["timer_id_1"] == "email-1"


async def test_broker_fetch_unprocessed_rejects_non_async_session() -> None:
    broker = _make_broker()
    with pytest.raises(TypeError, match="AsyncSession"):
        await broker.fetch_unprocessed(session=object())  # ty: ignore[invalid-argument-type]


def _fetch_unprocessed_session_mock() -> AsyncMock:
    """AsyncSession mock whose ``execute().mappings().all()`` returns an empty list."""
    session = AsyncMock(spec=AsyncSession)
    result = MagicMock()
    result.mappings.return_value.all.return_value = []
    session.execute.return_value = result
    return session


async def test_broker_fetch_unprocessed_builds_select_all_columns() -> None:
    broker = _make_broker()
    session = _fetch_unprocessed_session_mock()
    rows = await broker.fetch_unprocessed(session=session)
    assert rows == []
    stmt = session.execute.await_args_list[0].args[0]
    sql = str(stmt)
    assert "SELECT" in sql
    assert "FROM outbox" in sql
    assert "ORDER BY outbox.id" in sql
    # No queue filter compiled when queue=None
    assert "WHERE" not in sql


async def test_broker_fetch_unprocessed_filters_by_queue() -> None:
    broker = _make_broker()
    session = _fetch_unprocessed_session_mock()
    await broker.fetch_unprocessed(session=session, queue="orders")
    stmt = session.execute.await_args_list[0].args[0]
    sql = str(stmt)
    assert "WHERE outbox.queue =" in sql
    params = stmt.compile().params
    assert params["queue_1"] == "orders"


async def test_broker_fetch_unprocessed_applies_default_limit() -> None:
    # Guardrail against accidental SELECT * with no LIMIT against a backlogged table.
    broker = _make_broker()
    session = _fetch_unprocessed_session_mock()
    await broker.fetch_unprocessed(session=session)
    stmt = session.execute.await_args_list[0].args[0]
    assert stmt.compile().params["param_1"] == 1000


async def test_broker_fetch_unprocessed_respects_explicit_limit() -> None:
    broker = _make_broker()
    session = _fetch_unprocessed_session_mock()
    await broker.fetch_unprocessed(session=session, limit=5)
    stmt = session.execute.await_args_list[0].args[0]
    assert stmt.compile().params["param_1"] == 5


async def test_broker_cancel_timer_returns_false_when_nothing_deleted() -> None:
    broker = _make_broker()
    session = AsyncMock(spec=AsyncSession)
    session.execute.return_value.rowcount = 0
    deleted = await broker.cancel_timer(queue="orders", timer_id="x", session=session)
    assert deleted is False


async def test_broker_publish_batch_executes_single_insert_for_many_rows() -> None:
    from sqlalchemy.ext.asyncio import AsyncSession  # noqa: PLC0415

    broker = _make_broker()
    session = AsyncMock(spec=AsyncSession)
    await broker.publish_batch(b"a", b"b", b"c", queue="orders", session=session)
    # Two execute calls: the multi-row INSERT, then SELECT pg_notify(...).
    assert session.execute.await_count == 2
    rows = session.execute.await_args_list[0].args[1]
    assert len(rows) == 3
    assert all(r["queue"] == "orders" for r in rows)
    assert {r["payload"] for r in rows} == {b"a", b"b", b"c"}
    notify_stmt, notify_params = session.execute.await_args_list[1].args
    assert "pg_notify" in str(notify_stmt)
    assert notify_params["payload"] == "orders"


async def test_no_producer_methods_raise() -> None:
    from faststream_outbox.broker import _NoProducer  # noqa: PLC0415

    producer = _NoProducer()
    with pytest.raises(NotImplementedError):
        await producer.publish()
    with pytest.raises(NotImplementedError):
        await producer.request()
    with pytest.raises(NotImplementedError):
        await producer.publish_batch()


def test_no_producer_connect_disconnect_noop() -> None:
    from faststream_outbox.broker import _NoProducer  # noqa: PLC0415

    producer = _NoProducer()
    producer.connect()  # must not raise
    producer.disconnect()  # must not raise


def test_broker_client_property_raises_without_engine() -> None:
    metadata = MetaData()
    t = make_outbox_table(metadata)
    broker = OutboxBroker(outbox_table=t)
    with pytest.raises(RuntimeError, match="not connected"):
        _ = broker.client


async def test_broker_validate_schema_delegates_to_client() -> None:
    metadata = MetaData()
    t = make_outbox_table(metadata)
    engine = AsyncMock()
    broker = OutboxBroker(engine, outbox_table=t)
    broker.config.broker_config.client = AsyncMock()
    await broker.validate_schema()
    broker.config.broker_config.client.validate_schema.assert_awaited_once()  # type: ignore[union-attr]


def test_validate_schema_sync_raises_when_alembic_missing() -> None:
    """Without alembic installed the validator must raise ImportError with the install hint."""
    metadata = MetaData()
    t = make_outbox_table(metadata)
    # Alembic is imported at module load; simulate "not installed" by zeroing the
    # sentinels the function checks — matches what client.py's except-ImportError does.
    with (
        patch.multiple(
            "faststream_outbox.client",
            _alembic_compare_metadata=None,
            _AlembicMigrationContext=None,
        ),
        pytest.raises(ImportError, match=r"pip install faststream-outbox\[validate\]"),
    ):
        _validate_schema_sync(MagicMock(), t)


async def test_broker_ping_done_subscriber_task_is_false() -> None:
    from unittest.mock import MagicMock  # noqa: PLC0415

    metadata = MetaData()
    t = make_outbox_table(metadata)
    engine = AsyncMock()
    broker = OutboxBroker(engine, outbox_table=t)

    @broker.subscriber("orders")
    async def handle(body: dict) -> None: ...

    # Force client.ping() True so the for-loop over subscribers is reached.
    broker.config.broker_config.client.ping = AsyncMock(return_value=True)  # type: ignore[union-attr]
    sub = next(iter(broker._subscribers))  # noqa: SLF001
    done_task = MagicMock()
    done_task.done.return_value = True
    sub.tasks = [done_task]
    assert await broker.ping() is False


async def test_broker_ping_live_subscriber_task_is_true() -> None:
    from unittest.mock import MagicMock  # noqa: PLC0415

    metadata = MetaData()
    t = make_outbox_table(metadata)
    engine = AsyncMock()
    broker = OutboxBroker(engine, outbox_table=t)

    @broker.subscriber("orders")
    async def handle(body: dict) -> None: ...

    broker.config.broker_config.client.ping = AsyncMock(return_value=True)  # type: ignore[union-attr]
    sub = next(iter(broker._subscribers))  # noqa: SLF001
    live_task = MagicMock()
    live_task.done.return_value = False
    sub.tasks = [live_task]
    assert await broker.ping() is True


def test_outbox_params_storage_caches_logger() -> None:
    from unittest.mock import MagicMock  # noqa: PLC0415

    from faststream_outbox.broker import OutboxParamsStorage  # noqa: PLC0415

    storage = OutboxParamsStorage()
    context = MagicMock()
    a = storage.get_logger(context=context)
    b = storage.get_logger(context=context)  # cache hit on L46
    assert a is b


# --- configs ---


async def test_broker_connect_raises_without_engine() -> None:
    metadata = MetaData()
    t = make_outbox_table(metadata)
    broker = OutboxBroker(outbox_table=t)
    with pytest.raises(IncorrectState, match="Engine not available"):
        await broker._connect()  # noqa: SLF001


# --- client ---


def test_client_table_property() -> None:

    metadata = MetaData()
    t = make_outbox_table(metadata)
    client = OutboxClient(AsyncMock(), t)
    assert client.table is t


async def test_client_fetch_empty_queues_returns_empty() -> None:

    metadata = MetaData()
    t = make_outbox_table(metadata)
    client = OutboxClient(AsyncMock(), t)
    assert await client.fetch(AsyncMock(), [], limit=10, lease_ttl_seconds=60.0) == []


# --- Liskov-widening guards: real client must reject None conn (Protocol allows it for the fake) ---


async def test_client_fetch_raises_typeerror_on_none_conn() -> None:
    metadata = MetaData()
    t = make_outbox_table(metadata)
    client = OutboxClient(AsyncMock(), t)
    with pytest.raises(TypeError, match=r"OutboxClient\.fetch requires a live AsyncConnection"):
        await client.fetch(None, ["orders"], limit=1, lease_ttl_seconds=60.0)


async def test_client_delete_with_lease_raises_typeerror_on_none_conn() -> None:
    metadata = MetaData()
    t = make_outbox_table(metadata)
    client = OutboxClient(AsyncMock(), t)
    with pytest.raises(TypeError, match=r"OutboxClient\.delete_with_lease requires a live AsyncConnection"):
        await client.delete_with_lease(None, 1, uuid.uuid4())


async def test_client_mark_pending_with_lease_raises_typeerror_on_none_conn() -> None:
    metadata = MetaData()
    t = make_outbox_table(metadata)
    client = OutboxClient(AsyncMock(), t)
    now = _dt.datetime.now(tz=_dt.UTC)
    with pytest.raises(TypeError, match=r"OutboxClient\.mark_pending_with_lease requires a live AsyncConnection"):
        await client.mark_pending_with_lease(
            None,
            1,
            uuid.uuid4(),
            delay_seconds=1.0,
            attempts_count=1,
            first_attempt_at=now,
            last_attempt_at=now,
        )


# --- OutboxMessage.reject + assert_state_set logger branch ---


async def test_outbox_message_reject_calls_raw_then_super() -> None:
    inner = _make_msg()
    msg = OutboxMessage(
        raw_message=inner,
        body=b"",
        headers={},
        content_type=None,
        message_id="1",
        correlation_id="1",
    )
    await msg.reject()
    assert inner.to_delete  # raw_message.reject ran
    assert msg.committed is not None  # super().reject ran


async def test_message_assert_state_set_logs_when_logger_given() -> None:
    from unittest.mock import MagicMock  # noqa: PLC0415

    msg = _make_msg()
    logger = MagicMock()
    await msg.assert_state_set(logger=logger)
    logger.log.assert_called_once()
    assert msg.state_set


# --- OutboxRoute / specs / subscriber config ---


def test_outbox_route_constructs() -> None:
    from faststream_outbox.router import OutboxRoute  # noqa: PLC0415

    async def handler(body: str) -> None: ...

    route = OutboxRoute(handler, "orders")
    assert route is not None


def test_subscriber_specification_name_lists_queues() -> None:
    metadata = MetaData()
    t = make_outbox_table(metadata)
    broker = OutboxBroker(outbox_table=t)

    @broker.subscriber(["orders", "shipments"])
    async def handle(body: str) -> None: ...

    sub = next(iter(broker._subscribers))  # noqa: SLF001
    name = sub.specification.name
    assert "orders" in name
    assert "shipments" in name


async def test_subscriber_specification_get_schema() -> None:
    from faststream_outbox import TestOutboxBroker  # noqa: PLC0415

    metadata = MetaData()
    t = make_outbox_table(metadata)
    broker = OutboxBroker(outbox_table=t)

    @broker.subscriber("orders")
    async def handle(body: str) -> None: ...

    # get_schema() requires the subscriber to be set up (dependant computed),
    # which happens during broker.start(); use the test broker to get there.
    async with TestOutboxBroker(broker):
        sub = next(iter(broker._subscribers))  # noqa: SLF001
        schema = sub.specification.get_schema()
        assert schema  # non-empty dict
        spec = next(iter(schema.values()))
        title = spec.operation.message.title
        assert title is not None
        assert title.endswith(":Message")


# --- FakeOutboxClient direct tests ---


def test_fake_client_table_property() -> None:
    from faststream_outbox.testing import FakeOutboxClient  # noqa: PLC0415

    assert FakeOutboxClient().table is None


async def test_fake_client_fetch_empty_queues() -> None:
    from faststream_outbox.testing import FakeOutboxClient  # noqa: PLC0415

    client = FakeOutboxClient()
    assert await client.fetch(None, [], limit=10, lease_ttl_seconds=60.0) == []


async def test_fake_client_delete_miss() -> None:
    from faststream_outbox.testing import FakeOutboxClient  # noqa: PLC0415

    client = FakeOutboxClient()
    assert await client.delete_with_lease(None, 123, uuid.uuid4()) is False


async def test_fake_client_mark_pending_miss() -> None:
    from faststream_outbox.testing import FakeOutboxClient  # noqa: PLC0415

    client = FakeOutboxClient()
    now = _dt.datetime.now(tz=_dt.UTC)
    updated = await client.mark_pending_with_lease(
        None,
        999,
        uuid.uuid4(),
        delay_seconds=0.0,
        attempts_count=1,
        first_attempt_at=now,
        last_attempt_at=now,
    )
    assert updated is False


async def test_fake_client_validate_schema_raises_and_ping_passes() -> None:
    client = FakeOutboxClient()
    # validate_schema on the fake must raise loudly — a silent pass would let users
    # ship a broken DB schema while their TestOutboxBroker-backed tests stay green.
    with pytest.raises(NotImplementedError, match="validate_schema is unavailable"):
        await client.validate_schema()
    assert await client.ping() is True


# --- OutboxBrokerConfig connect/disconnect (no-op stubs) ---


async def test_outbox_broker_config_connect_disconnect_noop() -> None:
    from faststream_outbox.configs import OutboxBrokerConfig  # noqa: PLC0415

    cfg = OutboxBrokerConfig()
    await cfg.connect()  # must not raise
    await cfg.disconnect()  # must not raise


# --- subscriber get_one + _make_response_publisher ---


async def test_subscriber_get_one_raises() -> None:
    metadata = MetaData()
    t = make_outbox_table(metadata)
    broker = OutboxBroker(outbox_table=t)

    @broker.subscriber("orders")
    async def handle(body: str) -> None: ...

    sub = next(iter(broker._subscribers))  # noqa: SLF001
    with pytest.raises(NotImplementedError, match="get_one"):
        await sub.get_one()
    # _make_response_publisher returns ()
    assert sub._make_response_publisher(MagicMock()) == ()  # noqa: SLF001


async def test_subscriber_client_property_raises_when_broker_has_no_engine() -> None:
    broker = _make_broker()  # no engine → broker_config.client is None

    @broker.subscriber("orders")
    async def handle(body: dict) -> None: ...

    sub = next(iter(broker._subscribers))  # noqa: SLF001
    with pytest.raises(RuntimeError, match="not connected"):
        _ = sub._client  # noqa: SLF001


# --- _open_listen_connection fallback paths ---


def _make_subscriber_for_listener_test() -> OutboxSubscriber:
    broker = _make_broker()

    @broker.subscriber("orders")
    async def handle(body: dict) -> None: ...

    return next(iter(broker._subscribers))  # noqa: SLF001


async def test_open_listen_connection_returns_none_for_non_asyncpg_driver() -> None:
    sub = _make_subscriber_for_listener_test()
    engine = MagicMock()
    engine.url.drivername = "postgresql"  # no +asyncpg suffix

    result = await sub._open_listen_connection(engine)  # noqa: SLF001

    assert result is None


async def test_open_listen_connection_returns_none_when_asyncpg_connect_fails() -> None:
    sub = _make_subscriber_for_listener_test()
    engine = MagicMock()
    engine.url.drivername = "postgresql+asyncpg"
    engine.dialect.create_connect_args.return_value = (
        [],
        {"host": "h", "user": "u", "password": "p", "database": "db"},
    )

    with (
        patch.object(sub, "_log") as log_mock,
        patch(
            "faststream_outbox.subscriber.usecase._asyncpg.connect",
            new=AsyncMock(side_effect=OSError("boom")),
        ),
    ):
        result = await sub._open_listen_connection(engine)  # noqa: SLF001

    assert result is None
    log_mock.assert_called_once()
    assert "LISTEN setup failed" in log_mock.call_args.kwargs["message"]


async def test_open_listen_connection_passes_multihost_kwargs_to_asyncpg() -> None:
    """
    Multi-host URLs must reach asyncpg as host/port lists, not a re-rendered DSN.

    ``?host=h1:5432&host=h2:5432`` renders back as URL-encoded host tokens asyncpg
    can't parse. SQLAlchemy-only kwargs (``prepared_statement_cache_size``, ...) must
    be stripped before ``asyncpg.connect``, which rejects unknown kwargs.
    """
    sub = _make_subscriber_for_listener_test()
    engine = MagicMock()
    engine.url.drivername = "postgresql+asyncpg"
    engine.dialect.create_connect_args.return_value = (
        [],
        {
            "host": ["h1", "h2"],
            "port": [5432, 5432],
            "user": "u",
            "password": "p",
            "database": "db",
            "prepared_statement_cache_size": 100,
        },
    )

    fake_conn = MagicMock()
    fake_conn.add_listener = AsyncMock()
    connect_mock = AsyncMock(return_value=fake_conn)

    with (
        patch("faststream_outbox.subscriber.usecase._asyncpg.connect", new=connect_mock),
        patch.object(OutboxSubscriber, "_notify_channel", new="outbox_orders"),
    ):
        result = await sub._open_listen_connection(engine)  # noqa: SLF001

    assert result is fake_conn
    connect_mock.assert_awaited_once()
    assert connect_mock.await_args is not None
    kwargs = connect_mock.await_args.kwargs
    assert kwargs["host"] == ["h1", "h2"]
    assert kwargs["port"] == [5432, 5432]
    assert "prepared_statement_cache_size" not in kwargs
    fake_conn.add_listener.assert_awaited_once()


# --- listen_conn health check (H2 — silent listener death) ---


async def test_fetch_inner_raises_when_listen_health_check_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    """A dead listen_conn must surface as an exception so the outer loop reconnects."""
    monkeypatch.setattr("faststream_outbox.subscriber.usecase._LISTEN_HEALTH_CHECK_INTERVAL", 0.0)
    sub = _make_subscriber_for_listener_test()
    sub.running = True

    fake_listen_conn = MagicMock()
    fake_listen_conn.fetchval = AsyncMock(side_effect=ConnectionResetError("listener dead"))

    with pytest.raises(ConnectionResetError):
        await sub._fetch_inner(fetch_conn=None, listen_conn=fake_listen_conn)  # noqa: SLF001

    fake_listen_conn.fetchval.assert_awaited_once_with("SELECT 1")


async def test_fetch_inner_listen_health_check_succeeds_and_resumes(monkeypatch: pytest.MonkeyPatch) -> None:
    """A healthy listen_conn probe updates last_listen_check and the loop continues."""
    monkeypatch.setattr("faststream_outbox.subscriber.usecase._LISTEN_HEALTH_CHECK_INTERVAL", 0.0)
    sub = _make_subscriber_for_listener_test()
    sub.running = True

    fake_listen_conn = MagicMock()

    async def _ok_then_stop(*_args: object) -> int:
        sub.running = False
        return 1

    fake_listen_conn.fetchval = _ok_then_stop

    # Bypass the fetch branch by saturating inflight; the health check still runs before that.
    for _ in range(sub._inflight.maxsize):  # noqa: SLF001
        sub._inflight.put_nowait(MagicMock())  # noqa: SLF001

    await sub._fetch_inner(fetch_conn=None, listen_conn=fake_listen_conn)  # noqa: SLF001


async def test_fetch_inner_raises_on_listen_health_check_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    """A listen_conn whose probe hangs must surface as TimeoutError via wait_for."""
    monkeypatch.setattr("faststream_outbox.subscriber.usecase._LISTEN_HEALTH_CHECK_INTERVAL", 0.0)
    monkeypatch.setattr("faststream_outbox.subscriber.usecase._LISTEN_HEALTH_CHECK_TIMEOUT", 0.01)
    sub = _make_subscriber_for_listener_test()
    sub.running = True

    async def _hang(*_args: object) -> None:
        await asyncio.sleep(60)

    fake_listen_conn = MagicMock()
    fake_listen_conn.fetchval = _hang

    with pytest.raises(TimeoutError):
        await sub._fetch_inner(fetch_conn=None, listen_conn=fake_listen_conn)  # noqa: SLF001


# --- M3 — per-worker cached writer connection -----------------------------------------


def _make_broker_for_dispatch(fake: FakeOutboxClient) -> tuple[OutboxBroker, TestOutboxBroker]:
    """
    Build a broker + TestOutboxBroker harness so logger / config wiring is initialized.

    The caller enters the harness via ``async with`` to run dispatch_one against a real
    subscriber instance whose ``_outer_config.logger`` is wired by FastStream's lifecycle.
    Subscribes with ``max_deliveries=1`` so a row with ``deliveries_count >= 2`` bypasses
    the handler (``allow_delivery`` returns False) and routes straight to the flush path —
    which is what these tests assert against.
    """
    broker = _make_broker()

    @broker.subscriber("orders", max_deliveries=1)
    async def handle(body: dict) -> None: ...

    test_broker = TestOutboxBroker(broker)
    test_broker.fake_client = fake
    return broker, test_broker


def _make_msg_over_max_deliveries(**overrides: object) -> OutboxInnerMessage:
    """Build a row already past max_deliveries=1 so dispatch_one skips consume() and goes to flush."""
    overrides.setdefault("deliveries_count", 2)
    return _make_msg(**overrides)


async def test_fake_client_fetch_ties_break_on_id() -> None:
    """L2: rows with identical ``next_attempt_at`` are claimed in ``id`` order."""
    client = FakeOutboxClient()
    same_time = _dt.datetime(2026, 5, 23, 12, 0, 0, tzinfo=_dt.UTC)
    id_a = client.feed(queue="orders", payload=b"a", next_attempt_at=same_time)
    id_b = client.feed(queue="orders", payload=b"b", next_attempt_at=same_time)
    id_c = client.feed(queue="orders", payload=b"c", next_attempt_at=same_time)
    assert (id_a, id_b, id_c) == (1, 2, 3)

    fetched = await client.fetch(None, ["orders"], limit=3, lease_ttl_seconds=60.0)
    assert [r.id for r in fetched] == [1, 2, 3]


@pytest.mark.parametrize("writer_conn", [None, "sentinel"])
async def test_dispatch_one_threads_writer_conn_into_delete(writer_conn: object) -> None:
    """``dispatch_one`` forwards ``writer_conn`` (sentinel or ``None``) into ``delete_with_lease``."""
    fake = FakeOutboxClient()
    broker, test_broker = _make_broker_for_dispatch(fake)
    msg = _make_msg_over_max_deliveries()
    conn_arg = MagicMock() if writer_conn == "sentinel" else None

    with patch.object(fake, "delete_with_lease", new=AsyncMock(return_value=True)) as spy:
        async with test_broker:
            sub = next(iter(broker._subscribers))  # noqa: SLF001
            await sub.dispatch_one(msg, writer_conn=conn_arg)

    spy.assert_awaited_once()
    assert spy.await_args is not None
    assert spy.await_args.args[0] is conn_arg


async def test_dispatch_one_propagates_flush_error_when_writer_conn_set() -> None:
    """A flush error against a cached writer conn must propagate so _worker_loop can reconnect."""

    class RaisingFake(FakeOutboxClient):
        async def delete_with_lease(self, conn: object, message_id: int, acquired_token: uuid.UUID) -> bool:  # noqa: ARG002
            msg = "writer conn poisoned"
            raise RuntimeError(msg)

    broker, test_broker = _make_broker_for_dispatch(RaisingFake())
    msg = _make_msg_over_max_deliveries()

    async with test_broker:
        sub = next(iter(broker._subscribers))  # noqa: SLF001
        with pytest.raises(RuntimeError, match="writer conn poisoned"):
            await sub.dispatch_one(msg, writer_conn=MagicMock())


async def test_dispatch_one_swallows_flush_error_when_writer_conn_none() -> None:
    """Legacy behavior: without a writer_conn, a raising delete is logged and swallowed."""

    class RaisingFake(FakeOutboxClient):
        async def delete_with_lease(self, conn: object, message_id: int, acquired_token: uuid.UUID) -> bool:  # noqa: ARG002
            msg = "delete blew up"
            raise RuntimeError(msg)

    broker, test_broker = _make_broker_for_dispatch(RaisingFake())
    msg = _make_msg_over_max_deliveries()

    async with test_broker:
        sub = next(iter(broker._subscribers))  # noqa: SLF001
        # Must not raise.
        await sub.dispatch_one(msg, writer_conn=None)


@pytest.mark.parametrize("writer_conn", [None, "sentinel"])
async def test_flush_retry_threads_writer_conn_into_mark_pending(writer_conn: object) -> None:
    """``_flush_retry`` forwards ``writer_conn`` (sentinel or ``None``) into ``mark_pending_with_lease``."""
    fake = FakeOutboxClient()
    broker, test_broker = _make_broker_for_dispatch(fake)
    msg = _make_msg()
    msg.pending_delay_seconds = 1.0  # set post-construction; field is init=False
    conn_arg = MagicMock() if writer_conn == "sentinel" else None

    with patch.object(fake, "mark_pending_with_lease", new=AsyncMock(return_value=True)) as spy:
        async with test_broker:
            sub = next(iter(broker._subscribers))  # noqa: SLF001
            await sub._flush_retry(msg, writer_conn=conn_arg)  # noqa: SLF001

    spy.assert_awaited_once()
    assert spy.await_args is not None
    assert spy.await_args.args[0] is conn_arg


async def test_dispatch_one_outer_except_swallows_consume_failure() -> None:
    """
    The defensive outer except in dispatch_one catches consume/assert_state_set bugs.

    Handler errors are normally caught by AckPolicy middleware. The outer except is the
    safety net for middleware-bypassing failures — patch ``consume`` directly to exercise it.
    """
    fake = FakeOutboxClient()
    broker, test_broker = _make_broker_for_dispatch(fake)
    msg = _make_msg()

    async def _boom(_row: object) -> None:
        msg_str = "consume bypassed middleware"
        raise RuntimeError(msg_str)

    async with test_broker:
        sub = next(iter(broker._subscribers))  # noqa: SLF001
        with patch.object(sub, "consume", new=_boom):
            # Must not raise — the outer except logs and returns.
            await sub.dispatch_one(msg, writer_conn=None)


async def test_flush_terminal_logs_lease_lost_at_warning_with_structured_fields() -> None:
    """M7: when delete returns rowcount=0 (lease reclaimed) the broker emits a WARNING with structured fields."""

    class LeaseLostFake(FakeOutboxClient):
        async def delete_with_lease(self, *args: object, **kwargs: object) -> bool:  # noqa: ARG002
            return False

    broker, test_broker = _make_broker_for_dispatch(LeaseLostFake())
    msg = _make_msg(id=42, queue="orders", deliveries_count=3)

    async with test_broker:
        sub = next(iter(broker._subscribers))  # noqa: SLF001
        with patch.object(sub, "_log") as spy_log:
            await sub._flush_terminal(msg, writer_conn=None)  # noqa: SLF001

    spy_log.assert_called_once()
    call = spy_log.call_args
    assert call.kwargs["log_level"] == logging.WARNING
    extra = call.kwargs["extra"]
    assert extra["event"] == "lease_lost"
    assert extra["phase"] == "terminal"
    assert extra["row_id"] == 42
    assert extra["queue"] == "orders"
    assert extra["deliveries_count"] == 3


async def test_flush_retry_logs_lease_lost_at_warning_with_structured_fields() -> None:
    """M7: when retry UPDATE returns rowcount=0 (lease reclaimed) the broker emits a WARNING with structured fields."""

    class LeaseLostFake(FakeOutboxClient):
        async def mark_pending_with_lease(self, *args: object, **kwargs: object) -> bool:  # noqa: ARG002
            return False

    broker, test_broker = _make_broker_for_dispatch(LeaseLostFake())
    msg = _make_msg(id=99, queue="orders", deliveries_count=2)
    msg.pending_delay_seconds = 1.0

    async with test_broker:
        sub = next(iter(broker._subscribers))  # noqa: SLF001
        with patch.object(sub, "_log") as spy_log:
            await sub._flush_retry(msg, writer_conn=None)  # noqa: SLF001

    spy_log.assert_called_once()
    call = spy_log.call_args
    assert call.kwargs["log_level"] == logging.WARNING
    extra = call.kwargs["extra"]
    assert extra["event"] == "lease_lost"
    assert extra["phase"] == "retry"
    assert extra["row_id"] == 99
    assert extra["queue"] == "orders"
    assert extra["deliveries_count"] == 2


async def test_flush_retry_propagates_error_with_writer_conn() -> None:
    """``_flush_retry`` propagates client errors when writer_conn is provided (production path)."""

    class RaisingFake(FakeOutboxClient):
        async def mark_pending_with_lease(self, *args: object, **kwargs: object) -> bool:  # noqa: ARG002
            msg = "retry write poisoned"
            raise RuntimeError(msg)

    broker, test_broker = _make_broker_for_dispatch(RaisingFake())
    msg = _make_msg()
    msg.pending_delay_seconds = 1.0

    async with test_broker:
        sub = next(iter(broker._subscribers))  # noqa: SLF001
        with pytest.raises(RuntimeError, match="retry write poisoned"):
            await sub._flush_retry(msg, writer_conn=MagicMock())  # noqa: SLF001


async def test_worker_loop_opens_writer_conn_once_when_engine_available() -> None:
    """The worker loop opens exactly one writer conn per iteration of its outer reconnect wrapper."""
    fake = FakeOutboxClient()
    broker, test_broker = _make_broker_for_dispatch(fake)
    fake_engine = MagicMock()
    fake_conn = MagicMock()
    aenter = AsyncMock(return_value=fake_conn)
    aexit = AsyncMock(return_value=None)
    fake_engine.connect.return_value.__aenter__ = aenter
    fake_engine.connect.return_value.__aexit__ = aexit
    seen_conns: list[object] = []

    async with test_broker:
        sub = next(iter(broker._subscribers))  # noqa: SLF001
        sub.running = True

        async def _inner_then_stop(*, writer_conn: object) -> None:
            seen_conns.append(writer_conn)
            sub.running = False

        # FakeOutboxClient.engine returns None by default; override so worker takes engine-present branch.
        with (
            patch.object(type(fake), "engine", new_callable=lambda: property(lambda _self: fake_engine)),
            patch.object(sub, "_worker_inner", new=_inner_then_stop),
        ):
            await sub._worker_loop()  # noqa: SLF001

    assert fake_engine.connect.call_count == 1
    assert seen_conns == [fake_conn]


async def test_worker_loop_takes_no_conn_path_when_engine_is_none() -> None:
    """Test-broker path: FakeOutboxClient.engine is None → no engine.connect() call, writer_conn=None."""
    fake = FakeOutboxClient()
    broker, test_broker = _make_broker_for_dispatch(fake)
    seen_conns: list[object] = []

    async with test_broker:
        sub = next(iter(broker._subscribers))  # noqa: SLF001
        sub.running = True

        async def _inner_then_stop(*, writer_conn: object) -> None:
            seen_conns.append(writer_conn)
            sub.running = False

        with patch.object(sub, "_worker_inner", new=_inner_then_stop):
            await sub._worker_loop()  # noqa: SLF001

    assert seen_conns == [None]


async def test_fetch_loop_takes_no_conn_path_when_engine_is_none() -> None:
    """Test-broker path: FakeOutboxClient.engine is None → no engine.connect(), no listen_conn."""
    fake = FakeOutboxClient()
    broker, test_broker = _make_broker_for_dispatch(fake)
    seen_kwargs: list[dict[str, object]] = []

    async with test_broker:
        sub = next(iter(broker._subscribers))  # noqa: SLF001
        sub.running = True

        async def _inner_then_stop(*, fetch_conn: object, listen_conn: object) -> None:
            seen_kwargs.append({"fetch_conn": fetch_conn, "listen_conn": listen_conn})
            sub.running = False

        with patch.object(sub, "_fetch_inner", new=_inner_then_stop):
            await sub._fetch_loop()  # noqa: SLF001

    assert seen_kwargs == [{"fetch_conn": None, "listen_conn": None}]


async def test_worker_loop_reconnects_after_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """A raised inner-loop error triggers backoff + a fresh engine.connect() on the next iteration."""
    sleeps: list[float] = []

    async def _record_sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr("faststream_outbox.subscriber.usecase.anyio.sleep", _record_sleep)

    fake = FakeOutboxClient()
    broker, test_broker = _make_broker_for_dispatch(fake)
    fake_engine = MagicMock()
    conn_a = MagicMock()
    conn_b = MagicMock()
    cm_a = MagicMock()
    cm_a.__aenter__ = AsyncMock(return_value=conn_a)
    cm_a.__aexit__ = AsyncMock(return_value=None)
    cm_b = MagicMock()
    cm_b.__aenter__ = AsyncMock(return_value=conn_b)
    cm_b.__aexit__ = AsyncMock(return_value=None)
    fake_engine.connect.side_effect = [cm_a, cm_b]
    call_count = {"n": 0}

    async with test_broker:
        sub = next(iter(broker._subscribers))  # noqa: SLF001
        sub.running = True

        async def _raise_then_stop(*, writer_conn: object) -> None:  # noqa: ARG001
            call_count["n"] += 1
            if call_count["n"] == 1:
                msg = "first iteration poisoned"
                raise RuntimeError(msg)
            sub.running = False

        with (
            patch.object(type(fake), "engine", new_callable=lambda: property(lambda _self: fake_engine)),
            patch.object(sub, "_worker_inner", new=_raise_then_stop),
        ):
            await sub._worker_loop()  # noqa: SLF001

    assert fake_engine.connect.call_count == 2
    assert len(sleeps) == 1  # one backoff between iterations
    assert sleeps[0] > 0


async def test_outbox_client_delete_with_lease_uses_caller_conn() -> None:
    """``delete_with_lease`` runs its statement on the supplied conn, not via engine.begin()."""
    metadata = MetaData()
    t = make_outbox_table(metadata)
    engine = MagicMock()
    client = OutboxClient(engine, t)

    fake_conn = MagicMock()
    begin_cm = MagicMock()
    begin_cm.__aenter__ = AsyncMock(return_value=None)
    begin_cm.__aexit__ = AsyncMock(return_value=None)
    fake_conn.begin = MagicMock(return_value=begin_cm)
    fake_conn.execute = AsyncMock(return_value=MagicMock(rowcount=1))

    deleted = await client.delete_with_lease(fake_conn, 42, uuid.uuid4())

    assert deleted is True
    fake_conn.begin.assert_called_once()
    fake_conn.execute.assert_awaited_once()
    engine.connect.assert_not_called()  # caller conn, not pool checkout
    engine.begin.assert_not_called()


async def test_outbox_client_mark_pending_with_lease_uses_caller_conn() -> None:
    """``mark_pending_with_lease`` runs its statement on the supplied conn."""
    metadata = MetaData()
    t = make_outbox_table(metadata)
    engine = MagicMock()
    client = OutboxClient(engine, t)

    fake_conn = MagicMock()
    begin_cm = MagicMock()
    begin_cm.__aenter__ = AsyncMock(return_value=None)
    begin_cm.__aexit__ = AsyncMock(return_value=None)
    fake_conn.begin = MagicMock(return_value=begin_cm)
    fake_conn.execute = AsyncMock(return_value=MagicMock(rowcount=1))
    now = _dt.datetime.now(tz=_dt.UTC)

    updated = await client.mark_pending_with_lease(
        fake_conn,
        7,
        uuid.uuid4(),
        delay_seconds=3.0,
        attempts_count=2,
        first_attempt_at=now,
        last_attempt_at=now,
    )

    assert updated is True
    fake_conn.begin.assert_called_once()
    fake_conn.execute.assert_awaited_once()
    engine.connect.assert_not_called()
    engine.begin.assert_not_called()


# --- _compute_backoff (S1) ---------------------------------------------------


def test_compute_backoff_within_jitter_bounds() -> None:
    # attempt=3, base=1.0 → unjittered value 4.0, ±50% jitter → [2.0, 6.0].
    for _ in range(100):
        delay = _compute_backoff(3, ceiling=1000.0)
        assert 2.0 <= delay <= 6.0


def test_compute_backoff_caps_at_ceiling() -> None:
    # Large attempt + small ceiling → result is always pinned to the ceiling.
    assert _compute_backoff(20, ceiling=1.0) == 1.0


def test_compute_backoff_respects_base_factor() -> None:
    # base=0.1, attempt=1 → unjittered 0.1 → jittered [0.05, 0.15].
    for _ in range(100):
        delay = _compute_backoff(1, ceiling=10.0, base=0.1)
        assert 0.05 <= delay <= 0.15
