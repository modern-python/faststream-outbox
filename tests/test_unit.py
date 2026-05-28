import asyncio
import datetime as _dt
import json
import logging
import typing
import uuid
import warnings
from unittest.mock import AsyncMock, MagicMock, patch

import faststream.asgi.factories.asyncapi.try_it_out
import pytest
from faststream.exceptions import IncorrectState
from faststream.middlewares import AckPolicy
from faststream.response.publish_type import PublishType
from faststream.response.response import PublishCommand
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
    OutboxPublisher,
    OutboxRouter,
    TestOutboxBroker,
    make_outbox_table,
)
from faststream_outbox.client import OutboxClient, _validate_schema_sync
from faststream_outbox.configs import OutboxBrokerConfig
from faststream_outbox.envelope import _encode_payload
from faststream_outbox.message import OutboxInnerMessage, OutboxMessage
from faststream_outbox.parser.parser import OutboxParser
from faststream_outbox.publisher.config import OutboxPublisherSpecificationConfig
from faststream_outbox.publisher.fake import OutboxFakePublisher
from faststream_outbox.publisher.producer import OutboxProducer
from faststream_outbox.publisher.specification import OutboxPublisherSpecification
from faststream_outbox.response import OutboxPublishCommand
from faststream_outbox.subscriber.usecase import OutboxSubscriber, _compute_backoff
from faststream_outbox.testing import FakeOutboxClient, FakeOutboxProducer


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


def test_publisher_returns_outbox_publisher() -> None:
    broker = _make_broker()
    pub = broker.publisher("orders", headers={"source": "test"})
    assert isinstance(pub, OutboxPublisher)
    assert pub.queue == "orders"
    assert pub.headers == {"source": "test"}


def test_publisher_decorator_raises_not_implemented() -> None:
    broker = _make_broker()
    pub = broker.publisher("orders")

    async def handler(body: dict) -> None: ...

    with pytest.raises(NotImplementedError, match="cannot decorate"):
        pub(handler)


async def test_publisher_publish_rejects_non_async_session() -> None:
    broker = _make_broker()
    pub = broker.publisher("orders")
    with pytest.raises(TypeError, match="AsyncSession"):
        await pub.publish(b"x", session=object())  # ty: ignore[invalid-argument-type]


async def test_publish_command_validates_activate_args_mutex() -> None:
    session = _make_session_mock()
    with pytest.raises(ValueError, match="activate_in / activate_at"):
        OutboxPublishCommand(
            b"x",
            queue="orders",
            session=session,
            activate_in=_dt.timedelta(seconds=1),
            activate_at=_dt.datetime.now(tz=_dt.UTC) + _dt.timedelta(seconds=1),
        )


async def test_publish_command_rejects_naive_activate_at() -> None:
    session = _make_session_mock()
    naive = _dt.datetime(2026, 5, 23, 12, 0, 0)  # noqa: DTZ001
    with pytest.raises(ValueError, match="timezone-aware"):
        OutboxPublishCommand(b"x", queue="orders", session=session, activate_at=naive)


async def test_publish_command_rejects_non_async_session() -> None:
    with pytest.raises(TypeError, match="AsyncSession"):
        OutboxPublishCommand(b"x", queue="orders", session=object())  # ty: ignore[invalid-argument-type]


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


async def test_outbox_producer_request_raises_not_implemented() -> None:
    metadata = MetaData()
    t = make_outbox_table(metadata)
    producer = OutboxProducer(table=t, parser=None, decoder=None)
    session = _make_session_mock()
    cmd = OutboxPublishCommand(b"x", queue="orders", session=session)
    with pytest.raises(NotImplementedError, match="request-reply"):
        await producer.request(cmd)


def test_outbox_producer_connect_disconnect_noop() -> None:
    metadata = MetaData()
    t = make_outbox_table(metadata)
    producer = OutboxProducer(table=t, parser=None, decoder=None)
    producer.connect()  # must not raise
    producer.disconnect()  # must not raise


def test_broker_exposes_outbox_producer() -> None:
    broker = _make_broker()
    producer = broker.config.broker_config.producer
    assert isinstance(producer, OutboxProducer)


# --- Publisher / producer / spec coverage edges ---


def test_publish_command_from_cmd_raises_not_implemented() -> None:
    """The relay path is rejected, so ``from_cmd`` has no legitimate caller."""
    cmd = PublishCommand(body=b"x", _publish_type=PublishType.PUBLISH)
    with pytest.raises(NotImplementedError, match="from_cmd is not supported"):
        OutboxPublishCommand.from_cmd(cmd)


async def test_publisher_internal_publish_method_raises() -> None:
    """``_publish`` is unreachable in normal use (``__call__`` raises first), but kept for protocol parity."""
    broker = _make_broker()
    pub = broker.publisher("orders")
    cmd = PublishCommand(body=b"x", _publish_type=PublishType.PUBLISH)
    with pytest.raises(NotImplementedError, match="cannot decorate"):
        await pub._publish(cmd, _extra_middlewares=())  # noqa: SLF001


async def test_publisher_request_raises_not_implemented() -> None:
    broker = _make_broker()
    pub = broker.publisher("orders")
    with pytest.raises(NotImplementedError, match="request-reply"):
        await pub.request(b"x")


async def test_outbox_producer_publish_batch_empty_bodies_is_noop() -> None:
    """Empty ``batch_bodies`` returns before any SQL fires (the real broker also short-circuits)."""
    metadata = MetaData()
    t = make_outbox_table(metadata)
    producer = OutboxProducer(table=t, parser=None, decoder=None)
    session = _make_session_mock()
    cmd = OutboxPublishCommand(None, queue="orders", session=session)
    cmd.batch_bodies = ()  # PublishCommand carries body=None, but be explicit
    await producer.publish_batch(cmd)
    session.execute.assert_not_called()


def _make_outbox_publisher_spec(*, title: str | None = None, queue: str = "orders") -> OutboxPublisherSpecification:
    broker = _make_broker()
    spec_config = OutboxPublisherSpecificationConfig(
        queue=queue,
        title_=title,
        description_=None,
        schema_=None,
        include_in_schema=True,
    )
    return OutboxPublisherSpecification(
        _outer_config=broker.config.broker_config,
        specification_config=spec_config,
    )


def test_publisher_spec_name_defaults_to_queue_publisher() -> None:
    assert _make_outbox_publisher_spec(queue="orders").name == "orders:Publisher"


def test_publisher_spec_name_uses_explicit_title() -> None:
    assert _make_outbox_publisher_spec(queue="orders", title="OrderPub").name == "OrderPub"


def test_publisher_spec_get_schema_returns_publisher_spec() -> None:
    schema = _make_outbox_publisher_spec(queue="orders").get_schema()
    assert "orders:Publisher" in schema
    entry = schema["orders:Publisher"]
    assert entry.operation.message.title == "orders:Publisher:Message"


# --- FakeOutboxProducer direct coverage ---


async def test_fake_outbox_producer_publish_batch_inserts_rows() -> None:
    broker = _make_broker()
    fake_client = FakeOutboxClient()
    producer = FakeOutboxProducer(fake_client, broker, serializer=None, run_loops=False)
    cmd = OutboxPublishCommand(b"a", b"b", queue="orders", session=_make_session_mock())
    await producer.publish_batch(cmd)
    assert len(fake_client.rows) == 2
    assert {r.payload for r in fake_client.rows} == {b"a", b"b"}


async def test_fake_outbox_producer_publish_batch_empty_is_noop() -> None:
    broker = _make_broker()
    fake_client = FakeOutboxClient()
    producer = FakeOutboxProducer(fake_client, broker, serializer=None, run_loops=False)
    cmd = OutboxPublishCommand(None, queue="orders", session=_make_session_mock())
    cmd.batch_bodies = ()
    await producer.publish_batch(cmd)
    assert fake_client.rows == []


async def test_fake_outbox_producer_request_raises() -> None:
    broker = _make_broker()
    producer = FakeOutboxProducer(FakeOutboxClient(), broker, serializer=None, run_loops=False)
    cmd = OutboxPublishCommand(b"x", queue="orders", session=_make_session_mock())
    with pytest.raises(NotImplementedError, match="request-reply"):
        await producer.request(cmd)


def test_fake_outbox_producer_connect_sets_serializer() -> None:
    broker = _make_broker()
    producer = FakeOutboxProducer(FakeOutboxClient(), broker, serializer=None, run_loops=False)
    sentinel = object()
    producer.connect(serializer=sentinel)
    assert producer._serializer is sentinel  # noqa: SLF001


def test_fake_outbox_producer_disconnect_is_noop() -> None:
    broker = _make_broker()
    producer = FakeOutboxProducer(FakeOutboxClient(), broker, serializer=None, run_loops=False)
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
    cfg = OutboxBrokerConfig()
    await cfg.connect()  # must not raise
    await cfg.disconnect()  # must not raise


# --- subscriber get_one + _make_response_publisher ---


async def test_subscriber_get_one_and_aiter_raise_with_fetch_unprocessed_pointer() -> None:
    metadata = MetaData()
    t = make_outbox_table(metadata)
    broker = OutboxBroker(outbox_table=t)

    @broker.subscriber("orders")
    async def handle(body: str) -> None: ...

    sub = next(iter(broker._subscribers))  # noqa: SLF001

    with pytest.raises(NotImplementedError, match="fetch_unprocessed"):
        await sub.get_one()

    # __aiter__ is also unsupported (was silently abstract-inherited before B6).
    with pytest.raises(NotImplementedError, match="fetch_unprocessed"):
        await sub.__aiter__()

    # _make_response_publisher returns an OutboxFakePublisher wired to the producer
    # so handlers can ``return OutboxResponse(...)``.
    publishers = sub._make_response_publisher(MagicMock())  # noqa: SLF001
    assert len(publishers) == 1
    assert isinstance(publishers[0], OutboxFakePublisher)


async def test_subscriber_client_property_raises_when_broker_has_no_engine() -> None:
    broker = _make_broker()  # no engine → broker_config.client is None

    @broker.subscriber("orders")
    async def handle(body: dict) -> None: ...

    sub = next(iter(broker._subscribers))  # noqa: SLF001
    with pytest.raises(RuntimeError, match="not connected"):
        _ = sub._client  # noqa: SLF001


def test_outbox_router_uses_outbox_broker_config() -> None:
    """B5: router's config must be an ``OutboxBrokerConfig`` (not a plain ``BrokerConfig``)."""
    router = OutboxRouter()
    # Router exposes config via ConfigComposition.broker_config; the concrete type lives there.
    assert isinstance(router.config.broker_config, OutboxBrokerConfig)


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


# --- ConstantRetry / LinearRetry jitter (G) ----------------------------------


def test_constant_retry_with_jitter_within_bounds() -> None:
    # Symmetric jitter ±jitter_factor/2 of the base delay (10).
    first, last = _make_times()
    s = ConstantRetry(delay_seconds=10.0, jitter_factor=0.4)
    saw_variation = False
    last_delay: float | None = None
    for _ in range(100):
        delay = s.get_next_attempt_delay(first_attempt_at=first, last_attempt_at=last, attempts_count=1)
        assert delay is not None
        assert 8.0 <= delay <= 12.0
        if last_delay is not None and delay != last_delay:
            saw_variation = True
        last_delay = delay
    assert saw_variation, "jitter should produce variation across calls"


def test_constant_retry_without_jitter_is_deterministic() -> None:
    # Default jitter_factor=0.0 must preserve the pre-existing exact-delay behavior.
    first, last = _make_times()
    s = ConstantRetry(delay_seconds=7.5)
    for _ in range(10):
        delay = s.get_next_attempt_delay(first_attempt_at=first, last_attempt_at=last, attempts_count=1)
        assert delay == 7.5


def test_linear_retry_with_jitter_within_bounds() -> None:
    # attempts_count=3 → base = 1 + 2*(3-1) = 5; jitter_factor=0.5 → [3.75, 6.25].
    first, last = _make_times()
    s = LinearRetry(initial_delay_seconds=1.0, step_seconds=2.0, jitter_factor=0.5)
    for _ in range(100):
        delay = s.get_next_attempt_delay(first_attempt_at=first, last_attempt_at=last, attempts_count=3)
        assert delay is not None
        assert 3.75 <= delay <= 6.25


def test_linear_retry_without_jitter_is_deterministic() -> None:
    first, last = _make_times()
    s = LinearRetry(initial_delay_seconds=1.0, step_seconds=2.0)
    delay = s.get_next_attempt_delay(first_attempt_at=first, last_attempt_at=last, attempts_count=3)
    assert delay == 5.0


# --- subscriber misconfiguration (C) -----------------------------------------


def _register_subscriber(broker: OutboxBroker, **subscriber_kwargs: object) -> None:
    """
    Trigger registration-time misconfig validation; no handler needed.

    ``_validate_subscriber_config`` runs inside ``create_subscriber``, before
    ``add_call``, so the warning/error fires from the ``broker.subscriber(...)``
    call itself — no ``@`` decorator necessary.
    """
    broker.subscriber("orders", **subscriber_kwargs)  # ty: ignore[invalid-argument-type]


def test_subscriber_rejects_zero_max_workers() -> None:
    broker = _make_broker()
    with pytest.raises(ValueError, match="max_workers must be >= 1"):
        _register_subscriber(broker, max_workers=0)


def test_subscriber_rejects_zero_fetch_batch_size() -> None:
    broker = _make_broker()
    with pytest.raises(ValueError, match="fetch_batch_size must be >= 1"):
        _register_subscriber(broker, fetch_batch_size=0)


def test_subscriber_rejects_min_above_max_fetch_interval() -> None:
    broker = _make_broker()
    with pytest.raises(ValueError, match=r"min_fetch_interval .* must be <= max_fetch_interval"):
        _register_subscriber(broker, min_fetch_interval=10.0, max_fetch_interval=1.0)


def test_subscriber_rejects_ack_first() -> None:
    # ACK_FIRST has no legitimate outbox use — deletes before the handler runs, so a
    # handler crash silently drops the row. Better to refuse than warn-and-ship.
    broker = _make_broker()
    with pytest.raises(ValueError, match="ACK_FIRST is not supported"):
        _register_subscriber(broker, ack_policy=AckPolicy.ACK_FIRST)


def test_subscriber_warns_on_reject_with_retry_strategy() -> None:
    broker = _make_broker()
    with pytest.warns(UserWarning, match="REJECT_ON_ERROR rejects on the first handler error"):
        _register_subscriber(
            broker, ack_policy=AckPolicy.REJECT_ON_ERROR, retry_strategy=ConstantRetry(delay_seconds=1)
        )


def test_subscriber_warns_on_nack_with_no_retry() -> None:
    broker = _make_broker()
    with pytest.warns(UserWarning, match="NACK_ON_ERROR with retry_strategy=NoRetry"):
        _register_subscriber(broker, ack_policy=AckPolicy.NACK_ON_ERROR, retry_strategy=NoRetry())


def test_subscriber_warns_on_max_deliveries_with_no_retry() -> None:
    broker = _make_broker()
    with pytest.warns(UserWarning, match="max_deliveries is set but no retry_strategy"):
        _register_subscriber(broker, max_deliveries=5, retry_strategy=NoRetry())


def test_subscriber_warns_on_lease_ttl_below_max_fetch_interval() -> None:
    broker = _make_broker()
    with pytest.warns(UserWarning, match=r"lease_ttl_seconds .* <= max_fetch_interval"):
        _register_subscriber(broker, lease_ttl_seconds=5.0, max_fetch_interval=10.0)


def test_subscriber_no_warning_on_default_config() -> None:
    """The default subscriber config (no ack_policy, default retry, no max_deliveries) must be silent."""
    broker = _make_broker()
    with warnings.catch_warnings():
        warnings.simplefilter("error", UserWarning)
        # Defaults: max_workers=1, lease_ttl_seconds=60, max_fetch_interval=10 → no warning.
        _register_subscriber(broker)


def test_subscriber_reject_on_error_with_no_retry_is_silent() -> None:
    broker = _make_broker()
    with warnings.catch_warnings():
        warnings.simplefilter("error", UserWarning)
        # REJECT_ON_ERROR + NoRetry: the same effective behavior. We only warn about
        # REJECT + an *active* retry strategy, not REJECT + NoRetry.
        _register_subscriber(broker, ack_policy=AckPolicy.REJECT_ON_ERROR, retry_strategy=NoRetry())


# --- MetricsRecorder seam ------------------------------------------------------------


def _events_recorder() -> tuple[list[tuple[str, dict]], typing.Any]:
    events: list[tuple[str, dict]] = []

    def recorder(event: str, tags: typing.Any) -> None:
        events.append((event, dict(tags)))

    return events, recorder


def _make_broker_with_recorder(recorder: typing.Any, *, max_deliveries: int | None = None) -> OutboxBroker:
    metadata = MetaData()
    table = make_outbox_table(metadata)
    broker = OutboxBroker(outbox_table=table, metrics_recorder=recorder)

    @broker.subscriber("orders", max_deliveries=max_deliveries)
    async def handle(body: dict) -> None: ...

    return broker


async def test_metrics_default_recorder_is_noop_and_publish_consume_cycle_does_not_raise() -> None:
    broker = _make_broker()

    @broker.subscriber("orders")
    async def handle(body: dict) -> None: ...

    test_broker = TestOutboxBroker(broker)
    session = _make_session_mock()
    async with test_broker:
        await broker.publish({"x": 1}, queue="orders", session=session)


async def test_metrics_dispatched_and_acked_fire_with_expected_tags() -> None:
    events, recorder = _events_recorder()
    broker = _make_broker_with_recorder(recorder)
    session = _make_session_mock()
    async with TestOutboxBroker(broker):
        await broker.publish({"x": 1}, queue="orders", session=session)
    names = [e for e, _ in events]
    assert "dispatched" in names
    assert "acked" in names
    acked_tags = next(t for e, t in events if e == "acked")
    assert acked_tags["queue"] == "orders"
    # FastStream camel-cases the handler's __name__ for ``call_name``; assert
    # case-insensitively so the test isn't pinned to upstream's casing.
    assert acked_tags["subscriber"].lower() == "handle"
    assert acked_tags["duration_seconds"] >= 0.0
    dispatched_tags = next(t for e, t in events if e == "dispatched")
    assert dispatched_tags["size_bytes"] > 0


async def test_metrics_recorder_raise_is_swallowed_and_consume_completes() -> None:
    def raising(event: str, tags: typing.Any) -> None:  # noqa: ARG001
        msg = "recorder boom"
        raise RuntimeError(msg)

    broker = _make_broker_with_recorder(raising)
    test_broker = TestOutboxBroker(broker)
    session = _make_session_mock()
    async with test_broker:
        await broker.publish({"x": 1}, queue="orders", session=session)
        # Row must be deleted because consume completed and acked.
        assert not test_broker.fake_client.rows


async def test_metrics_lease_lost_terminal_emits_recorder_event() -> None:
    events, recorder = _events_recorder()

    class LeaseLostFake(FakeOutboxClient):
        async def delete_with_lease(self, *args: object, **kwargs: object) -> bool:  # noqa: ARG002
            return False

    broker = _make_broker_with_recorder(recorder, max_deliveries=1)
    test_broker = TestOutboxBroker(broker)
    test_broker.fake_client = LeaseLostFake()
    msg = _make_msg(id=42, queue="orders", deliveries_count=3)

    async with test_broker:
        sub = next(iter(broker._subscribers))  # noqa: SLF001
        await sub._flush_terminal(msg, writer_conn=None)  # noqa: SLF001

    lease_events = [t for e, t in events if e == "lease_lost"]
    assert len(lease_events) == 1
    assert lease_events[0]["phase"] == "terminal"
    assert lease_events[0]["row_id"] == 42
    assert lease_events[0]["queue"] == "orders"


async def test_metrics_lease_lost_retry_emits_recorder_event() -> None:
    events, recorder = _events_recorder()

    class LeaseLostFake(FakeOutboxClient):
        async def mark_pending_with_lease(self, *args: object, **kwargs: object) -> bool:  # noqa: ARG002
            return False

    broker = _make_broker_with_recorder(recorder, max_deliveries=1)
    test_broker = TestOutboxBroker(broker)
    test_broker.fake_client = LeaseLostFake()
    msg = _make_msg(id=99, queue="orders", deliveries_count=2)
    msg.pending_delay_seconds = 1.0

    async with test_broker:
        sub = next(iter(broker._subscribers))  # noqa: SLF001
        await sub._flush_retry(msg, writer_conn=None)  # noqa: SLF001

    lease_events = [t for e, t in events if e == "lease_lost"]
    assert len(lease_events) == 1
    assert lease_events[0]["phase"] == "retry"


async def test_metrics_fetched_emits_with_count() -> None:
    events, recorder = _events_recorder()
    broker = _make_broker_with_recorder(recorder)
    sub = next(iter(broker._subscribers))  # noqa: SLF001
    fake = FakeOutboxClient()
    fake.feed(queue="orders", payload=b"a")
    fake.feed(queue="orders", payload=b"b")
    fake.feed(queue="orders", payload=b"c")
    broker.config.broker_config.client = fake
    sub.running = True

    async def _stop_after(*args: object, **kwargs: object) -> list[OutboxInnerMessage]:  # noqa: ARG001
        sub.running = False
        return await FakeOutboxClient.fetch(fake, None, ["orders"], limit=3, lease_ttl_seconds=60.0)

    with patch.object(fake, "fetch", new=_stop_after):
        await sub._fetch_inner(fetch_conn=None, listen_conn=None)  # noqa: SLF001

    fetched = [t for e, t in events if e == "fetched"]
    assert len(fetched) == 1
    assert fetched[0]["count"] == 3
    assert fetched[0]["queue"] == "orders"


async def test_metrics_fetched_emits_count_zero_on_idle() -> None:
    events, recorder = _events_recorder()
    broker = _make_broker_with_recorder(recorder)
    sub = next(iter(broker._subscribers))  # noqa: SLF001
    fake = FakeOutboxClient()
    broker.config.broker_config.client = fake
    sub.running = True

    async def _empty_then_stop(*args: object, **kwargs: object) -> list[OutboxInnerMessage]:  # noqa: ARG001
        sub.running = False
        return []

    with patch.object(fake, "fetch", new=_empty_then_stop):
        await sub._fetch_inner(fetch_conn=None, listen_conn=None)  # noqa: SLF001

    fetched = [t for e, t in events if e == "fetched"]
    assert len(fetched) == 1
    assert fetched[0]["count"] == 0


async def test_metrics_max_deliveries_emits_terminal_reason() -> None:
    events, recorder = _events_recorder()
    broker = _make_broker_with_recorder(recorder, max_deliveries=1)
    fake = FakeOutboxClient()
    test_broker = TestOutboxBroker(broker)
    test_broker.fake_client = fake
    msg = _make_msg_over_max_deliveries()

    async with test_broker:
        sub = next(iter(broker._subscribers))  # noqa: SLF001
        await sub.dispatch_one(msg, writer_conn=None)

    terminals = [t for e, t in events if e == "nacked_terminal"]
    assert len(terminals) == 1
    assert terminals[0]["reason"] == "max_deliveries"
    assert "duration_seconds" not in terminals[0]


async def test_metrics_nacked_retried_includes_next_delay_and_exception_type() -> None:
    events, recorder = _events_recorder()

    metadata = MetaData()
    table = make_outbox_table(metadata)
    broker = OutboxBroker(outbox_table=table, metrics_recorder=recorder)

    @broker.subscriber("orders", retry_strategy=ConstantRetry(delay_seconds=42.0))
    async def handle(body: dict) -> None:  # noqa: ARG001
        msg = "boom"
        raise RuntimeError(msg)

    session = _make_session_mock()
    async with TestOutboxBroker(broker):
        await broker.publish({"x": 1}, queue="orders", session=session)

    retried = [t for e, t in events if e == "nacked_retried"]
    assert len(retried) == 1
    assert retried[0]["next_delay_seconds"] == 42.0
    assert retried[0]["exception_type"] == "RuntimeError"
    assert retried[0]["duration_seconds"] >= 0.0


async def test_metrics_retry_terminal_emits_with_duration() -> None:
    events, recorder = _events_recorder()

    metadata = MetaData()
    table = make_outbox_table(metadata)
    broker = OutboxBroker(outbox_table=table, metrics_recorder=recorder)

    @broker.subscriber("orders", retry_strategy=NoRetry())
    async def handle(body: dict) -> None:  # noqa: ARG001
        msg = "boom"
        raise RuntimeError(msg)

    session = _make_session_mock()
    async with TestOutboxBroker(broker):
        await broker.publish({"x": 1}, queue="orders", session=session)

    terminals = [t for e, t in events if e == "nacked_terminal"]
    assert len(terminals) == 1
    assert terminals[0]["reason"] == "retry_terminal"
    assert terminals[0]["exception_type"] == "RuntimeError"
    assert terminals[0]["duration_seconds"] >= 0.0


async def test_metrics_duration_seconds_reflects_handler_runtime() -> None:
    events, recorder = _events_recorder()

    metadata = MetaData()
    table = make_outbox_table(metadata)
    broker = OutboxBroker(outbox_table=table, metrics_recorder=recorder)

    @broker.subscriber("orders")
    async def handle(body: dict) -> None:  # noqa: ARG001
        await asyncio.sleep(0.02)

    session = _make_session_mock()
    async with TestOutboxBroker(broker):
        await broker.publish({"x": 1}, queue="orders", session=session)

    acked = next(t for e, t in events if e == "acked")
    assert acked["duration_seconds"] >= 0.015


async def test_metrics_published_event_fires_with_success_status_and_size() -> None:
    events, recorder = _events_recorder()
    broker = _make_broker_with_recorder(recorder)
    session = _make_session_mock()
    async with TestOutboxBroker(broker):
        await broker.publish({"x": 1}, queue="orders", session=session)

    pub = [t for e, t in events if e == "published"]
    assert len(pub) >= 1
    assert pub[0]["status"] == "success"
    assert pub[0]["queue"] == "orders"
    assert pub[0]["count"] == 1
    assert pub[0]["size_bytes"] > 0


async def test_metrics_published_batch_emits_count_equal_to_batch_size() -> None:
    events, recorder = _events_recorder()
    broker = _make_broker_with_recorder(recorder)
    session = _make_session_mock()
    async with TestOutboxBroker(broker):
        await broker.publish_batch({"x": 1}, {"y": 2}, {"z": 3}, queue="orders", session=session)

    pub = [t for e, t in events if e == "published"]
    # In sync test mode, publish_batch routes each body through fake publish individually,
    # so we see 3 published events (one per body) rather than 1 batch event with count=3.
    # Either contract is acceptable — assert the cumulative count totals to the batch size.
    assert sum(t["count"] for t in pub) == 3


# --- OutboxProducer error-path coverage --------------------------------------------------


def _make_producer(recorder: typing.Any) -> OutboxProducer:
    metadata = MetaData()
    table = make_outbox_table(metadata)
    return OutboxProducer(table=table, parser=None, decoder=None, metrics_recorder=recorder)


async def test_producer_publish_emits_error_event_and_reraises_on_sql_failure() -> None:
    events, recorder = _events_recorder()
    producer = _make_producer(recorder)
    session = AsyncMock(spec=AsyncSession)
    session.execute.side_effect = RuntimeError("forced INSERT failure")
    cmd = OutboxPublishCommand({"x": 1}, queue="orders", session=session)

    with pytest.raises(RuntimeError, match="forced INSERT failure"):
        await producer.publish(cmd)

    pub = [t for e, t in events if e == "published"]
    assert len(pub) == 1
    assert pub[0]["status"] == "error"
    assert pub[0]["count"] == 0
    assert pub[0]["exception_type"] == "RuntimeError"
    assert pub[0]["queue"] == "orders"
    assert pub[0]["duration_seconds"] >= 0.0
    assert pub[0]["size_bytes"] > 0


async def test_producer_publish_batch_emits_error_event_and_reraises_on_sql_failure() -> None:
    events, recorder = _events_recorder()
    producer = _make_producer(recorder)
    session = AsyncMock(spec=AsyncSession)
    session.execute.side_effect = RuntimeError("forced batch INSERT failure")
    bodies = [{"x": 1}, {"y": 2}, {"z": 3}]
    cmd = OutboxPublishCommand(*bodies, queue="orders", session=session)

    with pytest.raises(RuntimeError, match="forced batch INSERT failure"):
        await producer.publish_batch(cmd)

    pub = [t for e, t in events if e == "published"]
    assert len(pub) == 1
    assert pub[0]["status"] == "error"
    assert pub[0]["count"] == 0
    assert pub[0]["exception_type"] == "RuntimeError"
    assert pub[0]["queue"] == "orders"
    # The producer encodes every body before calling session.execute, so size_bytes
    # reflects the cumulative encoded payload size even though no row landed.
    assert pub[0]["size_bytes"] > 0
    assert pub[0]["duration_seconds"] >= 0.0


async def test_producer_emit_metric_swallows_recorder_exceptions(caplog: pytest.LogCaptureFixture) -> None:
    """A raising recorder must not poison the producer's success path."""

    def raising_recorder(event: str, tags: typing.Any) -> None:  # noqa: ARG001
        msg = "recorder boom"
        raise RuntimeError(msg)

    producer = _make_producer(raising_recorder)
    session = _make_session_mock(scalar_return=99)
    cmd = OutboxPublishCommand({"x": 1}, queue="orders", session=session)

    with caplog.at_level(logging.DEBUG, logger="faststream_outbox.publisher.producer"):
        row_id = await producer.publish(cmd)

    # publish completes despite the raising recorder.
    assert row_id == 99
    matching = [r for r in caplog.records if "metrics recorder raised" in r.getMessage() and r.exc_info is not None]
    assert matching, "expected DEBUG log 'metrics recorder raised' with exc_info"
