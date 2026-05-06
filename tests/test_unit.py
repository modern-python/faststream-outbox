import datetime as _dt
import uuid
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import MetaData

from faststream_outbox import (
    ConstantRetry,
    ExponentialRetry,
    LinearRetry,
    NoRetry,
    OutboxBroker,
    OutboxRouter,
    OutboxState,
    encode_payload,
    make_outbox_table,
)
from faststream_outbox.message import OutboxInnerMessage, OutboxMessage
from faststream_outbox.parser.parser import OutboxParser


# --- make_outbox_table ---


def test_make_outbox_table_columns_present() -> None:
    metadata = MetaData()
    t = make_outbox_table(metadata, table_name="my_outbox")
    expected = {
        "id",
        "queue",
        "payload",
        "headers",
        "state",
        "attempts_count",
        "deliveries_count",
        "created_at",
        "next_attempt_at",
        "first_attempt_at",
        "last_attempt_at",
        "acquired_at",
        "acquired_token",
    }
    assert {c.name for c in t.columns} == expected
    assert t.name == "my_outbox"


def test_make_outbox_table_attaches_to_metadata() -> None:
    metadata = MetaData()
    t = make_outbox_table(metadata, table_name="outbox")
    assert "outbox" in metadata.tables
    assert metadata.tables["outbox"] is t


# --- encode_payload ---


def test_encode_payload_dict_sets_content_type_and_correlation() -> None:
    payload, headers = encode_payload({"order_id": 1})
    assert payload == b'{"order_id": 1}'
    assert headers["content-type"] == "application/json"
    assert headers["correlation_id"]


def test_encode_payload_preserves_user_correlation_id() -> None:
    _, headers = encode_payload({"x": 1}, correlation_id="trace-abc")
    assert headers["correlation_id"] == "trace-abc"


def test_encode_payload_passes_through_bytes() -> None:
    payload, headers = encode_payload(b"raw bytes here")
    assert payload == b"raw bytes here"
    # No content-type for raw bytes
    assert "content-type" not in headers
    assert headers["correlation_id"]


def test_encode_payload_merges_user_headers() -> None:
    _, headers = encode_payload({"x": 1}, headers={"x-tenant": "acme"})
    assert headers["x-tenant"] == "acme"
    assert headers["content-type"] == "application/json"


# --- retry strategies ---


def _make_times() -> tuple[_dt.datetime, _dt.datetime]:
    first = _dt.datetime(2026, 1, 1, tzinfo=_dt.UTC)
    last = first + _dt.timedelta(seconds=10)
    return first, last


def test_no_retry_always_terminal() -> None:
    first, last = _make_times()
    assert (
        NoRetry().get_next_attempt_at(
            first_attempt_at=first,
            last_attempt_at=last,
            attempts_count=1,
        )
        is None
    )


def test_constant_retry_returns_last_plus_delay() -> None:
    first, last = _make_times()
    next_at = ConstantRetry(delay_seconds=30).get_next_attempt_at(
        first_attempt_at=first,
        last_attempt_at=last,
        attempts_count=1,
    )
    assert next_at == last + _dt.timedelta(seconds=30)


def test_constant_retry_max_attempts_reached() -> None:
    first, last = _make_times()
    s = ConstantRetry(delay_seconds=1, max_attempts=3)
    assert s.get_next_attempt_at(first_attempt_at=first, last_attempt_at=last, attempts_count=3) is None
    assert s.get_next_attempt_at(first_attempt_at=first, last_attempt_at=last, attempts_count=2) is not None


def test_constant_retry_max_total_delay_exceeded() -> None:
    first, last = _make_times()
    s = ConstantRetry(delay_seconds=100, max_total_delay_seconds=50)
    assert s.get_next_attempt_at(first_attempt_at=first, last_attempt_at=last, attempts_count=1) is None


def test_linear_retry_grows_with_attempts() -> None:
    first, last = _make_times()
    s = LinearRetry(initial_delay_seconds=10, step_seconds=5)
    n1 = s.get_next_attempt_at(first_attempt_at=first, last_attempt_at=last, attempts_count=1)
    n2 = s.get_next_attempt_at(first_attempt_at=first, last_attempt_at=last, attempts_count=3)
    assert n1 is not None
    assert n2 is not None
    assert n2 > n1


def test_exponential_retry_caps_at_max_delay() -> None:
    first, last = _make_times()
    s = ExponentialRetry(initial_delay_seconds=1, multiplier=2, max_delay_seconds=10)
    next_at = s.get_next_attempt_at(first_attempt_at=first, last_attempt_at=last, attempts_count=10)
    assert next_at == last + _dt.timedelta(seconds=10)


def test_exponential_retry_with_jitter_within_bounds() -> None:
    first, last = _make_times()
    s = ExponentialRetry(initial_delay_seconds=10, multiplier=1.0, jitter_factor=0.5)
    next_at = s.get_next_attempt_at(first_attempt_at=first, last_attempt_at=last, attempts_count=1)
    assert next_at is not None
    delta = (next_at - last).total_seconds()
    assert 10.0 <= delta <= 15.0


# --- OutboxInnerMessage state machine ---


def _make_msg(**overrides: object) -> OutboxInnerMessage:
    base: dict = {
        "id": 1,
        "queue": "q",
        "payload": b"p",
        "headers": None,
        "state": OutboxState.PROCESSING,
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
    assert msg.next_attempt_at > msg.last_attempt_at


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
    metadata = MetaData()
    t = make_outbox_table(metadata)
    broker = OutboxBroker(outbox_table=t)
    assert broker.config.broker_config.client is None


def test_broker_with_engine_has_client() -> None:
    metadata = MetaData()
    t = make_outbox_table(metadata)
    engine = AsyncMock()
    broker = OutboxBroker(engine, outbox_table=t)
    assert broker.config.broker_config.client is not None


async def test_broker_publish_raises() -> None:
    metadata = MetaData()
    t = make_outbox_table(metadata)
    broker = OutboxBroker(outbox_table=t)
    with pytest.raises(NotImplementedError, match="no publish API"):
        await broker.publish(b"x")


async def test_broker_request_raises() -> None:
    metadata = MetaData()
    t = make_outbox_table(metadata)
    broker = OutboxBroker(outbox_table=t)
    with pytest.raises(NotImplementedError):
        await broker.request(b"x")


async def test_broker_ping_no_client_returns_false() -> None:
    metadata = MetaData()
    t = make_outbox_table(metadata)
    broker = OutboxBroker(outbox_table=t)
    assert await broker.ping() is False


async def test_broker_ping_when_engine_query_fails() -> None:
    metadata = MetaData()
    t = make_outbox_table(metadata)
    engine = AsyncMock()
    engine.connect.return_value.__aenter__.side_effect = ConnectionError("nope")
    broker = OutboxBroker(engine, outbox_table=t)
    assert await broker.ping() is False


# --- registrator validation ---


def test_subscriber_empty_queue_list_raises() -> None:
    metadata = MetaData()
    t = make_outbox_table(metadata)
    broker = OutboxBroker(outbox_table=t)
    with pytest.raises(ValueError, match="at least one queue"):
        broker.subscriber([])


def test_publisher_raises_not_implemented() -> None:
    metadata = MetaData()
    t = make_outbox_table(metadata)
    broker = OutboxBroker(outbox_table=t)
    with pytest.raises(NotImplementedError, match="no publisher"):
        broker.publisher("orders")


def test_duplicate_subscriber_warns() -> None:
    metadata = MetaData()
    t = make_outbox_table(metadata)
    broker = OutboxBroker(outbox_table=t)

    @broker.subscriber("orders")
    async def first(body: dict) -> None: ...

    with pytest.warns(UserWarning, match="Duplicate subscriber"):

        @broker.subscriber("orders")
        async def second(body: dict) -> None: ...


def test_router_can_be_constructed() -> None:
    router = OutboxRouter(prefix="svc-")
    assert router is not None
