import asyncio
import datetime as _dt
import json
import logging
import math
import re
import typing
import uuid
import warnings
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import faststream.asgi.factories.asyncapi.try_it_out
import pytest
from faststream._internal.parser import DefaultCodec
from faststream._internal.producer import ProducerProto
from faststream.exceptions import IncorrectState
from faststream.middlewares import AckPolicy
from faststream.response.publish_type import PublishType
from faststream.response.response import PublishCommand
from faststream.specification import AsyncAPI
from pydantic import BaseModel
from sqlalchemy import MetaData, Table
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from faststream_outbox import (
    ConstantRetry,
    ExponentialRetry,
    LinearRetry,
    NoRetry,
    OutboxBroker,
    OutboxPublisher,
    OutboxResponse,
    OutboxRouter,
    TestOutboxBroker,
    make_dlq_table,
    make_outbox_table,
)
from faststream_outbox.annotations import OutboxMessage as AnnotatedOutboxMessage
from faststream_outbox.broker import OutboxParamsStorage
from faststream_outbox.client import (
    _AUTOGEN_BLIND_HINT,
    _SCHEMA_MISMATCH_PREFIX,
    OutboxClient,
    _compose_schema_mismatch_message,
    _validate_check_constraints_sync,
    _validate_schema_sync,
)
from faststream_outbox.configs import OutboxBrokerConfig
from faststream_outbox.envelope import _encode_payload
from faststream_outbox.message import OutboxInnerMessage, OutboxMessage
from faststream_outbox.parser.parser import OutboxParser
from faststream_outbox.publisher.config import OutboxPublisherSpecificationConfig
from faststream_outbox.publisher.fake import OutboxFakePublisher
from faststream_outbox.publisher.producer import OutboxProducer
from faststream_outbox.publisher.specification import OutboxPublisherSpecification
from faststream_outbox.registrator import _default_retry_strategy
from faststream_outbox.response import OutboxPublishCommand
from faststream_outbox.router import OutboxRoute
from faststream_outbox.subscriber.usecase import (
    _LAST_EXCEPTION_MAX_CHARS,
    _TRUNCATION_SUFFIX,
    OutboxSubscriber,
    _compute_backoff,
    _OutboxConfigError,
)
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
    # P7: the binding identifier is the longest derived name — "<t>_pending_idx" /
    # "<t>_timer_id_uq" (12-byte suffix), not the "outbox_" channel prefix (7). So the
    # longest valid table_name is 63 - 12 = 51 bytes.
    metadata = MetaData()
    name = "a" * 51
    t = make_outbox_table(metadata, table_name=name)
    assert t.name == name


def test_make_outbox_table_rejects_name_that_fits_channel_but_overflows_index() -> None:
    """P7: a 52-byte name fits the NOTIFY channel (7+52=59) but overflows the index name (52+12=64)."""
    metadata = MetaData()
    with pytest.raises(ValueError, match="63 bytes"):
        make_outbox_table(metadata, table_name="a" * 52)


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


def test_encode_payload_raises_on_correlation_id_conflict() -> None:
    """P2: an explicit correlation_id that mismatches headers['correlation_id'] is a conflict (was silently dropped)."""
    with pytest.raises(ValueError, match="correlation_id"):
        _encode_payload({"x": 1}, correlation_id="kwarg-id", headers={"correlation_id": "header-id"})


def test_encode_payload_correlation_id_matching_header_is_ok() -> None:
    """P2: kwarg == header is not a conflict."""
    _, headers = _encode_payload({"x": 1}, correlation_id="same", headers={"correlation_id": "same"})
    assert headers["correlation_id"] == "same"


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


def test_constant_retry_rejects_oversize_jitter() -> None:
    """P23: jitter_factor > 2 would make a jittered delay negative (a hot retry)."""
    with pytest.raises(ValueError, match="jitter_factor"):
        ConstantRetry(delay_seconds=1.0, jitter_factor=2.5)


def test_constant_retry_rejects_non_positive_delay() -> None:
    """P23: a non-positive delay is a hot retry."""
    with pytest.raises(ValueError, match="delay_seconds"):
        ConstantRetry(delay_seconds=0.0)


def test_exponential_retry_rejects_non_positive_initial_delay() -> None:
    """P23: ExponentialRetry needs a positive initial delay."""
    with pytest.raises(ValueError, match="initial_delay_seconds"):
        ExponentialRetry(initial_delay_seconds=0.0)


def test_retry_rejects_zero_max_attempts() -> None:
    """P23: max_attempts < 1 means 'never retry' expressed confusingly — reject it."""
    with pytest.raises(ValueError, match="max_attempts"):
        ConstantRetry(delay_seconds=1.0, max_attempts=0)


def test_retry_rejects_non_positive_max_total_delay() -> None:
    """P23: max_total_delay_seconds must be > 0 if set."""
    with pytest.raises(ValueError, match="max_total_delay_seconds"):
        ConstantRetry(delay_seconds=1.0, max_total_delay_seconds=0.0)


def test_linear_retry_rejects_non_positive_initial_delay() -> None:
    """P23: LinearRetry needs a positive initial delay."""
    with pytest.raises(ValueError, match="initial_delay_seconds"):
        LinearRetry(initial_delay_seconds=0.0, step_seconds=1.0)


def test_linear_retry_rejects_negative_step() -> None:
    """P23: a negative step would shrink delays toward zero (hot retry)."""
    with pytest.raises(ValueError, match="step_seconds"):
        LinearRetry(initial_delay_seconds=1.0, step_seconds=-1.0)


def test_exponential_retry_rejects_non_positive_multiplier() -> None:
    """P23: a non-positive multiplier is nonsensical for exponential backoff."""
    with pytest.raises(ValueError, match="multiplier"):
        ExponentialRetry(initial_delay_seconds=1.0, multiplier=0.0)


def test_exponential_retry_rejects_non_positive_max_delay() -> None:
    """P23: max_delay_seconds must be > 0 if set."""
    with pytest.raises(ValueError, match="max_delay_seconds"):
        ExponentialRetry(initial_delay_seconds=1.0, max_delay_seconds=0.0)


def test_warn_on_duplicate_queues_across_routers() -> None:
    """P22: a duplicate queue introduced via include_router is caught at start()-time."""
    broker = _make_broker()

    @broker.subscriber("orders")
    async def h1(body: dict) -> None: ...

    router = OutboxRouter()

    @router.subscriber("orders")  # same queue, via a router → invisible to registration-time check
    async def h2(body: dict) -> None: ...

    broker.include_router(router)
    with pytest.warns(UserWarning, match="compete for the same rows"):
        broker._warn_on_duplicate_queues()  # noqa: SLF001


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


async def test_assert_state_set_with_exception_nacks_for_retry() -> None:
    """B5: a handler that raised without acking must honor the retry strategy, not reject-delete."""
    msg = _make_msg(retry_strategy=ConstantRetry(delay_seconds=60), last_exception=RuntimeError("boom"))
    await msg.assert_state_set(logger=None)
    assert msg.state_set
    assert not msg.to_delete  # nack scheduled a retry, did not delete
    assert msg.pending_delay_seconds == 60.0
    assert msg.terminal_failure_reason is None


async def test_assert_state_set_with_exception_no_strategy_is_terminal_retry() -> None:
    """B5: handler raised, no retry strategy -> terminal via nack (retry_terminal), not rejected."""
    msg = _make_msg(last_exception=RuntimeError("boom"))
    await msg.assert_state_set(logger=None)
    assert msg.state_set
    assert msg.to_delete
    assert msg.terminal_failure_reason == "retry_terminal"


async def test_inner_message_nack_accepts_and_ignores_kwargs() -> None:
    """B6: NackMessage(delay=5) forwards delay= to nack(); we accept-and-ignore it."""
    msg = _make_msg(retry_strategy=ConstantRetry(delay_seconds=60))
    await msg.nack(delay=5)  # native-broker idiom; the kwarg must not raise
    assert msg.pending_delay_seconds == 60.0  # our strategy owns timing, not the kwarg


async def test_outbox_message_nack_accepts_and_ignores_kwargs() -> None:
    """B6: the StreamMessage wrapper the ack middleware calls must accept **options too."""
    inner = _make_msg(retry_strategy=ConstantRetry(delay_seconds=60))
    msg = OutboxMessage(
        raw_message=inner,
        body=b"",
        headers={},
        content_type=None,
        message_id="1",
        correlation_id="1",
    )
    await msg.nack(delay=5)
    assert inner.pending_delay_seconds == 60.0


class _RaisingRetryStrategy:
    """A buggy retry strategy that raises when computing the next delay."""

    def get_next_attempt_delay(self, **_kwargs: object) -> float | None:
        msg = "strategy boom"
        raise RuntimeError(msg)


async def test_nack_with_raising_strategy_degrades_to_retry_terminal() -> None:
    """B7: a strategy that raises must degrade to retry_terminal, never reject-delete."""
    msg = _make_msg(retry_strategy=_RaisingRetryStrategy())
    await msg.nack()
    assert msg.state_set
    assert msg.to_delete
    assert msg.terminal_failure_reason == "retry_terminal"


def test_exponential_retry_does_not_overflow_at_extreme_attempts() -> None:
    """B7: ExponentialRetry with no max_attempts/max_delay must not raise OverflowError."""
    strategy = ExponentialRetry(initial_delay_seconds=1.0, multiplier=2.0)
    now = _dt.datetime.now(tz=_dt.UTC)
    delay = strategy.get_next_attempt_delay(first_attempt_at=now, last_attempt_at=now, attempts_count=2000)
    assert delay is not None
    assert math.isfinite(delay)
    assert delay <= 100.0 * 365.0 * 24.0 * 60.0 * 60.0  # clamped at the absolute ceiling


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


async def test_broker_ping_times_out_on_hung_query(monkeypatch: pytest.MonkeyPatch) -> None:
    """F2-12: ping() bounds SELECT 1 with a timeout so a half-dead socket can't hang the liveness probe."""
    monkeypatch.setattr("faststream_outbox.client._PING_TIMEOUT_SECONDS", 0.01)

    class _HangConn:
        async def execute(self, *_args: object, **_kwargs: object) -> None:
            await asyncio.sleep(1.0)  # longer than the patched timeout → asyncio.timeout cancels it

    class _ConnCtx:
        async def __aenter__(self) -> "_HangConn":
            return _HangConn()

        async def __aexit__(self, *_exc: object) -> bool:
            return False

    engine = AsyncMock()
    engine.connect = _ConnCtx  # AsyncEngine.connect() returns an async CM (sync call)
    broker = _make_broker(engine)
    assert await broker.ping() is False


def test_outbox_client_rejects_over_long_table_identifier() -> None:
    """F3-02: a directly-constructed Table with an over-long name is rejected at OutboxClient construction."""
    table = Table("o" * 52, MetaData())  # "<name>_pending_idx" overflows the 63-byte identifier limit
    with pytest.raises(ValueError, match="too long"):
        OutboxClient(object(), table)  # ty: ignore[invalid-argument-type]  # validation runs before engine use


async def test_broker_stop_sets_running_false_before_stopping_subscribers() -> None:
    """F1-03: running flips False before the subscriber-stop gather, so a cancelled stop can't lie via ping()."""
    broker = _make_broker()

    async def handle(body: dict) -> None: ...

    broker.subscriber("orders")(handle)
    sub = next(iter(broker.subscribers))
    observed: dict[str, bool] = {}

    async def spy_stop() -> None:
        observed["running_during_stop"] = broker.running

    broker.running = True
    with patch.object(sub, "stop", new=spy_stop):
        await broker.stop()

    assert observed["running_during_stop"] is False  # set before the gather, not after
    assert broker.running is False


async def test_broker_stop_logs_subscriber_failure_and_completes() -> None:
    """A raising sub.stop must be logged via the helper, swallowed, and not re-raised."""
    broker = _make_broker()

    async def noop(body: dict) -> None: ...

    broker.subscriber("orders")(noop)

    async def raising_stop() -> None:
        msg = "subscriber boom"
        raise RuntimeError(msg)

    sub = next(iter(broker.subscribers))
    with (
        patch.object(sub, "stop", new=raising_stop),
        patch.object(broker, "_log_subscriber_stop_error") as log_spy,
    ):
        await broker.stop()  # must not re-raise

    log_spy.assert_called_once()
    logged_sub, logged_exc = log_spy.call_args.args
    assert logged_sub is sub
    assert isinstance(logged_exc, RuntimeError)
    assert broker.running is False


async def test_broker_log_subscriber_stop_error_emits_error_via_configured_logger() -> None:
    """``_log_subscriber_stop_error`` routes through the configured logger at ERROR."""
    broker = _make_broker()
    mock_logger = MagicMock()
    broker.config.broker_config.logger.logger.logger = mock_logger
    err = RuntimeError("boom")
    broker._log_subscriber_stop_error("sub-x", err)  # noqa: SLF001

    mock_logger.log.assert_called_once()
    args, kwargs = mock_logger.log.call_args
    assert args[0] == logging.ERROR
    assert kwargs["exc_info"] is err


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


async def test_publish_command_batch_bodies_preserves_none_body() -> None:
    """B8: batch_bodies must keep a leading/sole None body; upstream's getter drops body=None."""
    session = _make_session_mock()
    assert OutboxPublishCommand(None, b"x", queue="q", session=session).batch_bodies == (None, b"x")
    assert OutboxPublishCommand(None, queue="q", session=session).batch_bodies == (None,)
    assert OutboxPublishCommand(b"a", b"b", queue="q", session=session).batch_bodies == (b"a", b"b")


def test_publish_command_rejects_empty_queue() -> None:
    """P5: queue validation in the single-source-of-truth constructor."""
    with pytest.raises(ValueError, match="non-empty"):
        OutboxPublishCommand(b"x", queue="", session=_make_session_mock())


def test_publish_command_rejects_non_str_queue() -> None:
    """P5: a non-str queue is a TypeError, not an opaque SQL error."""
    with pytest.raises(TypeError, match="queue must be a str"):
        OutboxPublishCommand(b"x", queue=123, session=_make_session_mock())  # ty: ignore[invalid-argument-type]


def test_publish_command_rejects_oversize_queue() -> None:
    """P5: queue over the String(255) column limit is rejected up front."""
    with pytest.raises(ValueError, match="255"):
        OutboxPublishCommand(b"x", queue="q" * 256, session=_make_session_mock())


def test_publish_command_rejects_timer_id_for_batch() -> None:
    """P4: timer_id is meaningless for a batch (multiple bodies) — reject, don't silently drop."""
    with pytest.raises(ValueError, match="batch"):
        OutboxPublishCommand(b"a", b"b", queue="q", session=_make_session_mock(), timer_id="t")


def test_publish_command_rejects_correlation_id_for_batch() -> None:
    """P4: correlation_id is per-row single-publish only — reject on a batch command."""
    with pytest.raises(ValueError, match="batch"):
        OutboxPublishCommand(b"a", b"b", queue="q", session=_make_session_mock(), correlation_id="c")


async def test_broker_publish_batch_empty_still_rejects_non_async_session() -> None:
    """P1: the session-type check fires even on an empty batch (no command is built there)."""
    broker = _make_broker()
    with pytest.raises(TypeError, match="AsyncSession"):
        await broker.publish_batch(queue="orders", session=object())  # ty: ignore[invalid-argument-type]  # no bodies


async def test_producer_publish_emits_error_metric_on_encode_failure() -> None:
    """P3: an encode failure (content-type conflict) still emits the ``published`` error metric."""
    events, recorder = _events_recorder()
    metadata = MetaData()
    table = make_outbox_table(metadata)
    producer = OutboxProducer(table=table, parser=None, decoder=None, metrics_recorder=recorder)
    cmd = OutboxPublishCommand(
        {"x": 1},
        queue="orders",
        session=_make_session_mock(),
        headers={"content-type": "text/plain"},
    )
    with pytest.raises(ValueError, match="content-type"):
        await producer.publish(cmd)
    assert any(ev == "published" and tags.get("status") == "error" for ev, tags in events)


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


def test_outbox_producer_satisfies_producer_proto() -> None:
    """OutboxProducer satisfies ProducerProto: codec attribute present, no missing structural members."""
    table = make_outbox_table(MetaData())
    producer = OutboxProducer(table=table, parser=None, decoder=None)
    # Verify all ProducerProto structural members are present (Protocol is not
    # @runtime_checkable so isinstance() raises TypeError; check attrs directly).
    missing = typing.get_protocol_members(ProducerProto) - set(dir(producer))
    assert not missing, f"OutboxProducer missing ProducerProto attrs: {missing}"
    assert isinstance(producer.codec, DefaultCodec)


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


async def test_outbox_producer_publish_batch_with_none_body_inserts_row() -> None:
    """B8: a sole None body must insert one b"" row, not silently vanish (upstream drops body=None)."""
    metadata = MetaData()
    t = make_outbox_table(metadata)
    producer = OutboxProducer(table=t, parser=None, decoder=None)
    session = _make_session_mock()
    cmd = OutboxPublishCommand(None, queue="orders", session=session)
    await producer.publish_batch(cmd)
    # First execute is the multi-row INSERT; its rows arg carries one b"" payload.
    insert_rows = session.execute.call_args_list[0].args[1]
    assert len(insert_rows) == 1
    assert insert_rows[0]["payload"] == b""


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


async def test_fake_outbox_producer_publish_batch_with_none_body_inserts_row() -> None:
    """B8: a sole None body inserts one b"" row via the fake producer too."""
    broker = _make_broker()
    fake_client = FakeOutboxClient()
    producer = FakeOutboxProducer(fake_client, broker, serializer=None, run_loops=False)
    cmd = OutboxPublishCommand(None, queue="orders", session=_make_session_mock())
    await producer.publish_batch(cmd)
    assert len(fake_client.rows) == 1
    assert fake_client.rows[0].payload == b""


async def test_fake_outbox_producer_publish_batch_leading_none_inserts_all() -> None:
    """B8: (None, b"x") inserts 2 rows — the leading None is not dropped."""
    broker = _make_broker()
    fake_client = FakeOutboxClient()
    producer = FakeOutboxProducer(fake_client, broker, serializer=None, run_loops=False)
    cmd = OutboxPublishCommand(None, b"x", queue="orders", session=_make_session_mock())
    await producer.publish_batch(cmd)
    assert len(fake_client.rows) == 2
    assert {r.payload for r in fake_client.rows} == {b"", b"x"}


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
    # Alembic is probed via ``_import_checker.is_alembic_installed`` at import time;
    # simulate "not installed" by flipping the boolean the function checks.
    with (
        patch("faststream_outbox.client.is_alembic_installed", new=False),
        pytest.raises(ImportError, match=r"pip install faststream-outbox\[validate\]"),
    ):
        _validate_schema_sync(MagicMock(), t)


def test_compose_schema_mismatch_message_appends_hint_on_blind_drift() -> None:
    msg = _compose_schema_mismatch_message(
        ["missing CHECK constraint 'outbox_lease_ck' (expected '...')"],
        has_blind_drift=True,
    )
    assert msg.startswith(_SCHEMA_MISMATCH_PREFIX)
    assert _AUTOGEN_BLIND_HINT in msg
    assert "#fixing-drift-autogenerate-cant-see" in msg


def test_compose_schema_mismatch_message_omits_hint_without_blind_drift() -> None:
    msg = _compose_schema_mismatch_message(
        ["table 'outbox' missing column 'headers'"],
        has_blind_drift=False,
    )
    assert msg == _SCHEMA_MISMATCH_PREFIX + "table 'outbox' missing column 'headers'"
    assert _AUTOGEN_BLIND_HINT not in msg


def test_compose_schema_mismatch_message_joins_multiple_errors() -> None:
    msg = _compose_schema_mismatch_message(["a", "b"], has_blind_drift=False)
    assert msg == _SCHEMA_MISMATCH_PREFIX + "a; b"


# SQLAlchemy re-templates an explicitly-named CheckConstraint through a MetaData's ``ck``
# naming convention (the explicit name becomes the ``%(constraint_name)s`` token), so the live
# constraint name is NOT ``<table>_lease_ck`` — it is ``ck_<table>_<table>_lease_ck``. The probe
# must look it up under the convention-resolved name carried on the constraint object, not a
# literal suffix, or it falsely reports a correct schema as "missing CHECK constraint".
_CK_CONVENTION = {"ck": "ck_%(table_name)s_%(constraint_name)s"}
_RESOLVED_LEASE_CK_NAME = "ck_outbox_faststream_outbox_faststream_lease_ck"
_LEASE_CK_PREDICATE = "acquired_token is null = acquired_at is null"


def _mock_check_constraint_connection(rows: list[dict[str, str]]) -> MagicMock:
    """Build a connection whose ``execute(...).mappings().all()`` yields *rows* (name/definition dicts)."""
    connection = MagicMock()
    connection.execute.return_value.mappings.return_value.all.return_value = rows
    return connection


def test_validate_check_constraints_honors_naming_convention() -> None:
    metadata = MetaData(naming_convention=_CK_CONVENTION)
    table = make_outbox_table(metadata, table_name="outbox_faststream")
    connection = _mock_check_constraint_connection(
        [{"name": _RESOLVED_LEASE_CK_NAME, "definition": "CHECK (((acquired_token IS NULL) = (acquired_at IS NULL)))"}],
    )
    assert _validate_check_constraints_sync(connection, table) == []


def test_validate_check_constraints_missing_reports_convention_resolved_name() -> None:
    metadata = MetaData(naming_convention=_CK_CONVENTION)
    table = make_outbox_table(metadata, table_name="outbox_faststream")
    connection = _mock_check_constraint_connection([])  # constraint absent from the live DB
    errors = _validate_check_constraints_sync(connection, table)
    assert errors == [f"missing CHECK constraint {_RESOLVED_LEASE_CK_NAME!r} (expected '{_LEASE_CK_PREDICATE}')"]


def test_validate_check_constraints_falls_back_to_literal_name_without_constraint_object() -> None:
    """
    Fall back to the literal ``<table>_lease_ck`` when the Table carries no lease CheckConstraint.

    A hand-built/reflected ``Table`` has no convention to resolve, so the "missing" report still fires.
    """
    table = Table("bare_outbox", MetaData())  # no CheckConstraint attached
    connection = _mock_check_constraint_connection([])
    errors = _validate_check_constraints_sync(connection, table)
    assert errors == [f"missing CHECK constraint 'bare_outbox_lease_ck' (expected '{_LEASE_CK_PREDICATE}')"]


async def test_broker_ping_done_subscriber_task_is_false() -> None:

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


async def test_broker_ping_checks_router_registered_subscribers() -> None:
    """B11: a dead task on a router-registered subscriber must fail the probe."""
    metadata = MetaData()
    t = make_outbox_table(metadata)
    engine = AsyncMock()
    broker = OutboxBroker(engine, outbox_table=t)
    router = OutboxRouter()

    @router.subscriber("orders")
    async def handle(body: dict) -> None: ...

    broker.include_router(router)
    broker.config.broker_config.client.ping = AsyncMock(return_value=True)  # type: ignore[union-attr]
    # Router subscribers live on the router, not in broker._subscribers — the old
    # ping() iterated _subscribers and never saw this one.
    subs = list(broker.subscribers)
    assert subs
    assert all(s not in broker._subscribers for s in subs)  # noqa: SLF001
    done_task = MagicMock()
    done_task.done.return_value = True
    subs[0].tasks = [done_task]  # ty: ignore[unresolved-attribute]
    assert await broker.ping() is False


async def test_broker_ping_honors_timeout_when_probe_hangs() -> None:
    """B12: ping(timeout) must bound a hanging client.ping() and return False, not hang."""
    metadata = MetaData()
    t = make_outbox_table(metadata)
    engine = AsyncMock()
    broker = OutboxBroker(engine, outbox_table=t)

    async def _hang() -> bool:
        await asyncio.sleep(10)
        return True  # pragma: no cover - move_on_after cancels the sleep before this returns

    broker.config.broker_config.client.ping = _hang  # type: ignore[union-attr]
    result = await broker.ping(timeout=0.05)
    assert result is False


def test_outbox_params_storage_caches_logger() -> None:

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

    msg = _make_msg()
    logger = MagicMock()
    await msg.assert_state_set(logger=logger)
    logger.log.assert_called_once()
    assert msg.state_set


# --- OutboxRoute / specs / subscriber config ---


def test_outbox_route_constructs() -> None:

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

    assert FakeOutboxClient().table is None


async def test_fake_client_fetch_empty_queues() -> None:

    client = FakeOutboxClient()
    assert await client.fetch(None, [], limit=10, lease_ttl_seconds=60.0) == []


async def test_fake_client_delete_miss() -> None:

    client = FakeOutboxClient()
    assert await client.delete_with_lease(None, 123, uuid.uuid4()) is False


async def test_fake_client_mark_pending_miss() -> None:

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


def test_on_notify_wakes_only_for_served_queues() -> None:
    """P14: NOTIFY payload is the queue name; only wake for queues this subscriber serves."""
    sub = _make_subscriber_for_listener_test()  # serves "orders"
    sub._notify_event.clear()  # noqa: SLF001
    sub._on_notify(None, 123, "outbox_outbox", "orders")  # noqa: SLF001  # served
    assert sub._notify_event.is_set()  # noqa: SLF001

    sub._notify_event.clear()  # noqa: SLF001
    sub._on_notify(None, 123, "outbox_outbox", "other-queue")  # noqa: SLF001  # not served
    assert not sub._notify_event.is_set()  # noqa: SLF001


def test_on_notify_wakes_conservatively_on_unexpected_payload() -> None:
    """P14: an unexpected callback shape wakes (don't risk a missed delivery)."""
    sub = _make_subscriber_for_listener_test()
    sub._notify_event.clear()  # noqa: SLF001
    sub._on_notify()  # noqa: SLF001  # no args
    assert sub._notify_event.is_set()  # noqa: SLF001


async def test_stop_awaits_cancelled_tasks() -> None:
    """P16: stop() awaits the cancelled loop tasks so they've unwound before it returns."""
    sub = _make_subscriber_for_listener_test()
    sub.running = True

    async def _forever() -> None:
        await asyncio.sleep(3600)

    sub.add_task(_forever)
    await asyncio.sleep(0)  # let the task start (reach its await) before we cancel it
    tasks = list(sub.tasks)
    assert tasks
    await sub.stop()
    assert all(t.done() for t in tasks)  # gather() awaited the cancellations


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
    """``dispatch_one`` forwards ``writer_conn`` AND the row's id + lease token into ``delete_with_lease``."""
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
    # F7-06: the lease-token guard is load-bearing — assert id + acquired_token are threaded
    # from the row, not just the connection. A regression passing a stale/None token would
    # silently break the WHERE acquired_token = :token invariant.
    assert spy.await_args.args[1] == msg.id
    assert spy.await_args.args[2] == msg.acquired_token


async def test_dispatch_one_propagates_flush_error_when_writer_conn_set() -> None:
    """A flush error against a cached writer conn must propagate so _worker_loop can reconnect."""

    class RaisingFake(FakeOutboxClient):
        async def delete_with_lease(self, *args: object, **kwargs: object) -> bool:  # noqa: ARG002
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
        async def delete_with_lease(self, *args: object, **kwargs: object) -> bool:  # noqa: ARG002
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


async def test_dispatch_one_preserves_row_when_consume_early_exits_on_shutdown() -> None:
    """
    Shutdown race in dispatch_one.

    ``SubscriberUsecase.consume()`` returns ``None`` without invoking
    ``process_message`` when ``running`` is False. Previously ``dispatch_one`` fell
    into ``assert_state_set → reject() → _safe_flush`` and silently DELETEd the row.
    The early-return guard preserves the row so its lease expires and another
    replica reclaims it.
    """
    events, recorder = _events_recorder()
    metadata = MetaData()
    table = make_outbox_table(metadata)
    broker = OutboxBroker(outbox_table=table, metrics_recorder=recorder)

    @broker.subscriber("orders")
    async def handle(body: dict) -> None: ...

    fake = FakeOutboxClient()
    test_broker = TestOutboxBroker(broker)
    test_broker.fake_client = fake
    msg = _make_msg(queue="orders")

    async with test_broker:
        sub = next(iter(broker._subscribers))  # noqa: SLF001
        # Simulate the race: a worker has pulled a row from _inflight, then stop()
        # flipped running to False before dispatch_one's consume() call.
        sub.running = False
        with patch.object(fake, "delete_with_lease", new=AsyncMock(return_value=True)) as delete_spy:
            await sub.dispatch_one(msg, writer_conn=None)

    delete_spy.assert_not_awaited()
    assert not any(e == "acked" for e, _ in events)
    assert not any(e.startswith("nacked") for e, _ in events)


async def test_dispatch_one_preserves_row_when_consume_raises() -> None:
    """
    T3: a consume()-escaping exception preserves the row (no DELETE, no false ack/nack).

    ``consume()`` swallows ordinary handler exceptions, but a middleware-bypassing
    failure (or a framework bug) can escape it. ``dispatch_one`` catches that, logs,
    and **returns** — the row's state is undefined, so flushing or emitting an
    ack/nack would lie; the lease expires and another replica reclaims. Replacing
    that ``return`` with fall-through routes the row into
    ``assert_state_set → reject → DELETE`` (silent data loss). This pins the return.
    """
    events, recorder = _events_recorder()
    metadata = MetaData()
    table = make_outbox_table(metadata)
    broker = OutboxBroker(outbox_table=table, metrics_recorder=recorder)

    @broker.subscriber("orders")
    async def handle(body: dict) -> None: ...

    fake = FakeOutboxClient()
    test_broker = TestOutboxBroker(broker)
    test_broker.fake_client = fake
    msg = _make_msg(queue="orders")

    async with test_broker:
        sub = next(iter(broker._subscribers))  # noqa: SLF001
        # running stays True (not the shutdown race) — the escape itself must
        # preserve the row, so the only thing distinguishing correct-vs-mutant is
        # whether dispatch_one returns or falls through to the reject fallback.
        with (
            patch.object(sub, "consume", new=AsyncMock(side_effect=RuntimeError("middleware bypass"))),
            patch.object(fake, "delete_with_lease", new=AsyncMock(return_value=True)) as delete_spy,
        ):
            await sub.dispatch_one(msg, writer_conn=None)

    delete_spy.assert_not_awaited()
    assert not msg.state_set
    assert not msg.to_delete
    assert not any(e == "acked" for e, _ in events)
    assert not any(e.startswith("nacked") for e, _ in events)


async def test_dispatch_one_lease_lost_emits_only_lease_lost_not_acked() -> None:
    """
    P17: a lease-lost terminal flush emits ``lease_lost`` only — not a false ``acked`` that double-counts.

    Before P17 the acked/nacked metric fired before the flush, so a row whose lease was
    reclaimed (flush rowcount 0 → redelivered) was counted once here and again on redelivery.
    """
    events, recorder = _events_recorder()
    metadata = MetaData()
    table = make_outbox_table(metadata)
    broker = OutboxBroker(outbox_table=table, metrics_recorder=recorder)

    @broker.subscriber("orders")
    async def handle(body: dict) -> None: ...

    fake = FakeOutboxClient()
    test_broker = TestOutboxBroker(broker)
    test_broker.fake_client = fake
    msg = _make_msg(queue="orders")

    async with test_broker:
        sub = next(iter(broker._subscribers))  # noqa: SLF001
        # Lease lost: the delete finds no matching (id, token) row → rowcount 0.
        with patch.object(fake, "delete_with_lease", new=AsyncMock(return_value=False)):
            await sub.dispatch_one(msg, writer_conn=None)

    names = [e for e, _ in events]
    assert "lease_lost" in names
    assert "acked" not in names  # the false ack that double-counted is gone


async def test_worker_inner_swallows_config_error_without_reconnect() -> None:
    """
    P18: an _OutboxConfigError in the worker loop is logged and swallowed, not propagated.

    Letting it reach _run_with_reconnect would tear down the writer connection and back
    off (throttling unrelated rows). The worker must continue; the row's lease expires.
    """
    fake = FakeOutboxClient()
    broker, test_broker = _make_broker_for_dispatch(fake)
    async with test_broker:
        sub = next(iter(broker._subscribers))  # noqa: SLF001
        sub.running = True
        sub._inflight.put_nowait(_make_msg(queue="orders"))  # noqa: SLF001
        calls = {"n": 0}

        async def _raise_then_stop(row: object, *, writer_conn: object) -> None:
            del row, writer_conn
            calls["n"] += 1
            sub.running = False  # let the worker loop exit after this row
            msg = "bad relay chain"
            raise _OutboxConfigError(msg)

        with patch.object(sub, "dispatch_one", new=_raise_then_stop):
            await sub._worker_inner(writer_conn=None)  # noqa: SLF001  # must NOT raise

    assert calls["n"] == 1


async def test_dispatch_one_max_deliveries_emits_terminal_without_dispatched() -> None:
    """
    T7: the max_deliveries terminal emits nacked_terminal(reason=max_deliveries) with NO preceding 'dispatched'.

    The handler never runs (``allow_delivery`` short-circuits), so ``dispatched`` — which
    carries the in-process gauge's ``.inc()`` — must not fire. The Prometheus adapter tests
    hand-fed ``dispatched`` before ``nacked_terminal``, an order this real path never
    produces; that masked B9 (the gauge going negative). This pins the actual emit order.
    """
    events, recorder = _events_recorder()
    metadata = MetaData()
    table = make_outbox_table(metadata)
    broker = OutboxBroker(outbox_table=table, metrics_recorder=recorder)

    @broker.subscriber("orders", max_deliveries=1)
    async def handle(body: dict) -> None: ...

    fake = FakeOutboxClient()
    test_broker = TestOutboxBroker(broker)
    test_broker.fake_client = fake
    msg = _make_msg(queue="orders", deliveries_count=5)  # exceeds max_deliveries=1

    async with test_broker:
        sub = next(iter(broker._subscribers))  # noqa: SLF001
        with patch.object(fake, "delete_with_lease", new=AsyncMock(return_value=True)):
            await sub.dispatch_one(msg, writer_conn=None)

    names = [e for e, _ in events]
    terminal = [t for e, t in events if e == "nacked_terminal"]
    assert terminal
    assert terminal[0]["reason"] == "max_deliveries"
    assert "dispatched" not in names  # handler never ran → no gauge inc to balance
    assert "acked" not in names


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
    fake_conn.execution_options = AsyncMock(return_value=fake_conn)
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
    fake_conn.execution_options.assert_awaited_once_with(isolation_level="AUTOCOMMIT")


async def test_open_worker_resources_sets_autocommit() -> None:
    """``_open_worker_resources`` configures the writer conn as AUTOCOMMIT before yielding."""
    fake = FakeOutboxClient()
    broker, test_broker = _make_broker_for_dispatch(fake)
    fake_engine = MagicMock()
    raw_conn = MagicMock()
    autocommit_conn = MagicMock()
    raw_conn.execution_options = AsyncMock(return_value=autocommit_conn)
    fake_engine.connect.return_value.__aenter__ = AsyncMock(return_value=raw_conn)
    fake_engine.connect.return_value.__aexit__ = AsyncMock(return_value=None)

    async with test_broker:
        sub = next(iter(broker._subscribers))  # noqa: SLF001
        async with sub._open_worker_resources(fake_engine) as kwargs:  # noqa: SLF001
            yielded = kwargs["writer_conn"]

    raw_conn.execution_options.assert_awaited_once_with(isolation_level="AUTOCOMMIT")
    assert yielded is autocommit_conn  # the configured conn, not the raw one


async def test_open_worker_resources_yields_none_when_engine_is_none() -> None:
    """``_open_worker_resources(None)`` yields ``writer_conn=None`` without touching execution_options."""
    fake = FakeOutboxClient()
    broker, test_broker = _make_broker_for_dispatch(fake)

    async with test_broker:
        sub = next(iter(broker._subscribers))  # noqa: SLF001
        async with sub._open_worker_resources(None) as kwargs:  # noqa: SLF001
            assert kwargs == {"writer_conn": None}


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
    conn_a.execution_options = AsyncMock(return_value=conn_a)
    conn_b = MagicMock()
    conn_b.execution_options = AsyncMock(return_value=conn_b)
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


async def test_open_listen_connection_closes_conn_when_add_listener_fails() -> None:
    """B4: connect() succeeded but add_listener() failed → close the orphaned conn, don't leak it."""
    sub = _make_subscriber_for_listener_test()
    engine = MagicMock()
    engine.url.drivername = "postgresql+asyncpg"
    engine.dialect.create_connect_args.return_value = (
        [],
        {"host": "h", "user": "u", "password": "p", "database": "db"},
    )
    fake_conn = MagicMock()
    fake_conn.add_listener = AsyncMock(side_effect=OSError("listen rejected"))
    fake_conn.close = AsyncMock()
    with (
        patch("faststream_outbox.subscriber.usecase._asyncpg.connect", new=AsyncMock(return_value=fake_conn)),
        patch.object(OutboxSubscriber, "_notify_channel", new="outbox_orders"),
        patch.object(sub, "_log"),
    ):
        result = await sub._open_listen_connection(engine)  # noqa: SLF001
    assert result is None
    fake_conn.close.assert_awaited_once()


async def test_close_listen_connection_graceful_close_succeeds() -> None:
    """S1: a healthy graceful close is awaited; terminate() is not used."""
    sub = _make_subscriber_for_listener_test()
    conn = MagicMock()
    conn.close = AsyncMock()
    conn.terminate = MagicMock()
    await sub._close_listen_connection(conn)  # noqa: SLF001
    conn.close.assert_awaited_once()
    conn.terminate.assert_not_called()


async def test_close_listen_connection_falls_back_to_terminate_on_hang(monkeypatch: pytest.MonkeyPatch) -> None:
    """S1: a graceful close that hangs on a half-dead socket is bounded and falls back to terminate()."""
    monkeypatch.setattr("faststream_outbox.subscriber.usecase._LISTEN_CLOSE_TIMEOUT", 0.05)
    sub = _make_subscriber_for_listener_test()
    conn = MagicMock()

    async def _hang() -> None:
        await asyncio.sleep(3600)

    conn.close = _hang
    conn.terminate = MagicMock()
    await sub._close_listen_connection(conn)  # noqa: SLF001  # must return promptly, not hang
    conn.terminate.assert_called_once()


async def test_subscriber_start_resets_stopping_flag() -> None:
    """B2: start() clears _stopping so a stop()->start() cycle fetches again instead of hot-spinning."""
    sub = _make_subscriber_for_listener_test()
    sub._stopping = True  # noqa: SLF001  # simulate a completed drain
    with (
        patch("faststream._internal.endpoint.subscriber.SubscriberUsecase.start", new=AsyncMock()),
        patch.object(sub, "_post_start"),
        patch.object(sub, "add_task"),
    ):
        await sub.start()
    assert sub._stopping is False  # noqa: SLF001


async def test_fetch_reconnect_loop_exits_on_drain_without_churning() -> None:
    """B1: with _stopping set, the fetch reconnect loop must exit, not re-open connections in a tight spin."""
    fake = FakeOutboxClient()
    broker, test_broker = _make_broker_for_dispatch(fake)
    opens = {"n": 0}

    @asynccontextmanager
    async def _open(_engine: object) -> typing.AsyncIterator[dict[str, object]]:
        opens["n"] += 1  # pragma: no cover - the drain guard exits before resources open
        yield {}  # pragma: no cover

    async def _inner_returns_immediately() -> None:
        return  # pragma: no cover - inner is never entered during drain

    async with test_broker:
        sub = next(iter(broker._subscribers))  # noqa: SLF001
        sub.running = True
        sub._stopping = True  # noqa: SLF001
        # Without the halt_on_drain guard this loop re-opens resources forever and
        # asyncio.wait_for would time out.
        await asyncio.wait_for(
            sub._run_with_reconnect(  # noqa: SLF001
                name="fetch",
                open_resources=_open,
                inner=_inner_returns_immediately,
                halt_on_drain=True,
            ),
            timeout=1.0,
        )
    assert opens["n"] == 0  # never opened resources during drain


async def test_run_with_reconnect_resets_backoff_after_sustained_uptime(monkeypatch: pytest.MonkeyPatch) -> None:
    """B3: error_attempt resets when the connection was healthy longer than the reset threshold."""
    fake = FakeOutboxClient()
    broker, test_broker = _make_broker_for_dispatch(fake)
    attempts: list[int] = []

    async def _no_sleep(_delay: float) -> None: ...

    def _capture_backoff(attempt: int, ceiling: float, *, base: float = 1.0) -> float:
        del ceiling, base
        attempts.append(attempt)
        return 0.0

    monkeypatch.setattr("faststream_outbox.subscriber.usecase.anyio.sleep", _no_sleep)
    monkeypatch.setattr("faststream_outbox.subscriber.usecase._compute_backoff", _capture_backoff)
    # threshold 0 → every failure counts as "sustained uptime" → reset before each increment
    monkeypatch.setattr("faststream_outbox.subscriber.usecase._BACKOFF_RESET_THRESHOLD_SECONDS", 0.0)

    @asynccontextmanager
    async def _open(_engine: object) -> typing.AsyncIterator[dict[str, object]]:
        yield {}

    async with test_broker:
        sub = next(iter(broker._subscribers))  # noqa: SLF001
        sub.running = True
        calls = {"n": 0}

        async def _inner() -> None:
            calls["n"] += 1
            if calls["n"] >= 4:
                sub.running = False
                return
            boom = "blip"
            raise RuntimeError(boom)

        with patch.object(sub, "_log"):
            await sub._run_with_reconnect(name="t", open_resources=_open, inner=_inner)  # noqa: SLF001

    # 3 failures, each preceded by a reset → _compute_backoff always sees attempt 1
    # (the buggy lifetime-accumulating counter would produce [1, 2, 3]).
    assert attempts == [1, 1, 1]


async def test_run_with_reconnect_does_not_reset_backoff_when_open_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    """F1-06: a failing open (inner never runs) must NOT reset the backoff, even past the threshold."""
    fake = FakeOutboxClient()
    broker, test_broker = _make_broker_for_dispatch(fake)
    attempts: list[int] = []

    async def _no_sleep(_delay: float) -> None: ...

    def _capture_backoff(attempt: int, ceiling: float, *, base: float = 1.0) -> float:
        del ceiling, base
        attempts.append(attempt)
        return 0.0

    monkeypatch.setattr("faststream_outbox.subscriber.usecase.anyio.sleep", _no_sleep)
    monkeypatch.setattr("faststream_outbox.subscriber.usecase._compute_backoff", _capture_backoff)
    # threshold 0 → the OLD code (started captured before open) would treat every failed
    # open as "sustained uptime" and reset; the fix captures started only after open succeeds.
    monkeypatch.setattr("faststream_outbox.subscriber.usecase._BACKOFF_RESET_THRESHOLD_SECONDS", 0.0)

    async with test_broker:
        sub = next(iter(broker._subscribers))  # noqa: SLF001
        sub.running = True

        @asynccontextmanager
        async def _failing_open(_engine: object) -> typing.AsyncIterator[dict[str, object]]:
            if len(attempts) >= 3:  # exit cleanly after observing 3 escalating attempts
                sub.running = False
                yield {}
                return
            boom = "open failed"
            raise RuntimeError(boom)

        async def _inner() -> None: ...

        with patch.object(sub, "_log"):
            await sub._run_with_reconnect(name="t", open_resources=_failing_open, inner=_inner)  # noqa: SLF001

    # A failing open does NOT reset → attempts escalate. The pre-fix code reset each time → [1, 1, 1].
    assert attempts == [1, 2, 3]


async def test_outbox_client_delete_with_lease_uses_caller_conn() -> None:
    """``delete_with_lease`` runs its statement on the supplied conn with no explicit transaction."""
    metadata = MetaData()
    t = make_outbox_table(metadata)
    engine = MagicMock()
    client = OutboxClient(engine, t)

    fake_conn = MagicMock()
    fake_conn.begin = MagicMock()
    fake_conn.execute = AsyncMock(return_value=MagicMock(rowcount=1))

    deleted = await client.delete_with_lease(fake_conn, 42, uuid.uuid4())

    assert deleted is True
    fake_conn.execute.assert_awaited_once()
    fake_conn.begin.assert_not_called()  # autocommit writer conn — no per-row BEGIN/COMMIT
    engine.connect.assert_not_called()  # caller conn, not pool checkout
    engine.begin.assert_not_called()


async def test_outbox_client_mark_pending_with_lease_uses_caller_conn() -> None:
    """``mark_pending_with_lease`` runs its statement on the supplied conn with no explicit transaction."""
    metadata = MetaData()
    t = make_outbox_table(metadata)
    engine = MagicMock()
    client = OutboxClient(engine, t)

    fake_conn = MagicMock()
    fake_conn.begin = MagicMock()
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
    fake_conn.execute.assert_awaited_once()
    fake_conn.begin.assert_not_called()  # autocommit writer conn — no per-row BEGIN/COMMIT
    engine.connect.assert_not_called()
    engine.begin.assert_not_called()


async def test_fetch_inner_does_not_claim_during_drain() -> None:
    """
    T4: with _stopping set, _fetch_inner must claim NO rows (the drain "no new claims" invariant).

    Goes through the real loop guard. Changing it to ``while self.running:`` (dropping
    the ``_stopping`` conjunct) would keep claiming rows during drain — here the fetch
    spy must never be awaited.
    """
    fake = FakeOutboxClient()
    fake.feed(queue="orders", payload=b"x")  # a row is available to claim
    broker, test_broker = _make_broker_for_dispatch(fake)

    async with test_broker:
        sub = next(iter(broker._subscribers))  # noqa: SLF001
        sub.running = True
        sub._stopping = True  # noqa: SLF001  # drain in progress

        orig_fetch = fake.fetch

        async def _spy_fetch(
            *args: object, **kwargs: object
        ) -> object:  # pragma: no cover - only the mutant reaches fetch during drain
            sub.running = False  # ensure the mutant loop exits after one claim (no hang)
            return await orig_fetch(*args, **kwargs)  # ty: ignore[invalid-argument-type]

        fetch_mock = AsyncMock(side_effect=_spy_fetch)
        with patch.object(fake, "fetch", new=fetch_mock):
            await asyncio.wait_for(
                sub._fetch_inner(fetch_conn=None, listen_conn=None),  # noqa: SLF001
                timeout=2.0,
            )

    fetch_mock.assert_not_awaited()  # never claimed during drain
    assert sub._inflight.qsize() == 0  # noqa: SLF001


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


def test_subscriber_rejects_non_positive_min_fetch_interval() -> None:
    """P12: a non-positive min_fetch_interval would busy-poll."""
    broker = _make_broker()
    with pytest.raises(ValueError, match="min_fetch_interval must be > 0"):
        _register_subscriber(broker, min_fetch_interval=0.0)


def test_subscriber_rejects_non_positive_max_fetch_interval() -> None:
    """P12: a non-positive max_fetch_interval would busy-poll."""
    broker = _make_broker()
    with pytest.raises(ValueError, match="max_fetch_interval must be > 0"):
        _register_subscriber(broker, max_fetch_interval=0.0)


def test_subscriber_rejects_non_positive_lease_ttl() -> None:
    """P12: a non-positive lease_ttl_seconds means an instantly-expiring lease."""
    broker = _make_broker()
    with pytest.raises(ValueError, match="lease_ttl_seconds must be > 0"):
        _register_subscriber(broker, lease_ttl_seconds=0.0)


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
        # P17: the outcome metric now emits only after a successful flush, so give the
        # delete something to land on (the synthetic row isn't in the fake store).
        with patch.object(fake, "delete_with_lease", new=AsyncMock(return_value=True)):
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
    # ``_build_fake_publish_batch`` aggregates: one event per ``publish_batch`` call
    # with ``count == landed`` (mirrors the real producer's batch contract).
    assert len(pub) == 1
    assert pub[0]["count"] == 3


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

    # The producer now delegates to faststream_outbox.metrics._safe_emit, so the
    # swallow-and-DEBUG-log fires on that logger rather than the producer's own.
    with caplog.at_level(logging.DEBUG, logger="faststream_outbox.metrics"):
        row_id = await producer.publish(cmd)

    # publish completes despite the raising recorder.
    assert row_id == 99
    matching = [r for r in caplog.records if "metrics recorder raised" in r.getMessage() and r.exc_info is not None]
    assert matching, "expected DEBUG log 'metrics recorder raised' with exc_info"


# --- make_dlq_table -----------------------------------------------------------


async def test_dlq_cte_uses_schema_qualified_table_names() -> None:
    """B10: the DLQ CTE must render schema-qualified names when the tables carry a non-default schema."""
    engine = create_async_engine("postgresql+asyncpg://u:p@localhost/db")  # constructed, never connected
    try:
        metadata = MetaData(schema="app")
        outbox = make_outbox_table(metadata)
        dlq = make_dlq_table(metadata)
        client = OutboxClient(engine, outbox, dlq_table=dlq)
        stmt, _params = client._build_dlq_cte_stmt(  # noqa: SLF001
            1,
            uuid.uuid4(),
            {"failure_reason": "rejected", "last_exception": None},
        )
        sql = str(stmt)
        # The buggy quote(table.name) rendered bare "outbox" / "outbox_dlq"; format_table
        # carries the schema → "app.outbox" / "app.outbox_dlq" (B10).
        assert "DELETE FROM app.outbox " in sql
        assert "INSERT INTO app.outbox_dlq " in sql
    finally:
        await engine.dispose()


async def test_dlq_cte_quotes_adversarial_table_names() -> None:
    """F3-05: the DLQ CTE is the one raw-SQL identifier site — assert adversarial names stay quoted/escaped."""
    engine = create_async_engine("postgresql+asyncpg://u:p@localhost/db")  # constructed, never connected
    try:
        metadata = MetaData()
        outbox = make_outbox_table(metadata, table_name='ob"x')
        dlq = make_dlq_table(metadata, table_name="dlq;drop")
        client = OutboxClient(engine, outbox, dlq_table=dlq)
        stmt, _params = client._build_dlq_cte_stmt(  # noqa: SLF001
            1,
            uuid.uuid4(),
            {"failure_reason": "rejected", "last_exception": None},
        )
        sql = str(stmt)
        # The embedded double-quote is doubled (escaped) and the ';' name stays wrapped in
        # quotes — the identifier cannot break out of its quoting into injectable SQL. A
        # refactor dropping ``format_table`` for bare ``table.name`` would fail here.
        assert '"ob""x"' in sql
        assert '"dlq;drop"' in sql
    finally:
        await engine.dispose()


async def test_delete_with_lease_raises_when_dlq_payload_but_no_dlq_table() -> None:
    """P10: a dlq_payload with no dlq_table must raise, not silently degrade to a plain DELETE."""
    engine = create_async_engine("postgresql+asyncpg://u:p@localhost/db")
    try:
        metadata = MetaData()
        table = make_outbox_table(metadata)
        client = OutboxClient(engine, table)  # no dlq_table configured
        with pytest.raises(RuntimeError, match="dlq_table"):
            await client.delete_with_lease(
                MagicMock(),  # non-None conn; the guard raises before it's used
                1,
                uuid.uuid4(),
                dlq_payload={"failure_reason": "rejected", "last_exception": None},
            )
    finally:
        await engine.dispose()


async def test_fetch_cte_carries_partial_index_predicates_as_conjuncts() -> None:
    """
    T6: each OR arm of the fetch WHERE must carry its partial-index predicate as a conjunct.

    Branch A is ``acquired_token IS NULL``; Branch B is ``acquired_token IS NOT NULL AND
    acquired_at < cutoff``. The naive single-OR form (``acquired_at < cutoff`` without the
    ``IS NOT NULL`` conjunct) returns the same rows — the fake/real parity test can't tell
    them apart — but defeats Postgres' partial-index inference. This compiles the real
    statement and pins the shape.
    """
    engine = create_async_engine("postgresql+asyncpg://u:p@localhost/db")
    try:
        metadata = MetaData()
        table = make_outbox_table(metadata)
        client = OutboxClient(engine, table)
        captured: dict[str, typing.Any] = {}

        class _CapturingConn:
            def begin(self) -> object:
                @asynccontextmanager
                async def _cm() -> typing.AsyncIterator[None]:
                    yield

                return _cm()

            async def execute(self, stmt: object, _params: object) -> object:
                captured["stmt"] = stmt
                result = MagicMock()
                result.mappings.return_value.all.return_value = []
                return result

        await client.fetch(_CapturingConn(), ["orders"], limit=10, lease_ttl_seconds=60.0)  # ty: ignore[invalid-argument-type]
        sql = str(captured["stmt"].compile(dialect=postgresql.dialect())).lower()
        assert "acquired_token is null" in sql  # Branch A predicate
        assert "acquired_token is not null" in sql  # Branch B conjunct (the naive form drops this)
        # P11: queues bound as a single ANY(:queues) array (stable SQL across queue counts),
        # not an OR-of-N-equalities.
        assert "= any (" in sql  # ANY(:queues) array, not OR-of-equalities
    finally:
        await engine.dispose()


def test_make_dlq_table_columns_present() -> None:
    metadata = MetaData()
    t = make_dlq_table(metadata, table_name="my_dlq")
    expected = {
        "id",
        "original_id",
        "queue",
        "payload",
        "headers",
        "deliveries_count",
        "created_at",
        "failed_at",
        "failure_reason",
        "last_exception",
        "timer_id",
    }
    assert {c.name for c in t.columns} == expected
    assert t.name == "my_dlq"


def test_make_dlq_table_declares_queue_failed_index() -> None:
    metadata = MetaData()
    t = make_dlq_table(metadata, table_name="my_dlq")
    idx = next(idx for idx in t.indexes if idx.name == "my_dlq_queue_failed_idx")
    assert idx.unique is False
    assert [c.name for c in idx.columns] == ["queue", "failed_at"]


def test_make_dlq_table_attaches_to_metadata() -> None:
    metadata = MetaData()
    make_dlq_table(metadata, table_name="audit_dlq")
    assert "audit_dlq" in metadata.tables


def test_make_dlq_table_accepts_long_name_no_notify_check() -> None:
    """Unlike ``make_outbox_table``, the DLQ has no NOTIFY channel — long names are allowed."""
    metadata = MetaData()
    long_name = "x" * 80  # would exceed 63 bytes if a NOTIFY channel were derived
    t = make_dlq_table(metadata, table_name=long_name)
    assert t.name == long_name


# --- terminal_failure_reason wiring on OutboxInnerMessage ---------------------


async def test_terminal_failure_reason_set_on_max_deliveries() -> None:
    msg = _make_msg(deliveries_count=10)
    assert msg.allow_delivery(max_deliveries=5, logger=None) is False
    assert msg.terminal_failure_reason == "max_deliveries"


async def test_terminal_failure_reason_set_on_retry_terminal_without_strategy() -> None:
    msg = _make_msg()
    await msg.nack()
    assert msg.to_delete
    assert msg.terminal_failure_reason == "retry_terminal"


async def test_terminal_failure_reason_set_on_retry_strategy_returning_none() -> None:
    msg = _make_msg(retry_strategy=NoRetry())
    await msg.nack()
    assert msg.terminal_failure_reason == "retry_terminal"


async def test_terminal_failure_reason_unset_when_retry_scheduled() -> None:
    msg = _make_msg(retry_strategy=ConstantRetry(delay_seconds=60))
    await msg.nack()
    assert msg.pending_delay_seconds == 60.0
    assert msg.terminal_failure_reason is None


async def test_terminal_failure_reason_set_on_reject() -> None:
    msg = _make_msg()
    await msg.reject()
    assert msg.to_delete
    assert msg.terminal_failure_reason == "rejected"


async def test_terminal_failure_reason_unset_on_ack() -> None:
    msg = _make_msg()
    await msg.ack()
    assert msg.to_delete
    assert msg.terminal_failure_reason is None


# --- _flush_terminal DLQ wiring -----------------------------------------------


def _make_broker_with_dlq(recorder: typing.Any = None) -> tuple[OutboxBroker, TestOutboxBroker]:
    metadata = MetaData()
    table = make_outbox_table(metadata, table_name="outbox")
    dlq = make_dlq_table(metadata, table_name="outbox_dlq")
    kwargs: dict[str, typing.Any] = {"outbox_table": table, "dlq_table": dlq}
    if recorder is not None:
        kwargs["metrics_recorder"] = recorder
    broker = OutboxBroker(**kwargs)

    @broker.subscriber("orders", max_deliveries=1)
    async def handle(body: dict) -> None: ...

    test_broker = TestOutboxBroker(broker)
    return broker, test_broker


async def test_flush_terminal_builds_dlq_payload_when_failure_reason_set() -> None:
    """A terminal failure with ``dlq_table`` configured threads dlq_payload through."""
    broker, test_broker = _make_broker_with_dlq()
    fake = FakeOutboxClient()
    test_broker.fake_client = fake
    msg = _make_msg(id=7, queue="orders", deliveries_count=3)
    msg.terminal_failure_reason = "max_deliveries"
    msg.last_exception = RuntimeError("boom")

    with patch.object(fake, "delete_with_lease", new=AsyncMock(return_value=True)) as spy:
        async with test_broker:
            sub = next(iter(broker._subscribers))  # noqa: SLF001
            await sub._flush_terminal(msg, writer_conn=None)  # noqa: SLF001

    spy.assert_awaited_once()
    assert spy.await_args is not None
    kwargs = spy.await_args.kwargs
    assert kwargs["dlq_payload"]["failure_reason"] == "max_deliveries"
    assert "RuntimeError" in kwargs["dlq_payload"]["last_exception"]


async def test_flush_terminal_no_dlq_payload_on_ack_path() -> None:
    """Success-by-ack reaches _flush_terminal too (via to_delete) but reason stays None → no DLQ."""
    broker, test_broker = _make_broker_with_dlq()
    fake = FakeOutboxClient()
    test_broker.fake_client = fake
    msg = _make_msg(id=8, queue="orders")
    # terminal_failure_reason stays None (handler succeeded).

    with patch.object(fake, "delete_with_lease", new=AsyncMock(return_value=True)) as spy:
        async with test_broker:
            sub = next(iter(broker._subscribers))  # noqa: SLF001
            await sub._flush_terminal(msg, writer_conn=None)  # noqa: SLF001

    spy.assert_awaited_once()
    assert spy.await_args is not None
    assert spy.await_args.kwargs["dlq_payload"] is None


async def test_flush_terminal_no_dlq_payload_when_dlq_unconfigured() -> None:
    """Broker without ``dlq_table`` never builds dlq_payload, even on terminal failure."""
    fake = FakeOutboxClient()
    broker, test_broker = _make_broker_for_dispatch(fake)
    msg = _make_msg(id=9, queue="orders", deliveries_count=3)
    msg.terminal_failure_reason = "max_deliveries"

    with patch.object(fake, "delete_with_lease", new=AsyncMock(return_value=True)) as spy:
        async with test_broker:
            sub = next(iter(broker._subscribers))  # noqa: SLF001
            await sub._flush_terminal(msg, writer_conn=None)  # noqa: SLF001

    spy.assert_awaited_once()
    assert spy.await_args is not None
    assert spy.await_args.kwargs["dlq_payload"] is None


async def test_flush_terminal_emits_dlq_written_metric_after_successful_delete() -> None:
    events, recorder = _events_recorder()
    broker, test_broker = _make_broker_with_dlq(recorder)
    fake = FakeOutboxClient()
    test_broker.fake_client = fake
    msg = _make_msg(id=10, queue="orders", deliveries_count=3)
    msg.terminal_failure_reason = "retry_terminal"
    msg.last_exception = RuntimeError("boom")

    with patch.object(fake, "delete_with_lease", new=AsyncMock(return_value=True)):
        async with test_broker:
            sub = next(iter(broker._subscribers))  # noqa: SLF001
            await sub._flush_terminal(msg, writer_conn=None)  # noqa: SLF001

    dlq_events = [t for e, t in events if e == "dlq_written"]
    assert len(dlq_events) == 1
    assert dlq_events[0]["failure_reason"] == "retry_terminal"
    assert dlq_events[0]["exception_type"] == "RuntimeError"
    assert dlq_events[0]["queue"] == "orders"


async def test_flush_terminal_does_not_emit_dlq_written_on_lease_lost() -> None:
    """Lease-lost path (delete returned False) must skip the dlq_written emission."""
    events, recorder = _events_recorder()
    broker, test_broker = _make_broker_with_dlq(recorder)
    fake = FakeOutboxClient()
    test_broker.fake_client = fake
    msg = _make_msg(id=11, queue="orders", deliveries_count=3)
    msg.terminal_failure_reason = "max_deliveries"

    with patch.object(fake, "delete_with_lease", new=AsyncMock(return_value=False)):
        async with test_broker:
            sub = next(iter(broker._subscribers))  # noqa: SLF001
            await sub._flush_terminal(msg, writer_conn=None)  # noqa: SLF001

    assert not any(e == "dlq_written" for e, _ in events)
    assert [e for e, _ in events if e == "lease_lost"]


async def test_flush_terminal_dlq_payload_includes_repr_of_exception() -> None:
    """``last_exception`` is serialized via ``repr()`` — compact, type-aware."""
    broker, test_broker = _make_broker_with_dlq()
    fake = FakeOutboxClient()
    test_broker.fake_client = fake
    msg = _make_msg(id=12, queue="orders")
    msg.terminal_failure_reason = "rejected"
    msg.last_exception = ValueError("invalid payload")

    with patch.object(fake, "delete_with_lease", new=AsyncMock(return_value=True)) as spy:
        async with test_broker:
            sub = next(iter(broker._subscribers))  # noqa: SLF001
            await sub._flush_terminal(msg, writer_conn=None)  # noqa: SLF001

    assert spy.await_args is not None
    payload = spy.await_args.kwargs["dlq_payload"]
    assert payload["last_exception"] == repr(ValueError("invalid payload"))


async def test_flush_terminal_dlq_payload_last_exception_none_when_no_exc() -> None:
    """Manual ``reject()`` without an exception → DLQ row with ``last_exception=None``."""
    broker, test_broker = _make_broker_with_dlq()
    fake = FakeOutboxClient()
    test_broker.fake_client = fake
    msg = _make_msg(id=13, queue="orders")
    msg.terminal_failure_reason = "rejected"
    # last_exception is None — operator chose to drop, no exception context.

    with patch.object(fake, "delete_with_lease", new=AsyncMock(return_value=True)) as spy:
        async with test_broker:
            sub = next(iter(broker._subscribers))  # noqa: SLF001
            await sub._flush_terminal(msg, writer_conn=None)  # noqa: SLF001

    assert spy.await_args is not None
    payload = spy.await_args.kwargs["dlq_payload"]
    assert payload["failure_reason"] == "rejected"
    assert payload["last_exception"] is None


# --- last_exception truncation in DLQ payload --------------------------------


async def test_flush_terminal_dlq_payload_truncates_long_exception_repr() -> None:
    """A pathological exception with a multi-MB ``repr`` is truncated before write."""
    broker, test_broker = _make_broker_with_dlq()
    fake = FakeOutboxClient()
    test_broker.fake_client = fake
    msg = _make_msg(id=14, queue="orders")
    msg.terminal_failure_reason = "retry_terminal"
    # Build an exception whose ``repr`` is much larger than the cap.
    huge_payload = "x" * (_LAST_EXCEPTION_MAX_CHARS * 3)
    msg.last_exception = RuntimeError(huge_payload)

    with patch.object(fake, "delete_with_lease", new=AsyncMock(return_value=True)) as spy:
        async with test_broker:
            sub = next(iter(broker._subscribers))  # noqa: SLF001
            await sub._flush_terminal(msg, writer_conn=None)  # noqa: SLF001

    assert spy.await_args is not None
    rendered = spy.await_args.kwargs["dlq_payload"]["last_exception"]
    assert rendered is not None
    assert len(rendered) == _LAST_EXCEPTION_MAX_CHARS
    assert rendered.endswith(_TRUNCATION_SUFFIX)


async def test_flush_terminal_dlq_payload_short_exception_not_truncated() -> None:
    """A normal-sized exception ``repr`` passes through untouched."""
    broker, test_broker = _make_broker_with_dlq()
    fake = FakeOutboxClient()
    test_broker.fake_client = fake
    msg = _make_msg(id=15, queue="orders")
    msg.terminal_failure_reason = "retry_terminal"
    msg.last_exception = ValueError("short message")

    with patch.object(fake, "delete_with_lease", new=AsyncMock(return_value=True)) as spy:
        async with test_broker:
            sub = next(iter(broker._subscribers))  # noqa: SLF001
            await sub._flush_terminal(msg, writer_conn=None)  # noqa: SLF001

    assert spy.await_args is not None
    assert spy.await_args.kwargs["dlq_payload"]["last_exception"] == repr(ValueError("short message"))


# --- nacked_terminal metric reason on manual reject() / REJECT_ON_ERROR -----


async def test_metrics_manual_reject_without_exception_emits_nacked_terminal_rejected() -> None:
    """
    Manual ``msg.reject()`` (no exception raised) emits ``nacked_terminal(reason="rejected")``.

    Previously routed to ``acked`` because ``last_exception is None`` was checked first.
    """
    events, recorder = _events_recorder()
    metadata = MetaData()
    table = make_outbox_table(metadata)
    broker = OutboxBroker(outbox_table=table, metrics_recorder=recorder)

    @broker.subscriber("orders", ack_policy=AckPolicy.MANUAL)
    async def handle(body: dict, msg: AnnotatedOutboxMessage) -> None:
        del body
        await msg.reject()

    session = _make_session_mock()
    async with TestOutboxBroker(broker):
        await broker.publish({"x": 1}, queue="orders", session=session)

    terminals = [t for e, t in events if e == "nacked_terminal"]
    assert len(terminals) == 1
    assert terminals[0]["reason"] == "rejected"
    assert "exception_type" not in terminals[0]
    assert not any(e == "acked" for e, _ in events)


async def test_metrics_reject_on_error_terminal_emits_reason_rejected() -> None:
    """
    REJECT_ON_ERROR + handler raise emits ``reason="rejected"`` (was ``"retry_terminal"``).

    The metric branch previously hardcoded ``"retry_terminal"`` for the post-handler
    terminal path; it now reads ``terminal_failure_reason`` and includes ``exception_type``.
    """
    events, recorder = _events_recorder()
    metadata = MetaData()
    table = make_outbox_table(metadata)
    broker = OutboxBroker(outbox_table=table, metrics_recorder=recorder)

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)

        @broker.subscriber("orders", ack_policy=AckPolicy.REJECT_ON_ERROR)
        async def handle(body: dict) -> None:
            del body
            err = "explode"
            raise RuntimeError(err)

    session = _make_session_mock()
    async with TestOutboxBroker(broker):
        await broker.publish({"x": 1}, queue="orders", session=session)

    terminals = [t for e, t in events if e == "nacked_terminal"]
    assert len(terminals) == 1
    assert terminals[0]["reason"] == "rejected"
    assert terminals[0]["exception_type"] == "RuntimeError"


def test_default_retry_strategy_pins_documented_parameters() -> None:
    """The opt-out default must stay exactly as documented (CLAUDE.md / operations/checklist.md)."""
    strategy = _default_retry_strategy()
    assert isinstance(strategy, ExponentialRetry)
    assert strategy.initial_delay_seconds == 1.0
    assert strategy.multiplier == 2.0
    assert strategy.max_delay_seconds == 300.0
    assert strategy.max_attempts == 10
    assert strategy.jitter_factor == 0.2


async def test_dlq_cte_insert_columns_match_make_dlq_table() -> None:
    """
    Guard the hardcoded DLQ INSERT column list against drift from ``make_dlq_table``.

    ``_build_dlq_cte_stmt`` hardcodes the DLQ column list as an f-string with nothing
    linking it to the table definition (audit 2026-06-14). A future NOT-NULL-without-
    default column added to ``make_dlq_table`` would make every terminal DLQ write fail
    (poison rows that retry forever); a nullable one would silently drop audit data. Pin
    the two together: the CTE's INSERT columns must equal the DLQ table's columns minus
    the autoincrement ``id`` and the server-default ``failed_at`` (the two the CTE omits
    on purpose).
    """
    metadata = MetaData()
    outbox = make_outbox_table(metadata, table_name="outbox")
    dlq = make_dlq_table(metadata, table_name="outbox_dlq")
    engine = create_async_engine("postgresql+asyncpg://u:p@localhost/db")  # never connected
    try:
        client = OutboxClient(engine, outbox, dlq_table=dlq)
        stmt, _ = client._build_dlq_cte_stmt(  # noqa: SLF001
            1,
            uuid.uuid4(),
            {"failure_reason": "x", "last_exception": "y"},
        )
        sql = str(stmt)
    finally:
        await engine.dispose()

    match = re.search(r"INSERT INTO [^(]+\(([^)]+)\)", sql)
    assert match is not None, f"could not find INSERT column list in: {sql}"
    insert_cols = {c.strip() for c in match.group(1).split(",")}
    expected = {c.name for c in dlq.columns} - {"id", "failed_at"}
    assert insert_cols == expected, f"DLQ CTE INSERT columns {insert_cols} drifted from table columns {expected}"


def test_outbox_response_rejects_naive_activate_at_eagerly() -> None:
    """OutboxResponse must reject a naive activate_at at construction, not defer it to dispatch time."""
    naive = _dt.datetime(2030, 1, 1, 12, 0, 0)  # noqa: DTZ001  # deliberately tz-naive
    with pytest.raises(ValueError, match="OutboxResponse requires activate_at to be timezone-aware"):
        OutboxResponse(
            {"x": 1},
            queue="q",
            session=None,  # ty: ignore[invalid-argument-type]  # error raises before session is used
            activate_at=naive,
        )


def test_outbox_response_rejects_both_activate_args_eagerly() -> None:
    """OutboxResponse must reject activate_in + activate_at together at construction."""
    aware = _dt.datetime(2030, 1, 1, 12, 0, 0, tzinfo=_dt.UTC)
    with pytest.raises(ValueError, match="OutboxResponse accepts at most one of activate_in / activate_at"):
        OutboxResponse(
            {"x": 1},
            queue="q",
            session=None,  # ty: ignore[invalid-argument-type]  # error raises before session is used
            activate_in=_dt.timedelta(seconds=5),
            activate_at=aware,
        )


def test_outbox_response_rejects_empty_queue_eagerly() -> None:
    """F4-01: OutboxResponse must reject an empty queue at construction, not defer to dispatch."""
    with pytest.raises(ValueError, match="non-empty"):
        OutboxResponse({"x": 1}, queue="", session=_make_session_mock())


def test_outbox_response_rejects_non_str_queue_eagerly() -> None:
    """F4-01: a non-str queue is a TypeError at construction, not an opaque dispatch-time error."""
    with pytest.raises(TypeError, match="queue must be a str"):
        OutboxResponse({"x": 1}, queue=123, session=_make_session_mock())  # ty: ignore[invalid-argument-type]


def test_outbox_response_rejects_non_async_session_eagerly() -> None:
    """F4-02: OutboxResponse must reject a non-AsyncSession at construction, like broker.publish."""
    with pytest.raises(TypeError, match="AsyncSession"):
        OutboxResponse({"x": 1}, queue="q", session=object())  # ty: ignore[invalid-argument-type]


async def test_broker_publish_batch_empty_rejects_empty_queue() -> None:
    """F4-06: an empty batch validates queue the same way a non-empty one does."""
    broker = _make_broker()
    with pytest.raises(ValueError, match="non-empty"):
        await broker.publish_batch(queue="", session=_make_session_mock())  # no bodies


async def test_fetch_unprocessed_rejects_non_positive_limit() -> None:
    """F4-04: a non-positive limit raises rather than hitting SQL (limit=-1) or silently returning none (limit=0)."""
    broker = _make_broker()
    for bad in (0, -1):
        with pytest.raises(ValueError, match="limit"):
            await broker.fetch_unprocessed(session=_make_session_mock(), limit=bad)


def test_asyncapi_document_populates_channels_and_operations() -> None:
    """
    Regression: ``BrokerSpec(url=[])`` produced a structurally empty AsyncAPI document.

    Upstream's generator only emits channels/operations for brokers with a non-empty spec
    url; with ``url=[]`` the assembled doc had ``servers={} channels={} operations={}`` even
    though per-subscriber/publisher schema work was correct. ``_spec_url`` now supplies a
    placeholder when the engine is None, so the document populates.
    """
    metadata = MetaData()
    table = make_outbox_table(metadata, table_name="outbox")
    broker = OutboxBroker(outbox_table=table)

    @broker.subscriber("orders")
    async def handle(body: dict) -> None: ...  # registered for the spec; never invoked

    broker.publisher("events")

    spec = AsyncAPI(broker).to_specification().to_jsonable()  # ty: ignore[invalid-argument-type]  # BrokerUsecase invariance
    assert spec["servers"], "AsyncAPI servers must not be empty (url=[] regression)"
    channel_keys = " ".join(spec["channels"])
    assert "orders" in channel_keys, f"subscriber channel missing: {list(spec['channels'])}"
    assert "events" in channel_keys, f"publisher channel missing: {list(spec['channels'])}"
    assert spec["operations"], "AsyncAPI operations must not be empty"


def test_asyncapi_include_in_schema_false_excludes_publisher_channel() -> None:
    """A publisher with include_in_schema=False must not appear in the assembled AsyncAPI doc."""
    metadata = MetaData()
    table = make_outbox_table(metadata, table_name="outbox")
    broker = OutboxBroker(outbox_table=table)

    @broker.subscriber("orders")
    async def handle(body: dict) -> None: ...  # registered for the spec; never invoked

    broker.publisher("hidden_events", include_in_schema=False)

    spec = AsyncAPI(broker).to_specification().to_jsonable()  # ty: ignore[invalid-argument-type]  # BrokerUsecase invariance
    channel_keys = " ".join(spec["channels"])
    assert "orders" in channel_keys  # the included subscriber is present…
    assert "hidden_events" not in channel_keys  # …the excluded publisher is not


async def test_spec_url_uses_password_masked_engine_dsn_when_engine_present() -> None:
    """When wired with an engine, the AsyncAPI server url is the engine DSN with the password masked."""
    engine = create_async_engine("postgresql+asyncpg://user:supersecret@db.example:5432/outboxdb")
    try:
        metadata = MetaData()
        table = make_outbox_table(metadata, table_name="outbox")
        broker = OutboxBroker(engine, outbox_table=table)
        urls = broker.specification.url
    finally:
        await engine.dispose()

    assert urls, "spec url must be non-empty when an engine is present"
    assert "supersecret" not in urls[0], "password must be masked in the AsyncAPI server url"
    assert "db.example" in urls[0]
