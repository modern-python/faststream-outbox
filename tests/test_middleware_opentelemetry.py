"""
Tests for the native ``OutboxTelemetryMiddleware`` subclass + its provider.

End-to-end consume-scope tests drive through ``TestOutboxBroker``. Provider
unit tests exercise attribute mapping directly. We use OTel SDK's
``InMemoryMetricReader`` for assertions on meter data — same pattern as
``tests/test_metrics_opentelemetry.py``.
"""

import datetime as _dt
import typing
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


pytest.importorskip("opentelemetry")
from faststream.message import StreamMessage
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from sqlalchemy import MetaData
from sqlalchemy.ext.asyncio import AsyncSession

from faststream_outbox import OutboxBroker, TestOutboxBroker, make_outbox_table
from faststream_outbox.message import OutboxInnerMessage
from faststream_outbox.opentelemetry import (
    OutboxTelemetryMiddleware,
    OutboxTelemetrySettingsProvider,
)
from faststream_outbox.response import OutboxPublishCommand


def _make_broker(
    reader: InMemoryMetricReader,
    *,
    include_messages_counters: bool = True,
) -> OutboxBroker:
    metadata = MetaData()
    table = make_outbox_table(metadata)
    meter_provider = MeterProvider(metric_readers=[reader])
    return OutboxBroker(
        outbox_table=table,
        middlewares=[  # ty: ignore[invalid-argument-type]
            OutboxTelemetryMiddleware(
                meter_provider=meter_provider,
                tracer_provider=TracerProvider(),
                include_messages_counters=include_messages_counters,
            )
        ],
    )


def _session_mock() -> AsyncMock:
    s = AsyncMock(spec=AsyncSession)
    s.execute.return_value = MagicMock()
    s.execute.return_value.scalar.return_value = 42
    return s


def _make_inner_message(*, payload: bytes = b"hello", queue: str = "orders") -> OutboxInnerMessage:
    now = _dt.datetime.now(tz=_dt.UTC)
    return OutboxInnerMessage(
        id=7,
        queue=queue,
        payload=payload,
        headers=None,
        attempts_count=0,
        deliveries_count=1,
        created_at=now,
        next_attempt_at=now,
        first_attempt_at=None,
        last_attempt_at=None,
        acquired_at=now,
        acquired_token=uuid.uuid4(),
    )


def _instruments(reader: InMemoryMetricReader) -> dict[str, typing.Any]:
    data = reader.get_metrics_data()
    assert data is not None
    return {m.name: m.data for rm in data.resource_metrics for sm in rm.scope_metrics for m in sm.metrics}


# ----- end-to-end consume-scope (via TestOutboxBroker) -----


async def test_outbox_telemetry_middleware_records_process_duration_histogram() -> None:
    reader = InMemoryMetricReader()
    broker = _make_broker(reader)

    @broker.subscriber("orders")
    async def handle(body: dict) -> None:
        pass

    async with TestOutboxBroker(broker):
        await broker.publish({"x": 1}, queue="orders", session=_session_mock())

    instruments = _instruments(reader)
    assert "messaging.process.duration" in instruments
    points = instruments["messaging.process.duration"].data_points
    assert sum(p.count for p in points) == 1  # exactly one consume — would catch a double-record
    # Confirm the messaging.system attribute is the canonical short name.
    attrs = dict(points[0].attributes)
    assert attrs["messaging.system"] == "outbox"
    assert attrs["messaging.destination_publish.name"] == "orders"


async def test_outbox_telemetry_middleware_messages_counter_increments_when_enabled() -> None:
    reader = InMemoryMetricReader()
    broker = _make_broker(reader, include_messages_counters=True)

    @broker.subscriber("orders")
    async def handle(body: dict) -> None:
        pass

    async with TestOutboxBroker(broker):
        await broker.publish({"x": 1}, queue="orders", session=_session_mock())

    instruments = _instruments(reader)
    assert "messaging.process.messages" in instruments
    points = instruments["messaging.process.messages"].data_points
    assert sum(p.value for p in points) == 1  # exactly one message consumed


async def test_outbox_telemetry_middleware_messages_counter_absent_when_disabled() -> None:
    reader = InMemoryMetricReader()
    broker = _make_broker(reader, include_messages_counters=False)

    @broker.subscriber("orders")
    async def handle(body: dict) -> None:
        pass

    async with TestOutboxBroker(broker):
        await broker.publish({"x": 1}, queue="orders", session=_session_mock())

    instruments = _instruments(reader)
    assert "messaging.process.messages" not in instruments


# ----- provider unit tests -----


def test_outbox_telemetry_settings_provider_messaging_system() -> None:
    provider = OutboxTelemetrySettingsProvider()
    assert provider.messaging_system == "outbox"


def test_outbox_telemetry_provider_consume_attrs_from_inner_message() -> None:
    provider = OutboxTelemetrySettingsProvider()
    inner = _make_inner_message(payload=b"hello world", queue="orders")
    msg: StreamMessage[OutboxInnerMessage] = StreamMessage(inner, inner.payload, correlation_id="c-1")
    attrs = provider.get_consume_attrs_from_message(msg)
    assert attrs["messaging.system"] == "outbox"
    assert attrs["messaging.message.id"] == "7"
    assert attrs["messaging.message.conversation_id"] == "c-1"
    assert attrs["messaging.message.payload_size_bytes"] == len(b"hello world")
    assert attrs["messaging.destination_publish.name"] == "orders"


def test_outbox_telemetry_provider_consume_destination_name() -> None:
    provider = OutboxTelemetrySettingsProvider()
    inner = _make_inner_message(queue="my-queue")
    msg: StreamMessage[OutboxInnerMessage] = StreamMessage(inner, inner.payload)
    assert provider.get_consume_destination_name(msg) == "my-queue"


def test_outbox_telemetry_provider_publish_attrs_from_cmd_single() -> None:
    provider = OutboxTelemetrySettingsProvider()
    cmd = OutboxPublishCommand(
        {"x": 1},
        queue="orders",
        session=AsyncMock(spec=AsyncSession),
        correlation_id="corr-1",
    )
    attrs = provider.get_publish_attrs_from_cmd(cmd)
    assert attrs["messaging.system"] == "outbox"
    assert attrs["messaging.destination.name"] == "orders"
    assert attrs["messaging.message.conversation_id"] == "corr-1"
    # batch count should be absent for a single publish
    assert "messaging.batch.message_count" not in attrs


def test_outbox_telemetry_provider_publish_attrs_from_cmd_batch() -> None:
    provider = OutboxTelemetrySettingsProvider()
    cmd = OutboxPublishCommand(
        {"x": 1},
        {"y": 2},
        {"z": 3},
        queue="orders",
        session=AsyncMock(spec=AsyncSession),
    )
    attrs = provider.get_publish_attrs_from_cmd(cmd)
    assert attrs["messaging.batch.message_count"] == 3


def test_outbox_telemetry_provider_publish_destination_name() -> None:
    provider = OutboxTelemetrySettingsProvider()
    cmd = OutboxPublishCommand(
        {"x": 1},
        queue="dst-q",
        session=AsyncMock(spec=AsyncSession),
    )
    assert provider.get_publish_destination_name(cmd) == "dst-q"


async def test_outbox_telemetry_middleware_publish_scope_does_not_fire_under_test_broker() -> None:
    """
    ``TestOutboxBroker`` patches ``broker.publish`` directly, bypassing ``_basic_publish``.

    The middleware's ``publish_scope`` therefore must not fire. The recorder seam
    (via ``FakeOutboxProducer`` / ``_build_fake_publish``) is the publish-side
    metrics path in test mode. Negative-assert here so a future refactor that
    routes test-broker publishes through ``_basic_publish`` (and thus through
    the publish-scope middleware) trips this guardrail.
    """
    reader = InMemoryMetricReader()
    broker = _make_broker(reader)

    @broker.subscriber("orders")
    async def handle(body: dict) -> None:
        pass

    async with TestOutboxBroker(broker):
        await broker.publish({"x": 1}, queue="orders", session=_session_mock())

    instruments = _instruments(reader)
    # Publish-scope middleware would create messaging.publish.duration; under
    # the test broker the publish path bypasses _basic_publish, so the instrument
    # must be absent.
    assert "messaging.publish.duration" not in instruments


def test_outbox_telemetry_middleware_raises_friendly_error_when_extra_missing() -> None:
    """Emulating ``opentelemetry`` as not installed must surface the install-hint ImportError."""
    with (
        patch("faststream_outbox.opentelemetry.middleware.is_opentelemetry_installed", new=False),
        pytest.raises(ImportError, match=r"pip install 'faststream-outbox\[opentelemetry\]'"),
    ):
        OutboxTelemetryMiddleware()


async def test_outbox_telemetry_middleware_emits_consume_span_with_outbox_attributes() -> None:
    """The middleware owns the consume-scope span (the recorder seam can't): assert it fires."""
    reader = InMemoryMetricReader()
    span_exporter = InMemorySpanExporter()
    tracer_provider = TracerProvider()
    tracer_provider.add_span_processor(SimpleSpanProcessor(span_exporter))

    metadata = MetaData()
    table = make_outbox_table(metadata)
    meter_provider = MeterProvider(metric_readers=[reader])
    broker = OutboxBroker(
        outbox_table=table,
        middlewares=[  # ty: ignore[invalid-argument-type]
            OutboxTelemetryMiddleware(meter_provider=meter_provider, tracer_provider=tracer_provider)
        ],
    )

    @broker.subscriber("orders")
    async def handle(body: dict) -> None:
        pass

    async with TestOutboxBroker(broker):
        await broker.publish({"x": 1}, queue="orders", session=_session_mock())

    spans = span_exporter.get_finished_spans()
    outbox_spans = [sp for sp in spans if dict(sp.attributes or {}).get("messaging.system") == "outbox"]
    assert outbox_spans, f"no outbox consume span emitted; spans={[sp.name for sp in spans]}"
    attrs = dict(outbox_spans[0].attributes or {})
    assert attrs["messaging.system"] == "outbox"
    assert attrs["messaging.destination_publish.name"] == "orders"
