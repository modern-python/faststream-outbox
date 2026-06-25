"""Tests for the native ``OutboxPrometheusMiddleware`` subclass + its provider.

End-to-end consume-scope tests drive through ``TestOutboxBroker``. One smoke
test exercises the real ``broker.publish`` path (no test broker patching) so
publish-scope middleware fires through ``_basic_publish``.
"""

import datetime as _dt
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


pytest.importorskip("prometheus_client")
from faststream.message import StreamMessage
from prometheus_client import CollectorRegistry
from sqlalchemy import MetaData
from sqlalchemy.ext.asyncio import AsyncSession

from faststream_outbox import OutboxBroker, TestOutboxBroker, make_outbox_table
from faststream_outbox.message import OutboxInnerMessage
from faststream_outbox.metrics.prometheus import PrometheusRecorder
from faststream_outbox.prometheus import (
    OutboxMetricsSettingsProvider,
    OutboxPrometheusMiddleware,
)
from faststream_outbox.response import OutboxPublishCommand


def _make_broker(reg: CollectorRegistry) -> OutboxBroker:
    metadata = MetaData()
    table = make_outbox_table(metadata)
    return OutboxBroker(
        outbox_table=table,
        middlewares=[OutboxPrometheusMiddleware(registry=reg)],  # ty: ignore[invalid-argument-type]
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


# ----- end-to-end consume-scope (via TestOutboxBroker) -----


async def test_outbox_prometheus_middleware_consume_scope_increments_received_total() -> None:
    reg = CollectorRegistry()
    broker = _make_broker(reg)

    @broker.subscriber("orders")
    async def handle(body: dict) -> None:
        pass

    async with TestOutboxBroker(broker):
        await broker.publish({"x": 1}, queue="orders", session=_session_mock())

    # Handler name is camel-cased by FastStream; assert via collect() loop.
    found_value = 0.0
    for metric in reg.collect():
        if metric.name == "faststream_received_messages":
            for sample in metric.samples:
                if sample.name.endswith("_total") and sample.labels.get("broker") == "outbox":
                    found_value += sample.value  # F7-09: sum, not max — a duplicate series would push this past 1.0
    assert found_value == 1.0


async def test_outbox_prometheus_middleware_consume_processed_status_acked() -> None:
    reg = CollectorRegistry()
    broker = _make_broker(reg)

    @broker.subscriber("orders")
    async def handle(body: dict) -> None:
        pass

    async with TestOutboxBroker(broker):
        await broker.publish({"x": 1}, queue="orders", session=_session_mock())

    acked = 0.0
    for metric in reg.collect():
        if metric.name == "faststream_received_processed_messages":
            for sample in metric.samples:
                if (
                    sample.name.endswith("_total")
                    and sample.labels.get("broker") == "outbox"
                    and sample.labels.get("status") == "acked"
                ):
                    acked += sample.value  # F7-09: sum, not max — a duplicate series would push this past 1.0
    assert acked == 1.0


async def test_outbox_prometheus_middleware_message_size_observed() -> None:
    """Size histogram should have observed the row payload bytes."""
    reg = CollectorRegistry()
    broker = _make_broker(reg)

    @broker.subscriber("orders")
    async def handle(body: dict) -> None:
        pass

    async with TestOutboxBroker(broker):
        await broker.publish({"x": 1}, queue="orders", session=_session_mock())

    observed = 0.0
    for metric in reg.collect():
        if metric.name == "faststream_received_messages_size_bytes":
            for sample in metric.samples:
                if sample.name.endswith("_count") and sample.labels.get("broker") == "outbox":
                    observed += sample.value  # F7-09: sum, not max — a duplicate series would push this past 1.0
    assert observed == 1.0


# ----- real-broker publish-scope smoke (no test broker) -----


async def test_outbox_prometheus_middleware_publish_scope_fires_via_real_broker_publish() -> None:
    """publish_scope fires end-to-end via `_basic_publish` against the real producer."""
    reg = CollectorRegistry()
    broker = _make_broker(reg)
    # No test broker — publish goes through _basic_publish → publish_scope middleware
    # → OutboxProducer.publish (which uses the mocked session).
    await broker.publish({"x": 1}, queue="orders", session=_session_mock())

    published = 0.0
    for metric in reg.collect():
        if metric.name == "faststream_published_messages":
            for sample in metric.samples:
                if (
                    sample.name.endswith("_total")
                    and sample.labels.get("broker") == "outbox"
                    and sample.labels.get("destination") == "orders"
                    and sample.labels.get("status") == "success"
                ):
                    published += sample.value  # F7-09: sum, not max — a duplicate series would push this past 1.0
    assert published == 1.0


# ----- provider unit tests -----


def test_outbox_metrics_settings_provider_messaging_system() -> None:
    provider = OutboxMetricsSettingsProvider()
    assert provider.messaging_system == "outbox"


def test_outbox_metrics_settings_provider_consume_attrs_from_message() -> None:
    provider = OutboxMetricsSettingsProvider()
    inner = _make_inner_message(payload=b"hello world", queue="orders")
    msg: StreamMessage[OutboxInnerMessage] = StreamMessage(inner, inner.payload, correlation_id="c-1")
    attrs = provider.get_consume_attrs_from_message(msg)
    assert attrs == {
        "message_size": len(b"hello world"),
        "destination_name": "orders",
        "messages_count": 1,
    }


def test_outbox_metrics_settings_provider_publish_destination_name() -> None:
    provider = OutboxMetricsSettingsProvider()
    cmd = OutboxPublishCommand(
        {"x": 1},
        queue="my-queue",
        session=AsyncMock(spec=AsyncSession),
    )
    assert provider.get_publish_destination_name_from_cmd(cmd) == "my-queue"


# ----- co-existence test: middleware + recorder both fire -----


async def test_outbox_prometheus_middleware_and_recorder_share_consume_series() -> None:
    """Middleware + recorder hitting the same label tuple share a series — no double-count split."""
    # IMPORTANT: middleware and recorder must use *separate* registries because they
    # both register identically-named metrics with the same labels — Prometheus
    # forbids duplicate names per registry. Sharing one registry would raise on
    # construction. In a real deployment users register only one; this test just
    # confirms both contracts agree on the label schema.
    middleware_reg = CollectorRegistry()
    recorder_reg = CollectorRegistry()
    metadata = MetaData()
    table = make_outbox_table(metadata)
    broker = OutboxBroker(
        outbox_table=table,
        middlewares=[OutboxPrometheusMiddleware(registry=middleware_reg)],  # ty: ignore[invalid-argument-type]
        metrics_recorder=PrometheusRecorder(registry=recorder_reg),
    )

    @broker.subscriber("orders")
    async def handle(body: dict) -> None:
        pass

    async with TestOutboxBroker(broker):
        await broker.publish({"x": 1}, queue="orders", session=_session_mock())

    # Both must report acked == 1 with the same status/broker labels.
    def _acked_from(reg: CollectorRegistry) -> float:
        return next(
            sample.value
            for metric in reg.collect()
            if metric.name == "faststream_received_processed_messages"
            for sample in metric.samples
            if sample.name.endswith("_total")
            and sample.labels.get("broker") == "outbox"
            and sample.labels.get("status") == "acked"
        )

    assert _acked_from(middleware_reg) == 1.0
    assert _acked_from(recorder_reg) == 1.0


async def test_outbox_prometheus_middleware_publish_scope_does_not_fire_under_test_broker() -> None:
    """``TestOutboxBroker`` patches ``broker.publish`` directly, bypassing ``_basic_publish``.

    The middleware's ``publish_scope`` therefore must not fire. The recorder seam
    (via ``FakeOutboxProducer`` / ``_build_fake_publish``) is the publish-side
    metrics path in test mode. Negative-assert here so a future refactor that
    routes test-broker publishes through ``_basic_publish`` (and thus through
    the publish-scope middleware) trips this guardrail instead of silently
    double-counting in production users that test through ``TestOutboxBroker``.
    """
    reg = CollectorRegistry()
    broker = _make_broker(reg)

    @broker.subscriber("orders")
    async def handle(body: dict) -> None:
        pass

    async with TestOutboxBroker(broker):
        await broker.publish({"x": 1}, queue="orders", session=_session_mock())

    # A Counter that's never been ``.labels(...).inc()``-ed produces zero samples;
    # any non-zero sample on the publish-side series means middleware fired.
    non_zero_publish_samples = [
        (sample.name, dict(sample.labels), sample.value)
        for metric in reg.collect()
        if metric.name == "faststream_published_messages"
        for sample in metric.samples
        if sample.value > 0.0
    ]
    assert non_zero_publish_samples == [], (
        f"publish_scope middleware fired under TestOutboxBroker: {non_zero_publish_samples}"
    )


def test_outbox_prometheus_middleware_raises_friendly_error_when_extra_missing() -> None:
    """Emulating ``prometheus_client`` as not installed must surface the install-hint ImportError."""
    with (
        patch("faststream_outbox.prometheus.middleware.is_prometheus_client_installed", new=False),
        pytest.raises(ImportError, match=r"pip install 'faststream-outbox\[prometheus\]'"),
    ):
        OutboxPrometheusMiddleware(registry=CollectorRegistry())
