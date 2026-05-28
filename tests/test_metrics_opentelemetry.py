"""Unit tests for ``OpenTelemetryRecorder`` — drop-in adapter for the seam."""

import typing

import pytest


pytest.importorskip("opentelemetry")
from opentelemetry import metrics as ot_metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from faststream_outbox.metrics.opentelemetry import OpenTelemetryRecorder


def _reader_and_recorder(*, include_counts: bool = False) -> tuple[InMemoryMetricReader, OpenTelemetryRecorder]:
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    rec = OpenTelemetryRecorder(meter_provider=provider, include_messages_counters=include_counts)
    return reader, rec


def _collect_metrics(reader: InMemoryMetricReader) -> dict[str, typing.Any]:
    data = reader.get_metrics_data()
    out: dict[str, typing.Any] = {}
    if data is None:
        return out
    for resource_metric in data.resource_metrics:
        for scope_metric in resource_metric.scope_metrics:
            for metric in scope_metric.metrics:
                out[metric.name] = metric.data
    return out


def test_otel_fetched_records_batch_counter() -> None:
    reader, rec = _reader_and_recorder()
    rec("fetched", {"queue": "orders", "subscriber": "h", "count": 5})
    metrics = _collect_metrics(reader)
    assert "messaging.outbox.fetch.batches" in metrics
    points = metrics["messaging.outbox.fetch.batches"].data_points
    assert sum(p.value for p in points) == 1


def test_otel_dispatched_is_no_op_for_meter_only_adapter() -> None:
    reader, rec = _reader_and_recorder()
    rec("dispatched", {"queue": "q", "subscriber": "h", "deliveries_count": 1, "size_bytes": 8})
    # No process/publish instruments should report data for dispatched.
    assert _collect_metrics(reader) == {}


def test_otel_acked_records_process_duration_histogram() -> None:
    reader, rec = _reader_and_recorder()
    rec("acked", {"queue": "q", "subscriber": "h", "deliveries_count": 1, "duration_seconds": 0.02})
    metrics = _collect_metrics(reader)
    assert "messaging.process.duration" in metrics
    points = metrics["messaging.process.duration"].data_points
    # Histogram point: at least one bucket count > 0.
    assert sum(p.count for p in points) == 1


def test_otel_messages_counter_is_optional() -> None:
    reader_off, rec_off = _reader_and_recorder(include_counts=False)
    reader_on, rec_on = _reader_and_recorder(include_counts=True)
    rec_off("acked", {"queue": "q", "subscriber": "h", "duration_seconds": 0.01})
    rec_on("acked", {"queue": "q", "subscriber": "h", "duration_seconds": 0.01})
    assert "messaging.process.messages" not in _collect_metrics(reader_off)
    assert "messaging.process.messages" in _collect_metrics(reader_on)


def test_otel_nacked_terminal_carries_terminal_reason_attribute() -> None:
    reader, rec = _reader_and_recorder(include_counts=True)
    rec(
        "nacked_terminal",
        {
            "queue": "q",
            "subscriber": "h",
            "deliveries_count": 5,
            "reason": "max_deliveries",
        },
    )
    metrics = _collect_metrics(reader)
    points = metrics["messaging.process.messages"].data_points
    assert len(points) == 1
    attrs = dict(points[0].attributes)
    assert attrs["messaging.outbox.terminal_reason"] == "max_deliveries"
    assert attrs["messaging.outbox.status"] == "nacked"


def test_otel_lease_lost_increments_dedicated_counter_with_phase() -> None:
    reader, rec = _reader_and_recorder()
    rec("lease_lost", {"queue": "q", "subscriber": "h", "phase": "retry"})
    metrics = _collect_metrics(reader)
    assert "messaging.outbox.lease_lost" in metrics
    points = metrics["messaging.outbox.lease_lost"].data_points
    assert len(points) == 1
    attrs = dict(points[0].attributes)
    assert attrs["messaging.outbox.lease_phase"] == "retry"
    assert attrs["error.type"] == "lease_lost"


def test_otel_published_records_publish_duration() -> None:
    reader, rec = _reader_and_recorder(include_counts=True)
    rec(
        "published",
        {"queue": "q", "status": "success", "count": 2, "size_bytes": 64, "duration_seconds": 0.003},
    )
    metrics = _collect_metrics(reader)
    assert "messaging.publish.duration" in metrics
    assert "messaging.publish.messages" in metrics
    counter_points = metrics["messaging.publish.messages"].data_points
    assert sum(p.value for p in counter_points) == 2


def test_otel_unknown_event_is_silently_ignored() -> None:
    reader, rec = _reader_and_recorder()
    rec("dlq_written", {"queue": "q", "subscriber": "h"})  # forward-compat
    assert _collect_metrics(reader) == {}


def test_otel_meter_argument_takes_precedence_over_meter_provider() -> None:
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    explicit_meter = provider.get_meter("custom-meter")
    rec = OpenTelemetryRecorder(meter=explicit_meter)
    rec("fetched", {"queue": "q", "subscriber": "h", "count": 1})
    # Smoke-test: explicit-meter path didn't raise; data lands in the same reader.
    assert "messaging.outbox.fetch.batches" in _collect_metrics(reader)


def test_otel_default_meter_provider_path() -> None:
    # Using neither meter nor meter_provider falls back to the global provider —
    # which here is the no-op MeterProvider. Just verify the recorder constructs
    # and accepts events without raising.
    rec = OpenTelemetryRecorder()
    rec("fetched", {"queue": "q", "subscriber": "h", "count": 0})
    # Sanity: confirm the recorder pulled from the global provider (or its default).
    assert ot_metrics.get_meter_provider() is not None


def test_otel_nacked_retried_stamps_error_type_attribute() -> None:
    reader, rec = _reader_and_recorder()
    rec(
        "nacked_retried",
        {
            "queue": "q",
            "subscriber": "h",
            "deliveries_count": 1,
            "duration_seconds": 0.01,
            "next_delay_seconds": 5.0,
            "exception_type": "ValueError",
        },
    )
    metrics = _collect_metrics(reader)
    attrs = dict(metrics["messaging.process.duration"].data_points[0].attributes)
    assert attrs["error.type"] == "ValueError"


def test_otel_published_error_stamps_error_type_attribute() -> None:
    reader, rec = _reader_and_recorder()
    rec(
        "published",
        {
            "queue": "q",
            "status": "error",
            "count": 0,
            "size_bytes": 0,
            "duration_seconds": 0.001,
            "exception_type": "IntegrityError",
        },
    )
    metrics = _collect_metrics(reader)
    attrs = dict(metrics["messaging.publish.duration"].data_points[0].attributes)
    assert attrs["error.type"] == "IntegrityError"
    assert attrs["messaging.outbox.status"] == "error"
