"""Unit tests for ``PrometheusRecorder`` — drop-in adapter for the seam."""

import typing
from unittest.mock import AsyncMock, patch

import pytest


pytest.importorskip("prometheus_client")
from prometheus_client import CollectorRegistry
from sqlalchemy import MetaData
from sqlalchemy.ext.asyncio import AsyncSession

from faststream_outbox import NoRetry, OutboxBroker, TestOutboxBroker, make_outbox_table
from faststream_outbox.metrics.prometheus import PrometheusRecorder


def _sample(reg: CollectorRegistry, name: str, labels: dict[str, str]) -> float | None:
    return reg.get_sample_value(name, labels)


def _base_labels(handler: str = "h") -> dict[str, str]:
    return {"app_name": "", "broker": "outbox", "handler": handler}


def _make_recorder(**kwargs: typing.Any) -> tuple[CollectorRegistry, PrometheusRecorder]:
    reg = CollectorRegistry()
    return reg, PrometheusRecorder(registry=reg, **kwargs)


def test_prometheus_recorder_constructs_with_defaults() -> None:
    _, _ = _make_recorder()


def test_prometheus_fetched_event_increments_non_empty_label() -> None:
    reg, rec = _make_recorder()
    rec("fetched", {"queue": "q", "subscriber": "h", "count": 3})
    rec("fetched", {"queue": "q", "subscriber": "h", "count": 0})
    assert _sample(reg, "faststream_outbox_fetch_batches_total", {**_base_labels(), "non_empty": "true"}) == 1.0
    assert _sample(reg, "faststream_outbox_fetch_batches_total", {**_base_labels(), "non_empty": "false"}) == 1.0


def test_prometheus_dispatched_increments_received_and_in_process_and_size() -> None:
    reg, rec = _make_recorder()
    rec("dispatched", {"queue": "q", "subscriber": "h", "deliveries_count": 1, "size_bytes": 128})
    assert _sample(reg, "faststream_received_messages_total", _base_labels()) == 1.0
    assert _sample(reg, "faststream_received_messages_in_process", _base_labels()) == 1.0
    # Histogram size bucket: ensure observation landed (count != None).
    assert _sample(reg, "faststream_received_messages_size_bytes_count", _base_labels()) == 1.0


def test_prometheus_acked_maps_to_acked_status_and_decrements_in_process() -> None:
    reg, rec = _make_recorder()
    rec("dispatched", {"queue": "q", "subscriber": "h", "deliveries_count": 1, "size_bytes": 8})
    rec("acked", {"queue": "q", "subscriber": "h", "deliveries_count": 1, "duration_seconds": 0.02})
    assert _sample(reg, "faststream_received_processed_messages_total", {**_base_labels(), "status": "acked"}) == 1.0
    assert _sample(reg, "faststream_received_messages_in_process", _base_labels()) == 0.0
    assert _sample(reg, "faststream_received_processed_messages_duration_seconds_count", _base_labels()) == 1.0


def test_prometheus_nacked_retried_maps_to_nacked_status_with_exception_type() -> None:
    reg, rec = _make_recorder()
    rec("dispatched", {"queue": "q", "subscriber": "h", "deliveries_count": 1, "size_bytes": 8})
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
    assert _sample(reg, "faststream_received_processed_messages_total", {**_base_labels(), "status": "nacked"}) == 1.0
    assert (
        _sample(
            reg,
            "faststream_received_processed_messages_exceptions_total",
            {**_base_labels(), "exception_type": "ValueError"},
        )
        == 1.0
    )


def test_prometheus_nacked_terminal_records_reason_label() -> None:
    reg, rec = _make_recorder()
    rec("dispatched", {"queue": "q", "subscriber": "h", "deliveries_count": 5, "size_bytes": 4})
    rec(
        "nacked_terminal",
        {"queue": "q", "subscriber": "h", "deliveries_count": 5, "reason": "max_deliveries"},
    )
    assert _sample(reg, "faststream_outbox_terminal_total", {**_base_labels(), "reason": "max_deliveries"}) == 1.0
    assert _sample(reg, "faststream_received_processed_messages_total", {**_base_labels(), "status": "nacked"}) == 1.0


def test_prometheus_max_deliveries_terminal_does_not_drive_in_process_negative() -> None:
    """B9: nacked_terminal(reason=max_deliveries) has no preceding dispatched/duration → gauge must not go negative."""
    reg, rec = _make_recorder()
    # No 'dispatched' — the max_deliveries path short-circuits before the handler runs.
    rec("nacked_terminal", {"queue": "q", "subscriber": "h", "deliveries_count": 6, "reason": "max_deliveries"})
    sample = _sample(reg, "faststream_received_messages_in_process", _base_labels())
    assert sample is None or sample >= 0.0  # the unconditional .dec() bug produced -1.0
    # The terminal-reason and processed counters still fire (only the gauge dec is gated).
    assert _sample(reg, "faststream_outbox_terminal_total", {**_base_labels(), "reason": "max_deliveries"}) == 1.0
    assert _sample(reg, "faststream_received_processed_messages_total", {**_base_labels(), "status": "nacked"}) == 1.0


def test_prometheus_lease_lost_increments_error_status_and_phase_counter() -> None:
    reg, rec = _make_recorder()
    rec("lease_lost", {"queue": "q", "subscriber": "h", "phase": "terminal"})
    assert _sample(reg, "faststream_received_processed_messages_total", {**_base_labels(), "status": "error"}) == 1.0
    assert _sample(reg, "faststream_outbox_lease_lost_total", {**_base_labels(), "phase": "terminal"}) == 1.0


def test_prometheus_published_event_uses_destination_label() -> None:
    reg, rec = _make_recorder()
    rec(
        "published",
        {"queue": "q", "status": "success", "count": 1, "size_bytes": 16, "duration_seconds": 0.001},
    )
    # Upstream PrometheusMiddleware tags publish-side metrics by ``destination``,
    # not ``handler``. The recorder must match so users registering both seams
    # see one consistent time series per queue.
    assert (
        _sample(
            reg,
            "faststream_published_messages_total",
            {"app_name": "", "broker": "outbox", "destination": "q", "status": "success"},
        )
        == 1.0
    )


def test_prometheus_published_error_status_total_fires_at_count_zero() -> None:
    """
    P28: a status="error" published event (count=0) must increment the error-status total.

    The old ``if count > 0`` gate left ``published_messages_total{status="error"}`` — the
    exact series dashboards alert on — permanently at zero.
    """
    reg, rec = _make_recorder()
    rec(
        "published",
        {
            "queue": "q",
            "status": "error",
            "count": 0,
            "size_bytes": 0,
            "duration_seconds": 0.001,
            "exception_type": "ValueError",
        },
    )
    assert (
        _sample(
            reg,
            "faststream_published_messages_total",
            {"app_name": "", "broker": "outbox", "destination": "q", "status": "error"},
        )
        == 1.0
    )


def test_prometheus_published_duration_uses_destination_label() -> None:
    reg, rec = _make_recorder()
    rec(
        "published",
        {"queue": "orders", "status": "success", "count": 1, "size_bytes": 8, "duration_seconds": 0.001},
    )
    assert (
        _sample(
            reg,
            "faststream_published_messages_duration_seconds_count",
            {"app_name": "", "broker": "outbox", "destination": "orders"},
        )
        == 1.0
    )


def test_prometheus_published_exception_uses_destination_label() -> None:
    reg, rec = _make_recorder()
    rec(
        "published",
        {
            "queue": "orders",
            "status": "error",
            "count": 0,
            "size_bytes": 0,
            "duration_seconds": 0.001,
            "exception_type": "IntegrityError",
        },
    )
    assert (
        _sample(
            reg,
            "faststream_published_messages_exceptions_total",
            {"app_name": "", "broker": "outbox", "destination": "orders", "exception_type": "IntegrityError"},
        )
        == 1.0
    )


def test_prometheus_unknown_event_is_silently_ignored() -> None:
    _, rec = _make_recorder()
    rec("future_event_not_yet_added", {"queue": "q", "subscriber": "h"})  # forward-compat


def test_prometheus_dlq_written_records_reason_label() -> None:
    reg, rec = _make_recorder()
    rec(
        "dlq_written",
        {
            "queue": "q",
            "subscriber": "h",
            "deliveries_count": 3,
            "failure_reason": "retry_terminal",
            "exception_type": "RuntimeError",
        },
    )
    assert _sample(reg, "faststream_outbox_dlq_written_total", {**_base_labels(), "reason": "retry_terminal"}) == 1.0


def test_prometheus_dlq_written_supports_all_failure_reasons() -> None:
    reg, rec = _make_recorder()
    for reason in ("max_deliveries", "retry_terminal", "rejected"):
        rec("dlq_written", {"queue": "q", "subscriber": "h", "failure_reason": reason})
    for reason in ("max_deliveries", "retry_terminal", "rejected"):
        assert _sample(reg, "faststream_outbox_dlq_written_total", {**_base_labels(), "reason": reason}) == 1.0


def test_prometheus_app_name_label_is_applied() -> None:
    reg, rec = _make_recorder(app_name="checkout")
    rec("fetched", {"queue": "q", "subscriber": "h", "count": 1})
    assert (
        _sample(
            reg,
            "faststream_outbox_fetch_batches_total",
            {"app_name": "checkout", "broker": "outbox", "handler": "h", "non_empty": "true"},
        )
        == 1.0
    )


def test_prometheus_custom_label_value_resolved_per_event() -> None:
    reg, rec = _make_recorder(custom_labels={"env": "prod", "tenant": lambda tags: str(tags.get("queue", "x"))})
    rec("fetched", {"queue": "ordersA", "subscriber": "h", "count": 2})
    assert (
        _sample(
            reg,
            "faststream_outbox_fetch_batches_total",
            {
                "app_name": "",
                "broker": "outbox",
                "handler": "h",
                "env": "prod",
                "tenant": "ordersA",
                "non_empty": "true",
            },
        )
        == 1.0
    )


def test_prometheus_custom_metrics_prefix_renames_series() -> None:
    reg, rec = _make_recorder(metrics_prefix="my_app")
    rec("fetched", {"queue": "q", "subscriber": "h", "count": 1})
    # The default prefix metric must NOT exist.
    assert _sample(reg, "faststream_outbox_fetch_batches_total", {**_base_labels(), "non_empty": "true"}) is None
    assert _sample(reg, "my_app_outbox_fetch_batches_total", {**_base_labels(), "non_empty": "true"}) == 1.0


# ----- end-to-end recorder coverage (handler raises → nacked events flow) -----
# These tests prove the contract between subscriber emission sites and the
# Prometheus adapter end-to-end. Unit tests above hand-craft tag dicts and would
# not catch a rename like ``exception_type`` → ``exc_type`` in the emission code.


def _e2e_session() -> AsyncMock:
    return AsyncMock(spec=AsyncSession)


async def test_prometheus_e2e_handler_raises_emits_nacked_retried_with_exception_type() -> None:
    """Default retry strategy → handler raise schedules a retry → nacked_retried event."""
    reg = CollectorRegistry()
    metadata = MetaData()
    table = make_outbox_table(metadata)
    broker = OutboxBroker(outbox_table=table, metrics_recorder=PrometheusRecorder(registry=reg))

    @broker.subscriber("orders")
    async def handle(body: dict) -> None:
        del body
        msg = "boom"
        raise ValueError(msg)

    async with TestOutboxBroker(broker):
        await broker.publish({"x": 1}, queue="orders", session=_e2e_session())

    # The subscriber's `call_name` is "handle". Status must be "nacked".
    assert (
        _sample(
            reg,
            "faststream_received_processed_messages_total",
            {**_base_labels("Handle"), "status": "nacked"},
        )
        == 1.0
    )
    assert (
        _sample(
            reg,
            "faststream_received_processed_messages_exceptions_total",
            {**_base_labels("Handle"), "exception_type": "ValueError"},
        )
        == 1.0
    )


async def test_prometheus_e2e_handler_raises_with_noretry_emits_nacked_terminal_with_reason() -> None:
    """NoRetry strategy → handler raise terminates → nacked_terminal(reason="retry_terminal")."""
    reg = CollectorRegistry()
    metadata = MetaData()
    table = make_outbox_table(metadata)
    broker = OutboxBroker(outbox_table=table, metrics_recorder=PrometheusRecorder(registry=reg))

    @broker.subscriber("orders", retry_strategy=NoRetry())
    async def handle(body: dict) -> None:
        del body
        msg = "boom"
        raise RuntimeError(msg)

    async with TestOutboxBroker(broker):
        await broker.publish({"x": 1}, queue="orders", session=_e2e_session())

    assert (
        _sample(
            reg,
            "faststream_outbox_terminal_total",
            {**_base_labels("Handle"), "reason": "retry_terminal"},
        )
        == 1.0
    )
    # Terminal nacked path also bumps the upstream ``status="nacked"`` counter.
    assert (
        _sample(
            reg,
            "faststream_received_processed_messages_total",
            {**_base_labels("Handle"), "status": "nacked"},
        )
        == 1.0
    )


def test_prometheus_recorder_raises_friendly_error_when_extra_missing() -> None:
    """Emulating ``prometheus_client`` as not installed must surface the install-hint ImportError."""
    with (
        patch("faststream_outbox.metrics.prometheus.is_prometheus_client_installed", new=False),
        pytest.raises(ImportError, match=r"pip install 'faststream-outbox\[prometheus\]'"),
    ):
        PrometheusRecorder(registry=CollectorRegistry())
