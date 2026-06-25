"""OpenTelemetry meter adapter for the ``MetricsRecorder`` seam.

Instrument names, units, attribute keys, and constructor argument names mirror
``faststream.opentelemetry.TelemetryMiddleware`` (meter side). No ``outbox``
prefix on the instruments themselves — the ``messaging.system="outbox"``
attribute disambiguates outbox traffic from Kafka/Rabbit data on the same
``messaging.process.duration`` series, matching the OTel messaging semconv.

**This adapter is meter-only — no spans.** The callable seam can't bracket a
span lifecycle. For span tracing, use the native middleware integration in
:mod:`faststream_outbox.opentelemetry` (``OutboxTelemetryMiddleware``) — it
registers via ``broker_middlewares=[...]`` and wraps ``consume_scope`` /
``publish_scope``. Both seams compose: pair this adapter (which covers
outbox-internal events like ``fetched`` and ``lease_lost`` that have no
message context) with the middleware (which covers spans + bus-scope metrics).

Usage::

    from opentelemetry import metrics
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.exporter.prometheus import PrometheusMetricReader
    from prometheus_client import start_http_server

    reader = PrometheusMetricReader()
    metrics.set_meter_provider(MeterProvider(metric_readers=[reader]))
    start_http_server(9000)

    from faststream_outbox import OutboxBroker
    from faststream_outbox.metrics.opentelemetry import OpenTelemetryRecorder

    broker = OutboxBroker(
        engine,
        outbox_table=table,
        metrics_recorder=OpenTelemetryRecorder(),
    )
"""

import typing
from collections.abc import Mapping

from faststream_outbox._import_checker import is_opentelemetry_installed
from faststream_outbox.metrics import BROKER_SYSTEM


if typing.TYPE_CHECKING:
    from opentelemetry import metrics as ot_metrics

if is_opentelemetry_installed:
    from opentelemetry import metrics as ot_metrics


# Mirror FastStream's opentelemetry/consts.py keys verbatim. We bake the keys as
# string literals rather than importing the deprecated ``SpanAttributes`` enum
# from ``semconv.trace`` (FastStream still uses it; we don't have to). The
# ``messaging.*`` keys are part of the OTel semantic conventions contract.
_ATTR_SYSTEM = "messaging.system"
_ATTR_DEST = "messaging.destination.name"
_ATTR_OPERATION = "messaging.operation"
_ATTR_ERROR_TYPE = "error.type"

# Outbox-specific extension attributes — namespaced under ``messaging.outbox.*``
# so they don't collide with stock messaging-semconv keys.
_ATTR_HANDLER = "messaging.outbox.handler"
_ATTR_STATUS = "messaging.outbox.status"  # acked | nacked | error
_ATTR_TERMINAL_REASON = "messaging.outbox.terminal_reason"
_ATTR_LEASE_PHASE = "messaging.outbox.lease_phase"
_ATTR_DLQ_REASON = "messaging.outbox.dlq_reason"


class OpenTelemetryRecorder:
    """Drop-in OpenTelemetry meter adapter for ``MetricsRecorder``.

    Args:
        meter_provider: optional. Defaults to the globally configured meter
            provider via ``opentelemetry.metrics.get_meter(__name__)``.
        meter: optional pre-built meter; takes precedence over
            ``meter_provider``.
        include_messages_counters: if True, also create the optional
            ``messaging.process.messages`` / ``messaging.publish.messages``
            counters. Default False — matches upstream behaviour.

    """

    def __init__(
        self,
        *,
        meter_provider: "ot_metrics.MeterProvider | None" = None,
        meter: "ot_metrics.Meter | None" = None,
        include_messages_counters: bool = False,
    ) -> None:
        if not is_opentelemetry_installed:
            msg = (
                "OpenTelemetryRecorder requires the 'opentelemetry' extra: "
                "pip install 'faststream-outbox[opentelemetry]'"
            )
            raise ImportError(msg)
        if meter is not None:
            chosen_meter = meter
        elif meter_provider is not None:
            chosen_meter = meter_provider.get_meter(__name__)
        else:
            chosen_meter = ot_metrics.get_meter(__name__)
        self._meter = chosen_meter
        self._include_messages_counters = include_messages_counters

        # Instrument names match TelemetryMiddleware._MetricsContainer verbatim.
        self._process_duration = self._meter.create_histogram(
            name="messaging.process.duration",
            unit="s",
            description="Measures the duration of process operation",
        )
        self._publish_duration = self._meter.create_histogram(
            name="messaging.publish.duration",
            unit="s",
            description="Measures the duration of publish operation",
        )
        self._process_messages: ot_metrics.Counter | None = None
        self._publish_messages: ot_metrics.Counter | None = None
        if include_messages_counters:
            self._process_messages = self._meter.create_counter(
                name="messaging.process.messages",
                unit="message",
                description="Measures the number of processed messages",
            )
            self._publish_messages = self._meter.create_counter(
                name="messaging.publish.messages",
                unit="message",
                description="Measures the number of published messages",
            )

        # Outbox-specific instruments (no upstream equivalent).
        self._fetch_batches = self._meter.create_counter(
            name="messaging.outbox.fetch.batches",
            unit="batch",
            description="Fetch loop batches (including empty polls)",
        )
        self._lease_lost = self._meter.create_counter(
            name="messaging.outbox.lease_lost",
            unit="event",
            description="Lease-token mismatches on terminal write",
        )
        # Pairs with the ``nacked_terminal`` event so dashboards can compare
        # "row failed terminally" against "audit landed" to detect DLQ
        # misconfiguration (schema mismatch, etc.) without silent data loss.
        self._dlq_written = self._meter.create_counter(
            name="messaging.outbox.dlq_written",
            unit="event",
            description="DLQ audit rows written by terminal flush, broken down by reason",
        )
        self._drain_timeout = self._meter.create_counter(
            name="messaging.outbox.drain_timeout",
            unit="event",
            description="Drains that exceeded graceful_timeout, abandoning in-flight rows to lease-expiry retry",
        )

    def _attrs(self, tags: Mapping[str, typing.Any], *, operation: str) -> dict[str, typing.Any]:
        attrs: dict[str, typing.Any] = {
            _ATTR_SYSTEM: BROKER_SYSTEM,
            _ATTR_DEST: tags["queue"],
            _ATTR_OPERATION: operation,
        }
        handler = tags.get("subscriber")
        if handler:
            attrs[_ATTR_HANDLER] = handler
        return attrs

    def __call__(self, event: str, tags: Mapping[str, typing.Any]) -> None:  # noqa: C901, PLR0911, PLR0912
        if event == "fetched":
            self._fetch_batches.add(1, self._attrs(tags, operation="receive"))
            return

        if event == "dispatched":
            # In-flight gauge is not modelled — upstream TelemetryMiddleware
            # omits it too. Users wanting it can compute it from process
            # counts via OTel views.
            return

        if event in {"acked", "nacked_retried", "nacked_terminal"}:
            status = "acked" if event == "acked" else "nacked"
            attrs = self._attrs(tags, operation="process")
            attrs[_ATTR_STATUS] = status
            if event == "nacked_terminal":
                attrs[_ATTR_TERMINAL_REASON] = tags["reason"]
            exc = tags.get("exception_type")
            if exc is not None:
                attrs[_ATTR_ERROR_TYPE] = exc
            duration = tags.get("duration_seconds")
            if duration is not None:
                self._process_duration.record(duration, attrs)
            if self._process_messages is not None:
                self._process_messages.add(1, attrs)
            return

        if event == "lease_lost":
            attrs = self._attrs(tags, operation="process")
            attrs[_ATTR_LEASE_PHASE] = tags["phase"]
            attrs[_ATTR_ERROR_TYPE] = "lease_lost"
            self._lease_lost.add(1, attrs)
            return

        if event == "dlq_written":
            attrs = self._attrs(tags, operation="process")
            attrs[_ATTR_DLQ_REASON] = tags["failure_reason"]
            exc = tags.get("exception_type")
            if exc is not None:
                attrs[_ATTR_ERROR_TYPE] = exc
            self._dlq_written.add(1, attrs)
            return

        if event == "drain_timeout":
            self._drain_timeout.add(1, self._attrs(tags, operation="process"))
            return

        if event == "published":
            attrs = self._attrs(tags, operation="publish")
            attrs[_ATTR_STATUS] = tags.get("status", "success")
            exc = tags.get("exception_type")
            if exc is not None:
                attrs[_ATTR_ERROR_TYPE] = exc
            duration = tags.get("duration_seconds")
            if duration is not None:
                self._publish_duration.record(duration, attrs)
            if self._publish_messages is not None:
                count = tags.get("count", 0)
                if count:
                    self._publish_messages.add(count, attrs)
            return
        # Unknown event — silently ignored (forward-compatible).


__all__ = ["OpenTelemetryRecorder"]
