"""
Prometheus adapter for the ``MetricsRecorder`` seam.

Drop-in shape parity with ``faststream.prometheus.PrometheusMiddleware``.

Metric names, status values (``acked, nacked, error``), histogram buckets, and
constructor argument names all mirror upstream. Label sets follow upstream's
split: consume-side metrics tag by ``[app_name, broker, handler, *custom]``;
publish-side metrics tag by ``[app_name, broker, destination, *custom]``.
``broker`` is always ``"outbox"`` — same value as ``messaging.system`` from the
OpenTelemetry adapter and the native middleware providers in
:mod:`faststream_outbox.prometheus` / :mod:`faststream_outbox.opentelemetry`.
Existing FastStream Prometheus dashboards keep working — add ``broker="outbox"``
to PromQL filters to scope queries to outbox traffic.

Usage::

    from prometheus_client import REGISTRY
    from faststream_outbox import OutboxBroker
    from faststream_outbox.metrics.prometheus import PrometheusRecorder

    broker = OutboxBroker(
        engine,
        outbox_table=table,
        metrics_recorder=PrometheusRecorder(app_name="checkout", registry=REGISTRY),
    )
"""

import typing
from collections.abc import Callable, Mapping, Sequence


try:
    from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram
except ImportError as e:  # pragma: no cover
    msg = "PrometheusRecorder requires the 'prometheus' extra: pip install 'faststream-outbox[prometheus]'"
    raise ImportError(msg) from e

from faststream._internal.constants import EMPTY


try:
    # ``DEFAULT_SIZE_BUCKETS`` is a class attribute on FastStream's MetricsContainer,
    # not a module-level constant. Re-export through the attribute access so any
    # future upstream tweak (different bucket spacing, additional buckets) flows
    # in automatically.
    from faststream.prometheus.container import MetricsContainer as _UpstreamContainer

    _UPSTREAM_SIZE_BUCKETS: tuple[float, ...] = tuple(_UpstreamContainer.DEFAULT_SIZE_BUCKETS)
except ImportError:  # pragma: no cover
    _UPSTREAM_SIZE_BUCKETS = tuple(2.0**n for n in range(4, 25))

# Mirror FastStream's PrometheusMiddleware duration histogram boundaries verbatim
# so dashboards comparing process duration across brokers use the same buckets.
_DEFAULT_DURATION_BUCKETS: tuple[float, ...] = (
    0.005,
    0.01,
    0.025,
    0.05,
    0.075,
    0.1,
    0.25,
    0.5,
    0.75,
    1.0,
    2.5,
    5.0,
    7.5,
    10.0,
    float("inf"),
)
_BROKER_LABEL = "outbox"


class PrometheusRecorder:
    """
    Drop-in Prometheus adapter for ``MetricsRecorder``.

    Args:
        registry: Prometheus collector registry. Required (no global default so
            tests stay hermetic and label collisions surface immediately).
        app_name: stamped as the ``app_name`` label. ``EMPTY`` (FastStream
            sentinel) renders as the empty string — matches upstream behaviour.
        metrics_prefix: metric name prefix. Default ``"faststream"`` — same as
            upstream so series fall onto the existing exposition.
        received_messages_size_buckets: bucket boundaries for the message-size
            histogram. Defaults to FastStream's ``DEFAULT_SIZE_BUCKETS``.
        custom_labels: extra labels appended to every metric. Same shape as
            upstream: ``dict[str, str | Callable[[Mapping], str]]``. Callable
            values receive the recorder's ``tags`` mapping and return the
            label value.

    """

    def __init__(
        self,
        *,
        registry: CollectorRegistry,
        app_name: str = EMPTY,
        metrics_prefix: str = "faststream",
        received_messages_size_buckets: Sequence[float] | None = None,
        custom_labels: dict[str, str | Callable[[Mapping[str, typing.Any]], str]] | None = None,
    ) -> None:
        self._app_name = "" if app_name is EMPTY else app_name
        self._custom_label_keys = list((custom_labels or {}).keys())
        self._custom_label_resolvers = list((custom_labels or {}).values())

        # Upstream uses different label sets for consume vs publish: consume
        # tags by ``handler`` (the subscriber), publish tags by ``destination``
        # (the queue). Mirroring that split keeps dashboards drop-in compatible
        # with what ``faststream.prometheus.PrometheusMiddleware`` produces, so
        # users registering both seams (recorder + upstream middleware) see one
        # time series per (broker, handler) / (broker, destination) instead of
        # split, mismatched series.
        consume_labels = ["app_name", "broker", "handler", *self._custom_label_keys]
        publish_labels = ["app_name", "broker", "destination", *self._custom_label_keys]
        size_buckets = received_messages_size_buckets or _UPSTREAM_SIZE_BUCKETS
        p = metrics_prefix

        # ----- Consume side (mirror upstream PrometheusMiddleware verbatim) -----
        self._received_total = Counter(
            f"{p}_received_messages_total",
            "Count of received messages (one per dispatched handler invocation).",
            consume_labels,
            registry=registry,
        )
        self._received_size = Histogram(
            f"{p}_received_messages_size_bytes",
            "Size of received message bodies.",
            consume_labels,
            buckets=tuple(size_buckets),
            registry=registry,
        )
        self._in_process = Gauge(
            f"{p}_received_messages_in_process",
            "Messages currently being processed by handlers.",
            consume_labels,
            registry=registry,
        )
        self._processed_total = Counter(
            f"{p}_received_processed_messages_total",
            "Count of processed messages by status.",
            [*consume_labels, "status"],
            registry=registry,
        )
        self._processed_duration = Histogram(
            f"{p}_received_processed_messages_duration_seconds",
            "Handler duration.",
            consume_labels,
            buckets=_DEFAULT_DURATION_BUCKETS,
            registry=registry,
        )
        self._processed_exceptions = Counter(
            f"{p}_received_processed_messages_exceptions_total",
            "Handler exceptions broken down by exception type.",
            [*consume_labels, "exception_type"],
            registry=registry,
        )

        # ----- Publish side (mirror upstream verbatim) -----
        self._published_total = Counter(
            f"{p}_published_messages_total",
            "Count of published messages by status.",
            [*publish_labels, "status"],
            registry=registry,
        )
        self._published_duration = Histogram(
            f"{p}_published_messages_duration_seconds",
            "Publish operation duration (the INSERT).",
            publish_labels,
            buckets=_DEFAULT_DURATION_BUCKETS,
            registry=registry,
        )
        self._published_exceptions = Counter(
            f"{p}_published_messages_exceptions_total",
            "Publish exceptions broken down by exception type.",
            [*publish_labels, "exception_type"],
            registry=registry,
        )

        # ----- Outbox-specific (no upstream equivalent; distinct names) -----
        # Fired from subscriber-scope sites so they use the consume label set.
        self._fetch_batches = Counter(
            f"{p}_outbox_fetch_batches_total",
            "Fetch loop batches (including empty polls).",
            [*consume_labels, "non_empty"],
            registry=registry,
        )
        self._terminal_reason = Counter(
            f"{p}_outbox_terminal_total",
            "Terminal nacks broken down by reason.",
            [*consume_labels, "reason"],
            registry=registry,
        )
        self._lease_lost = Counter(
            f"{p}_outbox_lease_lost_total",
            "Lease-token mismatches on terminal write.",
            [*consume_labels, "phase"],
            registry=registry,
        )

    def _resolve_custom_values(self, tags: Mapping[str, typing.Any]) -> tuple[str, ...]:
        return tuple(
            resolver if isinstance(resolver, str) else resolver(tags) for resolver in self._custom_label_resolvers
        )

    def _consume_values(self, tags: Mapping[str, typing.Any]) -> tuple[str, ...]:
        # ``subscriber`` may be absent (e.g. ``published`` events fired from the
        # producer): map it to the empty string so the ``handler`` label still
        # has a stable value rather than KeyError-ing the metric lookup.
        handler = tags.get("subscriber", "")
        return (self._app_name, _BROKER_LABEL, handler, *self._resolve_custom_values(tags))

    def _publish_values(self, tags: Mapping[str, typing.Any]) -> tuple[str, ...]:
        # Upstream tags publish-side metrics by ``destination`` (the queue
        # name), not ``handler`` — see ``faststream/prometheus/container.py``
        # ``published_messages_*`` definitions. Matching that schema keeps
        # series consistent when ``OutboxPrometheusMiddleware`` is registered
        # alongside this recorder.
        destination = tags.get("queue", "")
        return (self._app_name, _BROKER_LABEL, destination, *self._resolve_custom_values(tags))

    def __call__(self, event: str, tags: Mapping[str, typing.Any]) -> None:  # noqa: C901
        consume_base = self._consume_values(tags)

        if event == "fetched":
            count = tags.get("count", 0)
            self._fetch_batches.labels(*consume_base, "true" if count else "false").inc()
            return

        if event == "dispatched":
            self._received_total.labels(*consume_base).inc()
            size = tags.get("size_bytes")
            if size is not None:
                self._received_size.labels(*consume_base).observe(size)
            self._in_process.labels(*consume_base).inc()
            return

        if event in {"acked", "nacked_retried", "nacked_terminal"}:
            # Map outbox event names to upstream ``ProcessingStatus`` values.
            status = "acked" if event == "acked" else "nacked"
            self._processed_total.labels(*consume_base, status).inc()
            self._in_process.labels(*consume_base).dec()
            duration = tags.get("duration_seconds")
            if duration is not None:
                self._processed_duration.labels(*consume_base).observe(duration)
            if event == "nacked_terminal":
                self._terminal_reason.labels(*consume_base, tags["reason"]).inc()
            exc = tags.get("exception_type")
            if exc is not None:
                self._processed_exceptions.labels(*consume_base, exc).inc()
            return

        if event == "lease_lost":
            # Lease loss is an internal-error condition — surface on the
            # upstream ``error`` status counter so existing alerts on
            # ``status="error"`` catch it.
            self._processed_total.labels(*consume_base, "error").inc()
            self._lease_lost.labels(*consume_base, tags["phase"]).inc()
            return

        if event == "published":
            publish_base = self._publish_values(tags)
            status = tags.get("status", "success")
            self._published_total.labels(*publish_base, status).inc()
            duration = tags.get("duration_seconds")
            if duration is not None:
                self._published_duration.labels(*publish_base).observe(duration)
            exc = tags.get("exception_type")
            if exc is not None:
                self._published_exceptions.labels(*publish_base, exc).inc()
            return
        # Unknown event — silently ignored so future events (e.g. ``dlq_written``)
        # don't break old recorders.


__all__ = ["PrometheusRecorder"]
