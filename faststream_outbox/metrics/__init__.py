"""
Metrics seam: a single callable invoked at well-defined instrumentation points.

The seam is intentionally minimal — ``Callable[[str, Mapping[str, Any]], None]`` —
so adapters live next to the library without dragging Prometheus, OpenTelemetry,
or StatsD into the core import path. Built-in adapters live in optional extras:

* ``faststream_outbox.metrics.prometheus.PrometheusRecorder`` (``[prometheus]``)
* ``faststream_outbox.metrics.opentelemetry.OpenTelemetryRecorder`` (``[opentelemetry]``)

Event vocabulary (stable, additive):

* ``fetched`` — every fetch tick. Tags: ``queue, subscriber, count``.
* ``dispatched`` — handler invoked. Tags: ``queue, subscriber, deliveries_count, size_bytes``.
* ``acked`` — handler returned cleanly. Tags include ``duration_seconds``.
* ``nacked_retried`` — handler raised, retry scheduled.
  Tags include ``duration_seconds, next_delay_seconds, exception_type``.
* ``nacked_terminal`` — terminal failure. Tags include ``reason`` (``max_deliveries`` |
  ``retry_terminal`` | ``rejected``). ``duration_seconds`` is present for the post-handler
  reasons (``retry_terminal``, ``rejected``) and absent for ``max_deliveries`` (no handler
  ran). ``exception_type`` is present when ``last_exception`` was set (post-handler raises;
  manual ``msg.reject()`` may omit it).
* ``lease_lost`` — terminal flush found a foreign lease. Tags include ``phase`` (``terminal`` | ``retry``).
  ``acked`` / ``nacked_retried`` / ``nacked_terminal`` are emitted only **after** the
  terminal/retry write lands (P17); a row whose lease was reclaimed emits ``lease_lost``
  *instead* (never a paired acked/nacked), so the row isn't counted twice when it
  redelivers.
* ``dlq_written`` — emitted from ``_flush_terminal`` after the DELETE+INSERT CTE commits.
  Fires only when ``OutboxBroker`` was constructed with ``dlq_table=...`` AND the row was
  terminal-by-failure (any ``nacked_terminal`` reason). Skipped on lease-lost. Tags:
  ``queue, subscriber, deliveries_count, failure_reason`` (same value set as
  ``nacked_terminal``'s ``reason`` tag), and ``exception_type`` when ``last_exception`` was
  set. Pair with ``nacked_terminal`` to alert on "row failed but audit didn't land"
  (``nacked_terminal`` rate > ``dlq_written`` rate).
* ``published`` — producer-side insert. Tags include ``status`` (``success`` | ``error``),
  ``count, size_bytes, duration_seconds``. No ``subscriber`` tag.
  ``count`` is **messages landed**, not publish attempts — errors and ``timer_id``
  no-ops both carry ``count=0`` (P6). ``count=0`` alone does not distinguish them:
  a successful ``timer_id`` conflict has ``status="success"`` with no ``exception_type``,
  while a failed publish has ``status="error"`` with an ``exception_type``.
  Counter-style adapters should `inc(count)` so totals
  reflect messages-on-the-wire; duration histograms record every attempt
  (including failures) so failed-publish latency stays observable.

Recorders run on the event loop and **must not block**. Synchronous
``prometheus_client.Counter.inc()`` is fine (microseconds); a blocking HTTP/StatsD
call is not. We do not wrap recorders in ``asyncio.to_thread`` — that would destroy
ordering and explode the task graph.
"""

import logging
import typing
from collections.abc import Callable, Mapping


MetricsRecorder = Callable[[str, Mapping[str, typing.Any]], None]


# Canonical broker-system identifier — stamped as ``messaging.system`` (OTel)
# and ``broker`` (Prometheus) by both the recorder-seam adapters in this package
# and the native middleware providers in ``faststream_outbox.{opentelemetry,
# prometheus}.provider``. Centralized here so dashboards see a single value
# across both seams and a rename is a one-line change.
BROKER_SYSTEM = "outbox"


_logger = logging.getLogger(__name__)


def _noop_recorder(_event: str, _tags: Mapping[str, typing.Any]) -> None:
    """Default recorder — does nothing; lets instrumentation sites call unconditionally."""


def _safe_emit(recorder: MetricsRecorder, event: str, tags: Mapping[str, typing.Any]) -> None:
    """
    Invoke ``recorder`` swallowing exceptions and logging at DEBUG.

    Shared by every call site that emits metrics from the test broker. A broken
    user-supplied recorder must never poison the dispatch path — DEBUG-level
    logging surfaces the failure to operators without flooding production logs.
    """
    try:
        recorder(event, tags)
    except Exception:  # noqa: BLE001
        _logger.log(logging.DEBUG, "metrics recorder raised", exc_info=True)


__all__ = ["BROKER_SYSTEM", "MetricsRecorder", "_noop_recorder", "_safe_emit"]
