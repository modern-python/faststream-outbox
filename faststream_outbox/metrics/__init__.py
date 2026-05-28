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
* ``nacked_terminal`` — terminal failure. Tags include ``reason`` (``max_deliveries`` | ``retry_terminal``);
  ``duration_seconds`` is present only for ``retry_terminal``.
* ``lease_lost`` — terminal flush found a foreign lease. Tags include ``phase`` (``terminal`` | ``retry``).
* ``published`` — producer-side insert. Tags include ``status`` (``success`` | ``error``),
  ``count, size_bytes, duration_seconds``. No ``subscriber`` tag.

Recorders run on the event loop and **must not block**. Synchronous
``prometheus_client.Counter.inc()`` is fine (microseconds); a blocking HTTP/StatsD
call is not. We do not wrap recorders in ``asyncio.to_thread`` — that would destroy
ordering and explode the task graph.
"""

import typing
from collections.abc import Callable, Mapping


MetricsRecorder = Callable[[str, Mapping[str, typing.Any]], None]


def _noop_recorder(_event: str, _tags: Mapping[str, typing.Any]) -> None:
    """Default recorder — does nothing; lets instrumentation sites call unconditionally."""


__all__ = ["MetricsRecorder", "_noop_recorder"]
