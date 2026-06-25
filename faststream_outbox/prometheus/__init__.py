"""Native Prometheus integration for the outbox broker.

Mirrors upstream FastStream's ``faststream/<broker>/prometheus/`` directory
convention. Use this when you want consume / publish counters + duration
histograms via FastStream's middleware bus (``broker_middlewares=[...]``).
For outbox-internal events that have no message context (``fetched``,
``lease_lost``), use the recorder seam in ``faststream_outbox.metrics`` alongside.
"""

from faststream_outbox.prometheus.middleware import OutboxPrometheusMiddleware
from faststream_outbox.prometheus.provider import OutboxMetricsSettingsProvider


__all__ = ["OutboxMetricsSettingsProvider", "OutboxPrometheusMiddleware"]
