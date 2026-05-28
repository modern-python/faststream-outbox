"""
Native OpenTelemetry integration for the outbox broker.

Mirrors upstream FastStream's ``faststream/<broker>/opentelemetry/`` directory
convention. Use this when you want spans + meters via FastStream's middleware
bus (``broker_middlewares=[...]``). For outbox-internal events that have no
message context (``fetched``, ``lease_lost``), use the recorder seam in
``faststream_outbox.metrics`` alongside.
"""

from faststream_outbox.opentelemetry.middleware import OutboxTelemetryMiddleware
from faststream_outbox.opentelemetry.provider import OutboxTelemetrySettingsProvider


__all__ = ["OutboxTelemetryMiddleware", "OutboxTelemetrySettingsProvider"]
