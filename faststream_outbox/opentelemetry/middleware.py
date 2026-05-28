"""
Outbox subclass of FastStream's ``TelemetryMiddleware``.

Register via ``broker_middlewares=[...]`` to get spans + meters wrapping
``publish_scope`` and ``consume_scope`` — same registration pattern as the
upstream Kafka / Rabbit / Redis middleware. For outbox-internal events
(``fetched``, ``lease_lost``), use the ``MetricsRecorder`` seam alongside.
"""

from faststream.opentelemetry.middleware import TelemetryMiddleware
from opentelemetry.metrics import Meter, MeterProvider
from opentelemetry.trace import TracerProvider

from faststream_outbox.opentelemetry.provider import OutboxTelemetrySettingsProvider
from faststream_outbox.response import OutboxPublishCommand


class OutboxTelemetryMiddleware(TelemetryMiddleware[OutboxPublishCommand]):
    """Drop-in `TelemetryMiddleware` for the outbox broker."""

    def __init__(
        self,
        *,
        tracer_provider: TracerProvider | None = None,
        meter_provider: MeterProvider | None = None,
        meter: Meter | None = None,
        include_messages_counters: bool = True,
    ) -> None:
        super().__init__(
            settings_provider_factory=lambda _: OutboxTelemetrySettingsProvider(),
            tracer_provider=tracer_provider,
            meter_provider=meter_provider,
            meter=meter,
            include_messages_counters=include_messages_counters,
        )
