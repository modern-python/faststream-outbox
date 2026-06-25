"""Outbox subclass of FastStream's ``TelemetryMiddleware``.

Register via ``broker_middlewares=[...]`` to get spans + meters wrapping
``publish_scope`` and ``consume_scope`` — same registration pattern as the
upstream Kafka / Rabbit / Redis middleware. For outbox-internal events
(``fetched``, ``lease_lost``), use the ``MetricsRecorder`` seam alongside.
"""

import typing

from faststream_outbox._import_checker import is_opentelemetry_installed, missing_extra_message


try:
    from faststream.opentelemetry.middleware import TelemetryMiddleware
except ImportError as _exc:  # pragma: no cover - only without the [opentelemetry] extra
    # ``faststream.opentelemetry`` imports ``opentelemetry`` at module top, and the
    # upstream class is needed at class-definition time so it can't be probe-guarded.
    # Surface the friendly message at import time instead of a raw ModuleNotFoundError
    # (B13); the __init__ probe guard below stays as defense for a falsified probe.
    raise ImportError(missing_extra_message("OutboxTelemetryMiddleware", "opentelemetry")) from _exc

from faststream_outbox.opentelemetry.provider import OutboxTelemetrySettingsProvider
from faststream_outbox.response import OutboxPublishCommand


if typing.TYPE_CHECKING:
    from opentelemetry.metrics import Meter, MeterProvider
    from opentelemetry.trace import TracerProvider


class OutboxTelemetryMiddleware(TelemetryMiddleware[OutboxPublishCommand]):
    """Drop-in `TelemetryMiddleware` for the outbox broker."""

    def __init__(
        self,
        *,
        tracer_provider: "TracerProvider | None" = None,
        meter_provider: "MeterProvider | None" = None,
        meter: "Meter | None" = None,
        include_messages_counters: bool = False,
    ) -> None:
        if not is_opentelemetry_installed:
            raise ImportError(missing_extra_message("OutboxTelemetryMiddleware", "opentelemetry"))
        super().__init__(
            settings_provider_factory=lambda _: OutboxTelemetrySettingsProvider(),
            tracer_provider=tracer_provider,
            meter_provider=meter_provider,
            meter=meter,
            include_messages_counters=include_messages_counters,
        )
