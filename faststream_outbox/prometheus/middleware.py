"""
Outbox subclass of FastStream's ``PrometheusMiddleware``.

Register via ``broker_middlewares=[...]`` for consume + publish counters and
duration histograms. Same registration pattern as ``KafkaPrometheusMiddleware``
/ ``RabbitPrometheusMiddleware``. For outbox-internal events that have no
message context (``fetched``, ``lease_lost``), pair with the recorder seam in
``faststream_outbox.metrics``.
"""

import typing
from collections.abc import Callable, Sequence

from faststream._internal.constants import EMPTY
from faststream.prometheus.middleware import PrometheusMiddleware

from faststream_outbox.message import OutboxInnerMessage
from faststream_outbox.prometheus.provider import OutboxMetricsSettingsProvider
from faststream_outbox.response import OutboxPublishCommand


if typing.TYPE_CHECKING:
    from prometheus_client import CollectorRegistry


class OutboxPrometheusMiddleware(
    PrometheusMiddleware[OutboxInnerMessage, OutboxPublishCommand],
):
    """Drop-in `PrometheusMiddleware` for the outbox broker."""

    def __init__(
        self,
        *,
        registry: "CollectorRegistry",
        app_name: str = EMPTY,
        metrics_prefix: str = "faststream",
        received_messages_size_buckets: Sequence[float] | None = None,
        custom_labels: dict[str, str | Callable[[typing.Any], str]] | None = None,
    ) -> None:
        super().__init__(
            settings_provider_factory=lambda _: OutboxMetricsSettingsProvider(),
            registry=registry,
            app_name=app_name,
            metrics_prefix=metrics_prefix,
            received_messages_size_buckets=received_messages_size_buckets,
            custom_labels=custom_labels,
        )
