"""Map outbox row/command fields to upstream's `ConsumeAttrs` TypedDict."""

import typing

from faststream.prometheus import ConsumeAttrs, MetricsSettingsProvider

from faststream_outbox.message import OutboxInnerMessage
from faststream_outbox.response import OutboxPublishCommand


if typing.TYPE_CHECKING:
    from faststream.message import StreamMessage


class OutboxMetricsSettingsProvider(
    MetricsSettingsProvider[OutboxInnerMessage, OutboxPublishCommand],
):
    """Settings provider for the outbox `PrometheusMiddleware` subclass."""

    __slots__ = ("messaging_system",)

    def __init__(self) -> None:
        # Canonical value — must match ``metrics.prometheus._BROKER_LABEL`` and
        # ``opentelemetry.provider.messaging_system`` so the ``broker`` label is
        # the same value across recorder + middleware seams.
        self.messaging_system = "outbox"

    def get_consume_attrs_from_message(
        self,
        msg: "StreamMessage[OutboxInnerMessage]",
    ) -> ConsumeAttrs:
        raw = msg.raw_message
        return {
            "message_size": len(raw.payload),
            "destination_name": raw.queue,
            # Consume is always single-row post-fetch — each ``dispatch_one``
            # call processes exactly one inflight row.
            "messages_count": 1,
        }

    def get_publish_destination_name_from_cmd(self, cmd: OutboxPublishCommand) -> str:
        return cmd.queue
