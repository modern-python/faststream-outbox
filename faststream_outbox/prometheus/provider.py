"""Map outbox row/command fields to upstream's `ConsumeAttrs` TypedDict."""

import typing

from faststream.prometheus import ConsumeAttrs, MetricsSettingsProvider

from faststream_outbox.message import OutboxInnerMessage
from faststream_outbox.metrics import BROKER_SYSTEM
from faststream_outbox.response import OutboxPublishCommand


if typing.TYPE_CHECKING:
    from faststream.message import StreamMessage


class OutboxMetricsSettingsProvider(
    MetricsSettingsProvider[OutboxInnerMessage, OutboxPublishCommand],
):
    """Settings provider for the outbox `PrometheusMiddleware` subclass."""

    __slots__ = ("messaging_system",)

    def __init__(self) -> None:
        # Canonical value — shared via the ``BROKER_SYSTEM`` constant so the
        # ``broker`` label is the same value across recorder + middleware seams.
        self.messaging_system = BROKER_SYSTEM

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
