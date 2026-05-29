"""Map outbox row/command fields onto OTel messaging semconv attributes."""

import typing

from faststream.opentelemetry import TelemetrySettingsProvider
from faststream.opentelemetry.consts import MESSAGING_DESTINATION_PUBLISH_NAME

from faststream_outbox.message import OutboxInnerMessage
from faststream_outbox.metrics import BROKER_SYSTEM
from faststream_outbox.response import OutboxPublishCommand


if typing.TYPE_CHECKING:
    from faststream.message import StreamMessage


# Bake semconv keys as string literals — upstream FastStream still imports the
# deprecated ``SpanAttributes`` enum from ``opentelemetry.semconv.trace``; we
# sidestep that since the wire keys are stable contract values.
_ATTR_SYSTEM = "messaging.system"
_ATTR_DEST = "messaging.destination.name"
_ATTR_MESSAGE_ID = "messaging.message.id"
_ATTR_CONVERSATION_ID = "messaging.message.conversation_id"
_ATTR_PAYLOAD_SIZE = "messaging.message.payload_size_bytes"
_ATTR_BATCH_COUNT = "messaging.batch.message_count"


class OutboxTelemetrySettingsProvider(
    TelemetrySettingsProvider[OutboxInnerMessage, OutboxPublishCommand],
):
    """Settings provider for the outbox `TelemetryMiddleware` subclass."""

    __slots__ = ("messaging_system",)

    def __init__(self) -> None:
        # Canonical value — shared with the recorder-seam adapters via the
        # ``BROKER_SYSTEM`` constant so dashboards see one ``messaging.system``
        # / ``broker`` value across both seams.
        self.messaging_system = BROKER_SYSTEM

    def get_consume_attrs_from_message(
        self,
        msg: "StreamMessage[OutboxInnerMessage]",
    ) -> dict[str, typing.Any]:
        raw = msg.raw_message
        return {
            _ATTR_SYSTEM: self.messaging_system,
            _ATTR_MESSAGE_ID: str(raw.id),
            _ATTR_CONVERSATION_ID: msg.correlation_id,
            _ATTR_PAYLOAD_SIZE: len(raw.payload),
            MESSAGING_DESTINATION_PUBLISH_NAME: raw.queue,
        }

    def get_consume_destination_name(
        self,
        msg: "StreamMessage[OutboxInnerMessage]",
    ) -> str:
        return msg.raw_message.queue

    def get_publish_attrs_from_cmd(
        self,
        cmd: OutboxPublishCommand,
    ) -> dict[str, typing.Any]:
        attrs: dict[str, typing.Any] = {
            _ATTR_SYSTEM: self.messaging_system,
            _ATTR_DEST: cmd.queue,
            _ATTR_CONVERSATION_ID: cmd.correlation_id,
        }
        # ``OutboxPublishCommand`` always inherits from ``BatchPublishCommand``;
        # only tag the batch count when it actually exceeds 1 so single-publish
        # spans stay clean.
        if len(cmd.batch_bodies) > 1:
            attrs[_ATTR_BATCH_COUNT] = len(cmd.batch_bodies)
        return attrs

    def get_publish_destination_name(self, cmd: OutboxPublishCommand) -> str:
        return cmd.queue
