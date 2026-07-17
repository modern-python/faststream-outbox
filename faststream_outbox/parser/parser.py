from typing import Any

from faststream.message import decode_message

from faststream_outbox.message import (
    CONTENT_TYPE_HEADER,
    CORRELATION_ID_HEADER,
    OutboxInnerMessage,
    OutboxMessage,
)


class OutboxParser:
    async def parse_message(self, msg: OutboxInnerMessage) -> OutboxMessage:
        headers = msg.headers or {}
        return OutboxMessage(
            raw_message=msg,
            body=msg.payload,
            headers=headers,
            content_type=headers.get(CONTENT_TYPE_HEADER),
            message_id=str(msg.id),
            correlation_id=headers.get(CORRELATION_ID_HEADER, str(msg.id)),
            # Set so ``SubscriberUsecase.__get_response_publisher`` (gated on a
            # truthy ``reply_to``) wires up the response publisher when a handler
            # returns ``OutboxResponse``. The inbound queue is the semantically
            # natural reply destination; the user's ``OutboxResponse.queue`` is
            # the authoritative destination — this value is only the gate.
            reply_to=msg.queue,
        )

    async def decode_message(self, msg: OutboxMessage) -> Any:
        return decode_message(msg)
