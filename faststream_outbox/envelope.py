"""
Encoding helper for users to call before inserting outbox rows.

This is the *only* producer-side helper the package ships. ``broker.publish()`` does
not exist; users insert rows themselves via SQLAlchemy::

    payload, headers = encode_payload(my_model, correlation_id=trace_id)
    await session.execute(insert(table).values(queue="orders", payload=payload, headers=headers))

The headers returned always contain ``correlation_id`` and (when relevant)
``content-type``, so the consumer's parser can round-trip the body through
``faststream.message.decode_message`` into the handler's annotated type.
"""

from typing import Any

from faststream.message import gen_cor_id
from faststream.message.utils import encode_message


def encode_payload(
    body: Any,
    *,
    headers: dict[str, str] | None = None,
    correlation_id: str | None = None,
) -> tuple[bytes, dict[str, str]]:
    """
    Serialize *body* into ``(payload_bytes, headers_dict)`` for insertion into the outbox.

    *body* may be ``bytes``, a pydantic model, a dataclass, a ``dict``, or any value
    FastStream's ``encode_message`` accepts. *correlation_id* is auto-generated if not
    supplied so handlers can always rely on it being present.
    """
    payload, content_type = encode_message(body, serializer=None)
    out_headers: dict[str, str] = dict(headers or {})
    if content_type and "content-type" not in out_headers:
        out_headers["content-type"] = content_type
    out_headers.setdefault("correlation_id", correlation_id or gen_cor_id())
    return payload, out_headers
