"""Internal payload encoding for ``broker.publish``."""

from typing import Any

from faststream.message import gen_cor_id
from faststream.message.utils import encode_message


def _encode_payload(
    body: Any,
    *,
    headers: dict[str, str] | None = None,
    correlation_id: str | None = None,
) -> tuple[bytes, dict[str, str]]:
    """
    Serialize *body* into ``(payload_bytes, headers_dict)`` for an outbox row.

    *body* may be ``bytes``, a pydantic model, a dataclass, a ``dict``, or any value
    FastStream's ``encode_message`` accepts. *correlation_id* is auto-generated if
    not supplied so handlers can always rely on it being present.
    """
    payload, content_type = encode_message(body, serializer=None)
    out_headers: dict[str, str] = dict(headers or {})
    if content_type and "content-type" not in out_headers:
        out_headers["content-type"] = content_type
    out_headers.setdefault("correlation_id", correlation_id or gen_cor_id())
    return payload, out_headers
