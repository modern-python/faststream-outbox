"""Internal payload encoding for ``broker.publish``."""

from typing import TYPE_CHECKING, Any

from faststream.message import gen_cor_id
from faststream.message.utils import encode_message


if TYPE_CHECKING:
    from fast_depends.library.serializer import SerializerProto


def _encode_payload(
    body: Any,
    *,
    headers: dict[str, str] | None = None,
    correlation_id: str | None = None,
    serializer: "SerializerProto | None" = None,
) -> tuple[bytes, dict[str, str]]:
    """
    Serialize *body* into ``(payload_bytes, headers_dict)`` for an outbox row.

    *body* may be ``bytes``, a pydantic model, a dataclass, a ``dict``, or any value
    FastStream's ``encode_message`` accepts. *correlation_id* is auto-generated if
    not supplied so handlers can always rely on it being present. *serializer* is
    forwarded to FastStream's ``encode_message`` — pass the broker's resolved
    ``FastDependsConfig._serializer`` so pydantic models / dataclasses encode the
    same way they do for every other FastStream broker.
    """
    payload, content_type = encode_message(body, serializer=serializer)
    out_headers: dict[str, str] = dict(headers or {})
    if content_type and "content-type" not in out_headers:
        out_headers["content-type"] = content_type
    out_headers.setdefault("correlation_id", correlation_id or gen_cor_id())
    return payload, out_headers
