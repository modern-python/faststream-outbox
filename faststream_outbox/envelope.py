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
    if "content-type" in out_headers and content_type and out_headers["content-type"] != content_type:
        msg = (
            f"headers['content-type']={out_headers['content-type']!r} conflicts with the "
            f"encoder's output ({content_type!r}). Drop content-type from headers, or pass "
            "body as bytes if you need to label the payload yourself."
        )
        raise ValueError(msg)
    if content_type:
        out_headers["content-type"] = content_type
    # An explicit ``correlation_id`` kwarg used to lose silently to a
    # ``headers["correlation_id"]`` of a different value (the kwarg was dropped).
    # Treat a genuine mismatch as a conflict (like content-type above); otherwise the
    # kwarg wins when set, falling back to the header, then a fresh id (P2).
    header_cid = out_headers.get("correlation_id")
    if correlation_id is not None and header_cid is not None and correlation_id != header_cid:
        msg = (
            f"correlation_id={correlation_id!r} conflicts with headers['correlation_id']="
            f"{header_cid!r}. Pass correlation_id one way, not both."
        )
        raise ValueError(msg)
    out_headers["correlation_id"] = correlation_id or header_cid or gen_cor_id()
    return payload, out_headers
