"""``OutboxProducer``: the canonical publish path for outbox rows.

Both ``OutboxBroker.publish`` / ``publish_batch`` and ``OutboxPublisher.publish``
route through this producer via FastStream's ``_basic_publish(cmd, producer=...)``
middleware stack, so encode → insert → ``published`` semantics live in one place. The
insert itself (and its NOTIFY) belongs to the broker's client. The producer never opens
its own session: every command carries the caller's ``AsyncSession`` so the row commits
atomically with the caller's domain writes.
"""

import time
import typing

from faststream._internal.endpoint.utils import ParserComposition
from faststream._internal.parser import DefaultCodec
from sqlalchemy import Table

from faststream_outbox.client import _OutboxInserter
from faststream_outbox.envelope import _encode_payload
from faststream_outbox.metrics import MetricsRecorder, _noop_recorder, _safe_emit
from faststream_outbox.parser.parser import OutboxParser
from faststream_outbox.response import _REQUEST_UNSUPPORTED_MSG, OutboxPublishCommand


if typing.TYPE_CHECKING:
    from collections.abc import Mapping

    from fast_depends.library.serializer import SerializerProto
    from faststream._internal.parser import CodecProto
    from faststream._internal.types import AsyncCallable, CustomCallable

    from faststream_outbox.client import AbstractOutboxClient
    from faststream_outbox.configs import OutboxBrokerConfig


class OutboxProducer:
    """``ProducerProto[OutboxPublishCommand]``: encodes, inserts through the broker's client, emits ``published``."""

    _parser: "AsyncCallable"
    _decoder: "AsyncCallable"

    def __init__(
        self,
        *,
        table: Table,
        parser: typing.Optional["CustomCallable"],
        decoder: typing.Optional["CustomCallable"],
        metrics_recorder: MetricsRecorder = _noop_recorder,
    ) -> None:
        self.serializer: SerializerProto | None = None
        self.broker_config: OutboxBrokerConfig | None = None
        # ProducerProto[0.7] requires a `codec` attribute. The outbox owns its
        # own encoding pipeline (_encode_payload) and never reads this attribute
        # at runtime — it exists solely to satisfy the protocol.
        self.codec: CodecProto = DefaultCodec()
        default = OutboxParser()
        self._parser = ParserComposition(parser, default.parse_message)
        self._decoder = ParserComposition(decoder, default.decode_message)
        self._metrics_recorder = metrics_recorder
        self._engineless_inserter = _OutboxInserter(table)

    def _client(self) -> "AbstractOutboxClient | _OutboxInserter":
        client = None if self.broker_config is None else self.broker_config.client
        return self._engineless_inserter if client is None else client

    def _emit_metric(self, event: str, tags: "Mapping[str, typing.Any]") -> None:
        # Delegate to the shared swallow-and-DEBUG-log helper so the recorder-isolation
        # contract lives in exactly one place (the subscriber's _emit_metric is the one
        # deliberate exception — it routes through self._log for handler-scoped context).
        _safe_emit(self._metrics_recorder, event, tags)

    async def publish(self, cmd: OutboxPublishCommand) -> int | None:
        start_perf = time.perf_counter()
        size_bytes = 0
        try:
            # P3: encode inside the try so a serialization / content-type / correlation_id
            # failure still emits the ``published`` error metric instead of bypassing it.
            payload, hdrs = _encode_payload(
                cmd.body,
                headers=cmd.headers,
                correlation_id=cmd.correlation_id,
                serializer=self.serializer,
            )
            size_bytes = len(payload)
            row_id = await self._client().insert(
                cmd.session,
                queue=cmd.queue,
                payload=payload,
                headers=hdrs,
                activate_in=cmd.activate_in,
                activate_at=cmd.activate_at,
                timer_id=cmd.timer_id,
            )
        except Exception as exc:
            self._emit_metric(
                "published",
                {
                    "queue": cmd.queue,
                    "status": "error",
                    "count": 0,
                    "size_bytes": size_bytes,
                    "duration_seconds": time.perf_counter() - start_perf,
                    "exception_type": type(exc).__name__,
                },
            )
            raise
        self._emit_metric(
            "published",
            {
                "queue": cmd.queue,
                "status": "success",
                # row_id is None on timer_id conflict (no row landed); count reflects what was inserted.
                "count": 0 if row_id is None else 1,
                "size_bytes": size_bytes,
                "duration_seconds": time.perf_counter() - start_perf,
            },
        )
        return row_id

    async def publish_batch(self, cmd: OutboxPublishCommand) -> None:
        rows: list[tuple[bytes, dict[str, str] | None]] = []
        total_size = 0
        start_perf = time.perf_counter()
        try:
            # P3: encode inside the try so a serialization failure on any body still
            # emits the ``published`` error metric.
            for body in cmd.batch_bodies:
                payload, hdrs = _encode_payload(body, headers=cmd.headers, serializer=self.serializer)
                total_size += len(payload)
                rows.append((payload, hdrs))
            await self._client().insert_batch(
                cmd.session,
                queue=cmd.queue,
                rows=rows,
                activate_in=cmd.activate_in,
                activate_at=cmd.activate_at,
            )
        except Exception as exc:
            self._emit_metric(
                "published",
                {
                    "queue": cmd.queue,
                    "status": "error",
                    "count": 0,
                    "size_bytes": total_size,
                    "duration_seconds": time.perf_counter() - start_perf,
                    "exception_type": type(exc).__name__,
                },
            )
            raise
        self._emit_metric(
            "published",
            {
                "queue": cmd.queue,
                "status": "success",
                "count": len(rows),
                "size_bytes": total_size,
                "duration_seconds": time.perf_counter() - start_perf,
            },
        )

    async def request(self, cmd: OutboxPublishCommand) -> typing.NoReturn:
        raise NotImplementedError(_REQUEST_UNSUPPORTED_MSG)
