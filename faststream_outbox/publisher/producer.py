"""
``OutboxProducer`` — the canonical insert path for outbox rows.

Both ``OutboxBroker.publish`` / ``publish_batch`` and ``OutboxPublisher.publish``
route through this producer via FastStream's ``_basic_publish(cmd, producer=...)``
middleware stack, so encode → insert → NOTIFY semantics live in one place. The
producer never opens its own session — every command carries the caller's
``AsyncSession`` so the row commits atomically with the caller's domain writes.
"""

import datetime as _dt
import time
import typing

from faststream._internal.endpoint.utils import ParserComposition
from faststream._internal.parser import DefaultCodec
from sqlalchemy import Float, Table, bindparam, func, insert, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from faststream_outbox._time import utcnow
from faststream_outbox.envelope import _encode_payload
from faststream_outbox.metrics import MetricsRecorder, _noop_recorder, _safe_emit
from faststream_outbox.parser.parser import OutboxParser
from faststream_outbox.response import _REQUEST_UNSUPPORTED_MSG, OutboxPublishCommand


if typing.TYPE_CHECKING:
    from collections.abc import Mapping

    from fast_depends.library.serializer import SerializerProto
    from faststream._internal.parser import CodecProto
    from faststream._internal.types import AsyncCallable, CustomCallable


def _is_future_dated(
    activate_in: _dt.timedelta | None,
    activate_at: _dt.datetime | None,
    now: _dt.datetime,
) -> bool:
    """Whether a row is genuinely future-dated (so NOTIFY is skipped — polling fires it at the gate)."""
    if activate_in is not None:
        return activate_in > _dt.timedelta(0)
    if activate_at is not None:
        return activate_at > now
    return False


class OutboxProducer:
    """``ProducerProto[OutboxPublishCommand]`` — runs encode + insert + NOTIFY on caller's session."""

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
        self._table = table
        self._channel = f"outbox_{table.name}"
        self.serializer: SerializerProto | None = None
        # ProducerProto[0.7] requires a `codec` attribute. The outbox owns its
        # own encoding pipeline (_encode_payload) and never reads this attribute
        # at runtime — it exists solely to satisfy the protocol.
        self.codec: CodecProto = DefaultCodec()
        default = OutboxParser()
        self._parser = ParserComposition(parser, default.parse_message)
        self._decoder = ParserComposition(decoder, default.decode_message)
        self._metrics_recorder = metrics_recorder

    def _emit_metric(self, event: str, tags: "Mapping[str, typing.Any]") -> None:
        # Delegate to the shared swallow-and-DEBUG-log helper so the recorder-isolation
        # contract lives in exactly one place (the subscriber's _emit_metric is the one
        # deliberate exception — it routes through self._log for handler-scoped context).
        _safe_emit(self._metrics_recorder, event, tags)

    def connect(
        self,
        connection: typing.Any = None,  # noqa: ARG002
        serializer: typing.Optional["SerializerProto"] = None,
    ) -> None:
        self.serializer = serializer

    def disconnect(self) -> None:
        # Caller owns the engine — never dispose anything here.
        pass

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
            row_id = await self._do_publish(cmd, payload, hdrs)
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

    async def _do_publish(
        self,
        cmd: OutboxPublishCommand,
        payload: bytes,
        hdrs: dict[str, str] | None,
    ) -> int | None:
        t = self._table
        values: dict[str, typing.Any] = {"queue": cmd.queue, "payload": payload, "headers": hdrs}
        # Server-side compute keeps timing immune to worker/DB clock skew (mirrors
        # client.mark_pending_with_lease).
        if cmd.activate_in is not None:
            values["next_attempt_at"] = func.now() + func.make_interval(
                0, 0, 0, 0, 0, 0, bindparam("activate_in_seconds", cmd.activate_in.total_seconds(), type_=Float)
            )
        elif cmd.activate_at is not None:
            values["next_attempt_at"] = cmd.activate_at
        if cmd.timer_id is not None:
            values["timer_id"] = cmd.timer_id
        # Skip NOTIFY only when the row is genuinely future-dated. A past activate_at
        # (e.g. a recovered idempotency token) is immediately eligible — fire NOTIFY.
        now = utcnow()
        is_future = _is_future_dated(cmd.activate_in, cmd.activate_at, now)

        if cmd.timer_id is not None:
            stmt = (
                pg_insert(t)
                .values(**values)
                .on_conflict_do_nothing(
                    index_elements=[t.c.queue, t.c.timer_id],
                    index_where=t.c.timer_id.is_not(None),
                )
                .returning(t.c.id)
            )
        else:
            stmt = insert(t).values(**values).returning(t.c.id)

        result = await cmd.session.execute(stmt)
        row_id: int | None = result.scalar()
        # Skip NOTIFY for future-dated rows (listeners can't act before the gate
        # opens — polling fires them at next tick) and on conflict (no row landed).
        if row_id is not None and not is_future:
            await self._notify(cmd.session, cmd.queue)
        return row_id

    async def publish_batch(self, cmd: OutboxPublishCommand) -> None:
        bodies = cmd.batch_bodies
        # Client-side time for batch: executemany doesn't compose cleanly with
        # column-level SQL expressions, and a few-ms drift versus the DB is
        # harmless for user-supplied scheduling. Retries still use server time.
        now = utcnow()
        if cmd.activate_in is not None:
            next_at: _dt.datetime | None = now + cmd.activate_in
        else:
            next_at = cmd.activate_at
        rows: list[dict[str, typing.Any]] = []
        total_size = 0
        start_perf = time.perf_counter()
        try:
            # P3: encode inside the try so a serialization failure on any body still
            # emits the ``published`` error metric.
            for body in bodies:
                payload, hdrs = _encode_payload(body, headers=cmd.headers, serializer=self.serializer)
                total_size += len(payload)
                row: dict[str, typing.Any] = {"queue": cmd.queue, "payload": payload, "headers": hdrs}
                if next_at is not None:
                    row["next_attempt_at"] = next_at
                rows.append(row)
            await cmd.session.execute(insert(self._table), rows)
            # Skip NOTIFY only when genuinely future-dated; past times are eligible.
            if not _is_future_dated(cmd.activate_in, cmd.activate_at, now):
                await self._notify(cmd.session, cmd.queue)
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

    async def _notify(self, session: typing.Any, queue: str) -> None:
        # ``pg_notify(:channel, :payload)`` — parameterized so channel and payload
        # bind cleanly (raw NOTIFY accepts only literals — injection-prone). Runs
        # on the caller's session so NOTIFY commits with the row insert; rollback
        # silently discards it. Non-Postgres dialects ignore it.
        await session.execute(
            text("SELECT pg_notify(:channel, :payload)"),
            {"channel": self._channel, "payload": queue},
        )
