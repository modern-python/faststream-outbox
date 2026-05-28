"""
OutboxBroker — a FastStream broker whose queue is a Postgres table.

Producers call ``broker.publish(body, queue=..., session=session)`` inside their
own SQLAlchemy transaction; the row commits with their domain writes. The broker
owns subscribers on the consumer side.
"""

import datetime as _dt
import logging
import typing
from collections.abc import Iterable, Sequence
from types import TracebackType

from faststream import BaseMiddleware
from faststream._internal.basic_types import LoggerProto
from faststream._internal.broker import BrokerUsecase
from faststream._internal.broker.registrator import Registrator
from faststream._internal.constants import EMPTY
from faststream._internal.di import FastDependsConfig
from faststream._internal.logger import DefaultLoggerStorage, make_logger_state
from faststream._internal.logger.logging import get_broker_logger
from faststream._internal.types import BrokerMiddleware, CustomCallable
from faststream.exceptions import IncorrectState
from faststream.specification.schema import BrokerSpec
from faststream.specification.schema.extra import Tag, TagDict
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession

from faststream_outbox.client import AbstractOutboxClient, OutboxClient, _row_to_message
from faststream_outbox.configs import OutboxBrokerConfig
from faststream_outbox.message import OutboxInnerMessage
from faststream_outbox.publisher.producer import OutboxProducer
from faststream_outbox.registrator import OutboxRegistrator
from faststream_outbox.response import OutboxPublishCommand


if typing.TYPE_CHECKING:
    from fast_depends.dependencies import Dependant
    from fast_depends.library.serializer import SerializerProto
    from faststream._internal.context.repository import ContextRepo
    from sqlalchemy import Table
    from sqlalchemy.ext.asyncio import AsyncEngine

    from faststream_outbox.subscriber.usecase import OutboxSubscriber


def _validate_activate_args(
    method_name: str,
    activate_in: _dt.timedelta | None,
    activate_at: _dt.datetime | None,
) -> None:
    """Mutex + tz-aware checks shared by the test fakes. Real broker delegates to ``OutboxPublishCommand``."""
    if activate_in is not None and activate_at is not None:
        msg = f"{method_name} accepts at most one of activate_in / activate_at"
        raise ValueError(msg)
    if activate_at is not None and activate_at.tzinfo is None:
        msg = f"{method_name} requires activate_at to be timezone-aware"
        raise ValueError(msg)


def _compute_next_at_client_side(
    activate_in: _dt.timedelta | None,
    activate_at: _dt.datetime | None,
) -> _dt.datetime | None:
    """Resolve activate_in / activate_at to a single ``next_attempt_at`` value (client clock)."""
    if activate_in is not None:
        return _dt.datetime.now(tz=_dt.UTC) + activate_in
    return activate_at


class _CaptureExceptionMiddleware(BaseMiddleware):
    """
    Stash the handler exception on the inner row before AckMiddleware nacks.

    FastStream's AcknowledgementMiddleware catches the handler exception in its
    own ``after_processed`` and calls ``message.nack()`` directly — the exception
    never propagates back to the worker loop. Without this middleware,
    ``OutboxInnerMessage._nack`` sees ``last_exception=None`` and retry strategies
    that branch on exception type can't work. We sit one step closer to the handler
    in the middleware stack so our ``after_processed`` runs before AckMiddleware's,
    capturing ``exc_val`` onto the row.
    """

    async def after_processed(
        self,
        exc_type: type[BaseException] | None = None,  # noqa: ARG002
        exc_val: BaseException | None = None,
        exc_tb: TracebackType | None = None,  # noqa: ARG002
    ) -> bool | None:
        if exc_val is not None and isinstance(self.msg, OutboxInnerMessage):
            self.msg.last_exception = exc_val
        return False


class OutboxParamsStorage(DefaultLoggerStorage):
    def get_logger(self, *, context: "ContextRepo") -> LoggerProto:
        if logger := self._get_logger_ref():
            return logger
        logger = get_broker_logger(
            name="outbox",
            default_context={"queue": "", "message_id": ""},
            message_id_ln=-1,
            fmt="%(asctime)s %(levelname)-8s - %(queue)-7s | %(message_id)s - %(message)s",
            context=context,
            log_level=self.logger_log_level,
        )
        self._logger_ref.add(logger)
        return logger


class OutboxBroker(
    OutboxRegistrator,
    BrokerUsecase[OutboxInnerMessage, "AsyncEngine", OutboxBrokerConfig],
):
    """FastStream broker backed by a Postgres outbox table."""

    _subscribers: list["OutboxSubscriber"]

    def __init__(
        self,
        engine: "AsyncEngine | None" = None,
        *,
        outbox_table: "Table",
        decoder: CustomCallable | None = None,
        parser: CustomCallable | None = None,
        dependencies: Iterable["Dependant"] = (),
        middlewares: Sequence[type[BaseMiddleware] | BrokerMiddleware[OutboxInnerMessage]] = (),
        graceful_timeout: float | None = 15.0,
        routers: Sequence[Registrator[OutboxInnerMessage]] = (),
        # Logging
        logger: LoggerProto | None = EMPTY,
        log_level: int = logging.INFO,
        # FastDepends
        apply_types: bool = True,
        serializer: "SerializerProto | None" = EMPTY,
        # AsyncAPI
        description: str | None = None,
        tags: Iterable[Tag | TagDict] = (),
    ) -> None:
        self._outbox_table = outbox_table
        client = OutboxClient(engine, outbox_table) if engine is not None else None
        fd_config = FastDependsConfig(use_fastdepends=apply_types, serializer=serializer)
        producer = OutboxProducer(table=outbox_table, parser=parser, decoder=decoder)
        broker_config = OutboxBrokerConfig(
            engine=engine,
            client=client,
            broker_middlewares=(_CaptureExceptionMiddleware, *middlewares),
            broker_parser=parser,
            broker_decoder=decoder,
            logger=make_logger_state(
                logger=logger,
                log_level=log_level,
                default_storage_cls=OutboxParamsStorage,
            ),
            fd_config=fd_config,
            broker_dependencies=dependencies,
            graceful_timeout=graceful_timeout,
            extra_context={"broker": self},
            producer=producer,
        )
        # Serializer lives on fd_config — wire it onto the producer so encoded
        # bodies use the same path as the broker's own publish flow.
        producer.serializer = fd_config._serializer  # noqa: SLF001
        specification = BrokerSpec(
            url=[],
            protocol="postgresql",
            protocol_version=None,
            description=description,
            tags=tags,
            security=None,
        )
        super().__init__(config=broker_config, specification=specification, routers=routers)  # ty: ignore[unknown-argument]

    @property
    def client(self) -> AbstractOutboxClient:
        client = self.config.broker_config.client
        if client is None:
            msg = "OutboxBroker is not connected; pass an AsyncEngine to the constructor."
            raise RuntimeError(msg)
        return client

    @typing.override
    async def _connect(self) -> "AsyncEngine":
        engine = self.config.broker_config.engine
        if engine is None:
            msg = "Engine not available. Pass an AsyncEngine to OutboxBroker(...)."
            raise IncorrectState(msg)
        return engine

    @typing.override
    async def __aenter__(self) -> typing.Self:
        await self.start()
        return self

    @typing.override
    async def start(self) -> None:
        await self.connect()
        await super().start()

    @typing.override
    async def ping(self, timeout: float | None = None) -> bool:
        client = self.config.broker_config.client
        if client is None:
            return False
        if not await client.ping():
            return False
        for subscriber in self._subscribers:
            for task in subscriber.tasks:
                if task.done():
                    return False
        return True

    async def validate_schema(self) -> None:
        """Validate the user's table matches what the package expects. Opt-in."""
        await self.client.validate_schema()

    async def publish(  # ty: ignore[invalid-method-override]
        self,
        body: typing.Any,
        *,
        queue: str,
        session: AsyncSession,
        headers: dict[str, str] | None = None,
        correlation_id: str | None = None,
        activate_in: _dt.timedelta | None = None,
        activate_at: _dt.datetime | None = None,
        timer_id: str | None = None,
    ) -> int | None:
        """
        Insert one outbox row using *session*'s open transaction.

        Must be called inside a transaction the caller owns (typically inside an
        ``async with session.begin():`` block). ``publish`` does not flush, commit,
        or open its own transaction — that is the whole point of the transactional
        outbox pattern: the row commits atomically with the caller's domain writes.

        Schedule a delayed delivery by passing exactly one of *activate_in* (relative)
        or *activate_at* (absolute, tz-aware). Pass *timer_id* to deduplicate per
        ``(queue, timer_id)`` — re-publishing with the same id is a no-op (returns
        ``None``). Cancel a not-yet-leased timer with :meth:`cancel_timer`.

        Returns the inserted row's ``id`` (BigInt PK), or ``None`` if a timer with
        the same ``(queue, timer_id)`` already exists.
        """
        cmd = OutboxPublishCommand(
            body,
            queue=queue,
            session=session,
            headers=headers,
            correlation_id=correlation_id,
            activate_in=activate_in,
            activate_at=activate_at,
            timer_id=timer_id,
        )
        result = await self._basic_publish(cmd, producer=self.config.producer)
        return typing.cast("int | None", result)

    async def publish_batch(  # ty: ignore[invalid-method-override]
        self,
        *bodies: typing.Any,
        queue: str,
        session: AsyncSession,
        headers: dict[str, str] | None = None,
        activate_in: _dt.timedelta | None = None,
        activate_at: _dt.datetime | None = None,
    ) -> None:
        """
        Insert multiple outbox rows via *session*. Same transactional contract as ``publish``.

        Each row gets its own auto-generated ``correlation_id``; pass *headers* to
        share static headers across all rows. *activate_in* / *activate_at* schedule
        every row in the batch identically — per-row timer dedup is not supported,
        use :meth:`publish` for that.
        """
        if not bodies:
            # Validate the activate args even when there's no work so callers
            # get the same misuse error on empty batches as on real ones.
            _validate_activate_args("broker.publish_batch", activate_in, activate_at)
            return
        first, *rest = bodies
        cmd = OutboxPublishCommand(
            first,
            *rest,
            queue=queue,
            session=session,
            headers=headers,
            activate_in=activate_in,
            activate_at=activate_at,
        )
        await self._basic_publish_batch(cmd, producer=self.config.producer)

    async def cancel_timer(
        self,
        *,
        queue: str,
        timer_id: str,
        session: AsyncSession,
    ) -> bool:
        """
        Delete a not-yet-leased timer row. Idempotent at the ``(queue, timer_id)`` level.

        Same transactional contract as :meth:`publish` — runs on the caller's session and
        commits with their transaction.

        Returns ``True`` if *this call* deleted the row. ``False`` means the row is no
        longer cancelable by you, which covers three cases:

        1. The row's handler is already in flight (``acquired_token IS NOT NULL``). The
           ``acquired_token IS NULL`` guard prevents this call from clobbering the
           in-flight lease; the delivery completes normally.
        2. Another caller already canceled the same ``(queue, timer_id)`` in a concurrent
           transaction.
        3. No row exists with that ``(queue, timer_id)`` — either the timer was never
           scheduled, the original ``publish(..., timer_id=...)`` hit the
           ``ON CONFLICT DO NOTHING`` path and returned ``None``, or (most common in
           long-running deployments) the row was already delivered and removed by a
           worker before this call.

        Treat ``False`` as "no cancellation needed from your transaction", not as
        "cancellation failed".
        """
        if not isinstance(session, AsyncSession):
            msg = "broker.cancel_timer requires an sqlalchemy.ext.asyncio.AsyncSession"
            raise TypeError(msg)
        t = self._outbox_table
        stmt = delete(t).where(
            t.c.queue == queue,
            t.c.timer_id == timer_id,
            t.c.acquired_token.is_(None),
        )
        result = await session.execute(stmt)
        return (result.rowcount or 0) > 0  # ty: ignore[unresolved-attribute]

    async def fetch_unprocessed(
        self,
        *,
        session: AsyncSession,
        queue: str | None = None,
        limit: int = 1000,
    ) -> list[OutboxInnerMessage]:
        """
        Return outbox rows currently in the table — pending, in-flight, or future-dated.

        Intended for test assertions: a successful delivery deletes the row, so anything
        still in the table is "unprocessed". Pass *queue* to filter to a single queue;
        omit it to return rows across all queues. *limit* caps the result set
        (default 1000) so an accidental call against a backlogged production table
        does not OOM the process. Runs on the caller's session (same transactional
        contract as :meth:`publish`); does not acquire a lease and does not mutate
        row state, so it is safe to call alongside running subscribers.
        """
        if not isinstance(session, AsyncSession):
            msg = "broker.fetch_unprocessed requires an sqlalchemy.ext.asyncio.AsyncSession"
            raise TypeError(msg)
        t = self._outbox_table
        stmt = select(*t.c).order_by(t.c.id).limit(limit)
        if queue is not None:
            stmt = stmt.where(t.c.queue == queue)
        result = await session.execute(stmt)
        return [_row_to_message(dict(row)) for row in result.mappings().all()]

    async def request(self, *args: typing.Any, **kwargs: typing.Any) -> typing.NoReturn:
        msg = "OutboxBroker does not support request-reply"
        raise NotImplementedError(msg)
