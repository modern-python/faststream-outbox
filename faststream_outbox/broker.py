"""
OutboxBroker — a FastStream broker whose queue is a Postgres table.

Producers call ``broker.publish(body, queue=..., session=session)`` inside their
own SQLAlchemy transaction; the row commits with their domain writes. The broker
owns subscribers on the consumer side.
"""

import logging
import typing
from collections.abc import Iterable, Sequence

from faststream import BaseMiddleware
from faststream._internal.basic_types import LoggerProto
from faststream._internal.broker import BrokerUsecase
from faststream._internal.broker.registrator import Registrator
from faststream._internal.constants import EMPTY
from faststream._internal.di import FastDependsConfig
from faststream._internal.logger import DefaultLoggerStorage, make_logger_state
from faststream._internal.logger.logging import get_broker_logger
from faststream._internal.types import BrokerMiddleware, CustomCallable
from faststream.specification.schema import BrokerSpec
from faststream.specification.schema.extra import Tag, TagDict
from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncSession

from faststream_outbox.client import OutboxClient
from faststream_outbox.configs import EngineState, OutboxBrokerConfig
from faststream_outbox.envelope import _encode_payload
from faststream_outbox.message import OutboxInnerMessage
from faststream_outbox.registrator import OutboxRegistrator


if typing.TYPE_CHECKING:
    from fast_depends.dependencies import Dependant
    from faststream._internal.context.repository import ContextRepo
    from sqlalchemy import Table
    from sqlalchemy.ext.asyncio import AsyncEngine

    from faststream_outbox.subscriber.usecase import OutboxSubscriber


class OutboxParamsStorage(DefaultLoggerStorage):
    _max_msg_id_ln = -1
    _max_queue_name = 7

    def get_logger(self, *, context: "ContextRepo") -> LoggerProto:
        if logger := self._get_logger_ref():
            return logger
        logger = get_broker_logger(
            name="outbox",
            default_context={"queue": "", "message_id": ""},
            message_id_ln=self._max_msg_id_ln,
            fmt=(
                "%(asctime)s %(levelname)-8s - "
                f"%(queue)-{self._max_queue_name}s | "
                f"%(message_id)-{self._max_msg_id_ln}s "
                "- %(message)s"
            ),
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

    def __init__(  # noqa: PLR0913
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
        # AsyncAPI
        description: str | None = None,
        tags: Iterable[Tag | TagDict] = (),
    ) -> None:
        self._outbox_table = outbox_table
        engine_state = EngineState(engine)
        client = OutboxClient(engine, outbox_table) if engine is not None else None
        fd_config = FastDependsConfig(use_fastdepends=apply_types)
        broker_config = OutboxBrokerConfig(
            engine_state=engine_state,
            outbox_table=outbox_table,
            client=client,
            broker_middlewares=middlewares,
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
            producer=_NoProducer(),  # ty: ignore[invalid-argument-type]
        )
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
    def client(self) -> OutboxClient:
        client = self.config.broker_config.client
        if client is None:
            msg = "OutboxBroker is not connected; pass an AsyncEngine to the constructor."
            raise RuntimeError(msg)
        return client

    @typing.override
    async def _connect(self) -> "AsyncEngine":
        return self.config.broker_config.engine_state.engine

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
    ) -> None:
        """
        Insert one outbox row using *session*'s open transaction.

        Must be called inside a transaction the caller owns (typically inside an
        ``async with session.begin():`` block). ``publish`` does not flush, commit,
        or open its own transaction — that is the whole point of the transactional
        outbox pattern: the row commits atomically with the caller's domain writes.
        """
        if not isinstance(session, AsyncSession):
            msg = "broker.publish requires an sqlalchemy.ext.asyncio.AsyncSession"
            raise TypeError(msg)
        payload, hdrs = _encode_payload(body, headers=headers, correlation_id=correlation_id)
        await session.execute(insert(self._outbox_table).values(queue=queue, payload=payload, headers=hdrs))

    async def publish_batch(  # ty: ignore[invalid-method-override]
        self,
        *bodies: typing.Any,
        queue: str,
        session: AsyncSession,
        headers: dict[str, str] | None = None,
    ) -> None:
        """
        Insert multiple outbox rows via *session*. Same transactional contract as ``publish``.

        Each row gets its own auto-generated ``correlation_id``; pass *headers* to
        share static headers across all rows.
        """
        if not isinstance(session, AsyncSession):
            msg = "broker.publish_batch requires an sqlalchemy.ext.asyncio.AsyncSession"
            raise TypeError(msg)
        if not bodies:
            return
        rows = []
        for body in bodies:
            payload, hdrs = _encode_payload(body, headers=headers)
            rows.append({"queue": queue, "payload": payload, "headers": hdrs})
        await session.execute(insert(self._outbox_table), rows)

    async def request(self, *args: typing.Any, **kwargs: typing.Any) -> typing.NoReturn:
        msg = "OutboxBroker does not support request-reply"
        raise NotImplementedError(msg)


class _NoProducer:
    """Stub satisfying FastStream's broker producer slot — the outbox has no real producer."""

    async def publish(self, *_args: typing.Any, **_kwargs: typing.Any) -> typing.NoReturn:
        msg = "OutboxBroker has no producer"
        raise NotImplementedError(msg)

    async def request(self, *_args: typing.Any, **_kwargs: typing.Any) -> typing.NoReturn:
        msg = "OutboxBroker has no producer"
        raise NotImplementedError(msg)

    async def publish_batch(self, *_args: typing.Any, **_kwargs: typing.Any) -> typing.NoReturn:
        msg = "OutboxBroker has no producer"
        raise NotImplementedError(msg)

    def connect(self, *_args: typing.Any, **_kwargs: typing.Any) -> None:
        pass

    def disconnect(self) -> None:
        pass
