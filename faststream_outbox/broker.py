"""
OutboxBroker — a FastStream broker whose queue is a Postgres table.

There is no ``publish()``: producers insert outbox rows themselves via SQLAlchemy,
inside their own transaction (that's what makes it "transactional"). The broker's
job is consumer-side only: it owns subscribers that poll the table.
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

from faststream_outbox.client import OutboxClient
from faststream_outbox.configs import EngineState, OutboxBrokerConfig
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

    async def publish(self, *args: typing.Any, **kwargs: typing.Any) -> typing.NoReturn:
        msg = (
            "OutboxBroker has no publish API. Insert outbox rows yourself via SQLAlchemy "
            "inside your own transaction; use faststream_outbox.encode_payload() to "
            "produce the (payload, headers) tuple."
        )
        raise NotImplementedError(msg)

    async def request(self, *args: typing.Any, **kwargs: typing.Any) -> typing.NoReturn:
        msg = "OutboxBroker does not support request-reply"
        raise NotImplementedError(msg)

    async def publish_batch(self, *args: typing.Any, **kwargs: typing.Any) -> typing.NoReturn:
        msg = "OutboxBroker has no publish API; insert rows via SQLAlchemy"
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
