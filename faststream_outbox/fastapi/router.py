"""FastAPI integration for the outbox transport.

``OutboxRouter`` is a thin subclass of FastStream's ``StreamRouter`` (which is
itself an ``APIRouter``). Mounting the router into a FastAPI app via
``app.include_router(router)`` wires up the lifespan so the inner ``OutboxBroker``
starts/stops with the app — user code never calls ``broker.start()`` directly.

FastAPI ``Depends(...)`` resolves inside subscriber handlers because the base
``StreamRouter._subscriber_compatibility_wrapper`` patches the user callable
through ``wrap_callable_to_fastapi_compatible``; that bridges FastAPI's
dependency resolver into the FastStream consume pipeline. That's the load-bearing
property for the outbox: handlers can receive an ``AsyncSession`` via the same
``Depends(get_session)`` they use for HTTP endpoints, and the row insert commits
with the caller's domain writes.
"""

import logging
import typing
from collections.abc import Callable, Iterable, Sequence
from enum import Enum

from fastapi.datastructures import Default
from fastapi.routing import APIRoute
from fastapi.utils import generate_unique_id
from faststream._internal.constants import EMPTY
from faststream._internal.fastapi import StreamRouter
from faststream._internal.types import BrokerMiddleware, CustomCallable
from faststream.middlewares import AckPolicy
from starlette.responses import JSONResponse

from faststream_outbox.broker import OutboxBroker as _OutboxBroker
from faststream_outbox.message import OutboxInnerMessage


if typing.TYPE_CHECKING:
    from fast_depends.library.serializer import SerializerProto
    from fastapi import params
    from fastapi.types import IncEx
    from faststream._internal.basic_types import LoggerProto
    from faststream._internal.context import ContextRepo
    from faststream.middlewares import BaseMiddleware
    from faststream.specification.base import SpecificationFactory
    from faststream.specification.schema.extra import Tag, TagDict
    from sqlalchemy import Table
    from sqlalchemy.ext.asyncio import AsyncEngine
    from starlette.routing import BaseRoute
    from starlette.types import ASGIApp, Lifespan

    from faststream_outbox.configs import LastExceptionRenderer
    from faststream_outbox.metrics import MetricsRecorder
    from faststream_outbox.publisher.usecase import OutboxPublisher
    from faststream_outbox.retry import RetryStrategyProto
    from faststream_outbox.subscriber.usecase import OutboxSubscriber


# Module-level singletons (B008 requires no function calls in default args).
_DEFAULT_RESPONSE_CLASS = Default(JSONResponse)
_DEFAULT_GENERATE_UNIQUE_ID = Default(generate_unique_id)
_DEFAULT_RESPONSE_MODEL = Default(None)


class OutboxRouter(StreamRouter[OutboxInnerMessage]):
    """FastAPI router for the outbox transport."""

    broker_class = _OutboxBroker
    broker: _OutboxBroker

    def __init__(  # noqa: PLR0913
        self,
        engine: "AsyncEngine | None" = None,
        *,
        outbox_table: "Table",
        dlq_table: "Table | None" = None,
        # Outbox broker kwargs (mirror ``OutboxBroker.__init__``). Note: ``apply_types``
        # is fixed to False by ``StreamRouter`` (FastAPI's FastDepends is used instead),
        # and ``dependencies`` here means FastAPI dependencies — the broker's
        # FastStream dependencies are not exposed for FastAPI flows.
        decoder: CustomCallable | None = None,
        parser: CustomCallable | None = None,
        middlewares: Sequence["type[BaseMiddleware] | BrokerMiddleware[OutboxInnerMessage]"] = (),
        graceful_timeout: float | None = 15.0,
        serializer: "SerializerProto | None" = EMPTY,
        # Metrics (recorder seam — mirrors ``OutboxBroker.__init__``)
        metrics_recorder: "MetricsRecorder | None" = None,
        last_exception_renderer: "LastExceptionRenderer | None" = None,
        # AsyncAPI / Specification
        specification: typing.Optional["SpecificationFactory"] = None,
        description: str | None = None,
        specification_tags: "Iterable[Tag | TagDict]" = (),
        # Logging
        logger: "LoggerProto | None" = EMPTY,
        log_level: int = logging.INFO,
        # StreamRouter ergonomics
        setup_state: bool = True,
        schema_url: str | None = "/asyncapi",
        context: typing.Optional["ContextRepo"] = None,
        # FastAPI APIRouter kwargs
        prefix: str = "",
        tags: list[str | Enum] | None = None,
        dependencies: Sequence["params.Depends"] | None = None,
        default_response_class: typing.Any = _DEFAULT_RESPONSE_CLASS,
        responses: dict[int | str, dict[str, typing.Any]] | None = None,
        callbacks: list["BaseRoute"] | None = None,
        routes: list["BaseRoute"] | None = None,
        redirect_slashes: bool = True,
        default: typing.Optional["ASGIApp"] = None,
        dependency_overrides_provider: typing.Any | None = None,
        route_class: type["APIRoute"] = APIRoute,
        on_startup: Sequence[Callable[[], typing.Any]] | None = None,
        on_shutdown: Sequence[Callable[[], typing.Any]] | None = None,
        lifespan: typing.Optional["Lifespan[typing.Any]"] = None,
        deprecated: bool | None = None,
        include_in_schema: bool = True,
        generate_unique_id_function: Callable[["APIRoute"], str] = _DEFAULT_GENERATE_UNIQUE_ID,
    ) -> None:
        super().__init__(
            # Positional → connection_args → broker_class(*connection_args, ...)
            engine,
            # Outbox-broker kwargs (flow through StreamRouter's **connection_kwars
            # into ``OutboxBroker(...)``)
            outbox_table=outbox_table,
            dlq_table=dlq_table,
            metrics_recorder=metrics_recorder,
            last_exception_renderer=last_exception_renderer,
            decoder=decoder,
            parser=parser,
            graceful_timeout=graceful_timeout,
            serializer=serializer,
            description=description,
            logger=logger,
            log_level=log_level,
            # Explicit StreamRouter kwargs
            middlewares=middlewares,
            specification=specification,
            specification_tags=specification_tags,
            setup_state=setup_state,
            schema_url=schema_url,
            context=context,
            # FastAPI APIRouter kwargs
            prefix=prefix,
            tags=tags,
            dependencies=dependencies,
            default_response_class=default_response_class,
            responses=responses,
            callbacks=callbacks,
            routes=routes,
            redirect_slashes=redirect_slashes,
            default=default,
            dependency_overrides_provider=dependency_overrides_provider,
            route_class=route_class,
            on_startup=on_startup,
            on_shutdown=on_shutdown,
            deprecated=deprecated,
            include_in_schema=include_in_schema,
            lifespan=lifespan,
            generate_unique_id_function=generate_unique_id_function,
        )

    def subscriber(  # ty: ignore[invalid-method-override]  # noqa: PLR0913
        self,
        queues: str | list[str],
        *,
        # Outbox-subscriber knobs (mirror ``OutboxRegistrator.subscriber``)
        max_workers: int = 1,
        retry_strategy: "RetryStrategyProto | None" = None,
        fetch_batch_size: int = 10,
        min_fetch_interval: float = 1.0,
        max_fetch_interval: float = 10.0,
        lease_ttl_seconds: float = 60.0,
        max_deliveries: int | None = None,
        terminal_flush_batch_size: int = 1,
        ack_policy: AckPolicy | None = None,
        propagate_inbound_headers: bool = False,
        # FastStream subscriber-level knobs
        dependencies: Iterable["params.Depends"] = (),
        parser: CustomCallable | None = None,
        decoder: CustomCallable | None = None,
        title_: str | None = None,
        description_: str | None = None,
        include_in_schema: bool = True,
        # FastAPI response-model knobs (defaults match ``StreamRouter`` expectations)
        response_model: typing.Any = _DEFAULT_RESPONSE_MODEL,
        response_model_include: typing.Optional["IncEx"] = None,
        response_model_exclude: typing.Optional["IncEx"] = None,
        response_model_by_alias: bool = True,
        response_model_exclude_unset: bool = False,
        response_model_exclude_defaults: bool = False,
        response_model_exclude_none: bool = False,
    ) -> "OutboxSubscriber":
        # ``StreamRouter.subscriber`` uses ``*extra: NameRequired | str`` — our
        # ``queues: str | list[str]`` is wider; the actual broker-side
        # ``OutboxRegistrator.subscriber`` accepts both.
        return typing.cast(
            "OutboxSubscriber",
            super().subscriber(
                queues,  # ty: ignore[invalid-argument-type]
                max_workers=max_workers,
                retry_strategy=retry_strategy,
                fetch_batch_size=fetch_batch_size,
                min_fetch_interval=min_fetch_interval,
                max_fetch_interval=max_fetch_interval,
                lease_ttl_seconds=lease_ttl_seconds,
                max_deliveries=max_deliveries,
                terminal_flush_batch_size=terminal_flush_batch_size,
                ack_policy=ack_policy,
                propagate_inbound_headers=propagate_inbound_headers,
                dependencies=dependencies,
                parser=parser,
                decoder=decoder,
                title_=title_,
                description_=description_,
                include_in_schema=include_in_schema,
                response_model=response_model,
                response_model_include=response_model_include,
                response_model_exclude=response_model_exclude,
                response_model_by_alias=response_model_by_alias,
                response_model_exclude_unset=response_model_exclude_unset,
                response_model_exclude_defaults=response_model_exclude_defaults,
                response_model_exclude_none=response_model_exclude_none,
            ),
        )

    def publisher(  # ty: ignore[invalid-method-override]
        self,
        queue: str,
        *,
        headers: dict[str, str] | None = None,
        title: str | None = None,
        description: str | None = None,
        schema: typing.Any | None = None,
        include_in_schema: bool = True,
    ) -> "OutboxPublisher":
        # ``StreamRouter.publisher`` forwards directly to ``self.broker.publisher``;
        # mirror its delegation so outbox users get the right return type.
        return self.broker.publisher(
            queue,
            headers=headers,
            title=title,
            description=description,
            schema=schema,
            include_in_schema=include_in_schema,
        )
