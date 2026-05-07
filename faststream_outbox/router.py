from collections.abc import Awaitable, Callable, Iterable, Sequence
from typing import TYPE_CHECKING

from faststream._internal.basic_types import SendableMessage
from faststream._internal.broker.router import BrokerRouter, SubscriberRoute
from faststream._internal.configs import BrokerConfig
from faststream._internal.types import BrokerMiddleware, CustomCallable, SubscriberMiddleware

from faststream_outbox.message import OutboxInnerMessage
from faststream_outbox.registrator import OutboxRegistrator


if TYPE_CHECKING:
    from fast_depends.dependencies import Dependant

    from faststream_outbox.retry import RetryStrategyProto


class OutboxRoute(SubscriberRoute):
    """Delayed-registration subscriber for use with ``OutboxRouter``."""

    def __init__(  # noqa: PLR0913
        self,
        call: Callable[..., SendableMessage] | Callable[..., Awaitable[SendableMessage]],
        queues: str | list[str],
        *,
        max_workers: int = 1,
        retry_strategy: "RetryStrategyProto | None" = None,
        fetch_batch_size: int = 10,
        min_fetch_interval: float = 1.0,
        max_fetch_interval: float = 10.0,
        lease_ttl_seconds: float = 60.0,
        max_deliveries: int | None = None,
        dependencies: Iterable["Dependant"] = (),
        parser: CustomCallable | None = None,
        decoder: CustomCallable | None = None,
        middlewares: Sequence[SubscriberMiddleware[OutboxInnerMessage]] = (),
        title_: str | None = None,
        description_: str | None = None,
        include_in_schema: bool = True,
    ) -> None:
        super().__init__(
            call=call,
            queues=queues,
            max_workers=max_workers,
            retry_strategy=retry_strategy,
            fetch_batch_size=fetch_batch_size,
            min_fetch_interval=min_fetch_interval,
            max_fetch_interval=max_fetch_interval,
            lease_ttl_seconds=lease_ttl_seconds,
            max_deliveries=max_deliveries,
            dependencies=dependencies,
            parser=parser,
            decoder=decoder,
            middlewares=middlewares,
            title_=title_,
            description_=description_,
            include_in_schema=include_in_schema,
        )


class OutboxRouter(OutboxRegistrator, BrokerRouter[OutboxInnerMessage, BrokerConfig]):
    """
    Includable router for ``OutboxBroker``.

    Use it to register subscribers in a separate module and attach them to the
    broker via ``broker.include_router(router)``. There is no ``prefix`` knob:
    queues are routed by their literal name, so producers and consumers must
    agree on the exact string. If you want namespacing, put it in the queue name.
    """

    def __init__(  # noqa: PLR0913
        self,
        handlers: Iterable[OutboxRoute] = (),
        *,
        dependencies: Iterable["Dependant"] = (),
        middlewares: Sequence[BrokerMiddleware[OutboxInnerMessage]] = (),
        parser: CustomCallable | None = None,
        decoder: CustomCallable | None = None,
        include_in_schema: bool | None = None,
        routers: Sequence[OutboxRegistrator] = (),
    ) -> None:
        super().__init__(
            config=BrokerConfig(
                broker_middlewares=middlewares,
                broker_dependencies=dependencies,
                broker_parser=parser,
                broker_decoder=decoder,
                include_in_schema=include_in_schema,
            ),
            handlers=handlers,  # ty: ignore[unknown-argument]
            routers=routers,
        )
