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
        release_stuck_timeout: float = 300.0,
        release_stuck_interval: float | None = None,
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
            release_stuck_timeout=release_stuck_timeout,
            release_stuck_interval=release_stuck_interval,
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
    """Includable router for ``OutboxBroker``."""

    def __init__(  # noqa: PLR0913
        self,
        prefix: str = "",
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
                prefix=prefix,
            ),
            handlers=handlers,  # ty: ignore[unknown-argument]
            routers=routers,
        )
