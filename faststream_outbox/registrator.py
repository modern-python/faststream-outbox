import warnings
from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, override

from faststream._internal.broker.registrator import Registrator
from faststream._internal.types import CustomCallable, SubscriberMiddleware

from faststream_outbox.message import OutboxInnerMessage
from faststream_outbox.retry import ExponentialRetry
from faststream_outbox.subscriber.factory import create_subscriber


if TYPE_CHECKING:
    from fast_depends.dependencies import Dependant

    from faststream_outbox.retry import RetryStrategyProto
    from faststream_outbox.subscriber.usecase import OutboxSubscriber


def _default_retry_strategy() -> "RetryStrategyProto":
    """
    Fallback retry policy when the user passes nothing.

    An outbox is a reliability primitive; defaulting to "delete on first error" turns
    every transient handler failure into silent data loss. Defaulting to a bounded
    exponential retry keeps the contract intuitive — users who actually want
    delete-on-error opt in explicitly with ``NoRetry()``.
    """
    return ExponentialRetry(
        initial_delay_seconds=1.0,
        multiplier=2.0,
        max_delay_seconds=300.0,
        max_attempts=10,
        jitter_factor=0.2,
    )


class OutboxRegistrator(Registrator[OutboxInnerMessage, "OutboxBrokerConfig"]):  # ty: ignore[unresolved-reference]
    @override
    def subscriber(  # ty: ignore[invalid-method-override]
        self,
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
    ) -> "OutboxSubscriber":
        queue_list = [queues] if isinstance(queues, str) else list(queues)
        if not queue_list:
            msg = "subscriber() requires at least one queue name"
            raise ValueError(msg)
        resolved_retry_strategy = retry_strategy if retry_strategy is not None else _default_retry_strategy()
        subscriber = create_subscriber(
            queues=queue_list,
            max_workers=max_workers,
            retry_strategy=resolved_retry_strategy,
            fetch_batch_size=fetch_batch_size,
            min_fetch_interval=min_fetch_interval,
            max_fetch_interval=max_fetch_interval,
            lease_ttl_seconds=lease_ttl_seconds,
            max_deliveries=max_deliveries,
            config=self.config,  # ty: ignore[invalid-argument-type]
            title_=title_,
            description_=description_,
            include_in_schema=include_in_schema,
        )
        existing = {q for s in self._subscribers for q in getattr(s, "_queues", [])}
        overlap = sorted(set(queue_list) & existing)
        if overlap:
            warnings.warn(
                f"Duplicate subscriber registered for queues {overlap}: workers will compete "
                f"for the same rows. Use one subscriber per queue, or attach multiple handlers "
                f"to the same subscriber.",
                stacklevel=2,
            )
        super().subscriber(subscriber)
        return subscriber.add_call(
            parser_=parser or self._parser,
            decoder_=decoder or self._decoder,
            dependencies_=dependencies,
            middlewares_=middlewares,
        )

    @override
    def publisher(self, *args: object, **kwargs: object) -> object:  # ty: ignore[invalid-method-override]
        msg = "OutboxBroker has no publisher() — insert outbox rows via SQLAlchemy in your own transaction."
        raise NotImplementedError(msg)
