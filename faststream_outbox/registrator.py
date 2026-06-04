import warnings
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, override

from faststream._internal.broker.registrator import Registrator
from faststream._internal.types import CustomCallable
from faststream.middlewares import AckPolicy

from faststream_outbox.message import OutboxInnerMessage
from faststream_outbox.publisher.factory import create_publisher
from faststream_outbox.publisher.usecase import OutboxPublisher
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
        ack_policy: AckPolicy | None = None,
        propagate_inbound_headers: bool = False,
        dependencies: Iterable["Dependant"] = (),
        parser: CustomCallable | None = None,
        decoder: CustomCallable | None = None,
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
            ack_policy=ack_policy,
            propagate_inbound_headers=propagate_inbound_headers,
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
        )

    @override
    def publisher(  # ty: ignore[invalid-method-override]
        self,
        queue: str,
        *,
        headers: dict[str, str] | None = None,
        title: str | None = None,
        description: str | None = None,
        schema: Any | None = None,
        include_in_schema: bool = True,
    ) -> OutboxPublisher:
        """
        Construct a queue-scoped publisher.

        The publisher is standalone-only — call ``await pub.publish(body, session=session)``
        from inside your own transaction. Attempting to use it as a relay decorator on a
        subscriber raises ``NotImplementedError`` at decoration time, since the dispatch
        loop has no reachable ``AsyncSession`` without breaking the outbox transactional
        contract.
        """
        publisher = create_publisher(
            queue=queue,
            headers=headers,
            broker_config=self.config,  # ty: ignore[invalid-argument-type]
            title_=title,
            description_=description,
            schema_=schema,
            include_in_schema=include_in_schema,
        )
        super().publisher(publisher)
        return publisher
