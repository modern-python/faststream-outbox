import typing

from faststream._internal.endpoint.subscriber.call_item import CallsCollection

from faststream_outbox.subscriber.config import OutboxSubscriberConfig, OutboxSubscriberSpecificationConfig
from faststream_outbox.subscriber.usecase import OutboxSubscriber, OutboxSubscriberSpecification


if typing.TYPE_CHECKING:
    from faststream_outbox.configs import OutboxBrokerConfig
    from faststream_outbox.retry import RetryStrategyProto


def create_subscriber(  # noqa: PLR0913
    *,
    queues: list[str],
    max_workers: int,
    retry_strategy: "RetryStrategyProto | None",
    fetch_batch_size: int,
    min_fetch_interval: float,
    max_fetch_interval: float,
    release_stuck_timeout: float,
    release_stuck_interval: float,
    max_deliveries: int | None,
    config: "OutboxBrokerConfig",
    title_: str | None = None,
    description_: str | None = None,
    include_in_schema: bool = True,
) -> OutboxSubscriber:
    usecase_config = OutboxSubscriberConfig(
        _outer_config=config,
        queues=queues,
        max_workers=max_workers,
        retry_strategy=retry_strategy,
        fetch_batch_size=fetch_batch_size,
        min_fetch_interval=min_fetch_interval,
        max_fetch_interval=max_fetch_interval,
        release_stuck_timeout=release_stuck_timeout,
        release_stuck_interval=release_stuck_interval,
        max_deliveries=max_deliveries,
    )
    specification_config = OutboxSubscriberSpecificationConfig(
        queues=queues,
        title_=title_,
        description_=description_,
        include_in_schema=include_in_schema,
    )
    calls: CallsCollection[typing.Any] = CallsCollection()
    specification = OutboxSubscriberSpecification(
        _outer_config=config,
        specification_config=specification_config,
        calls=calls,
    )
    return OutboxSubscriber(
        config=usecase_config,
        specification=specification,
        calls=calls,
    )
