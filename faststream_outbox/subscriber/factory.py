import typing

from faststream._internal.constants import EMPTY
from faststream._internal.endpoint.subscriber.call_item import CallsCollection
from faststream.middlewares import AckPolicy

from faststream_outbox.subscriber.config import OutboxSubscriberConfig, OutboxSubscriberSpecificationConfig
from faststream_outbox.subscriber.usecase import OutboxSubscriber, OutboxSubscriberSpecification


if typing.TYPE_CHECKING:
    from faststream_outbox.configs import OutboxBrokerConfig
    from faststream_outbox.retry import RetryStrategyProto


def create_subscriber(
    *,
    queues: list[str],
    max_workers: int,
    retry_strategy: "RetryStrategyProto | None",
    fetch_batch_size: int,
    min_fetch_interval: float,
    max_fetch_interval: float,
    lease_ttl_seconds: float,
    max_deliveries: int | None,
    config: "OutboxBrokerConfig",
    ack_policy: AckPolicy | None = None,
    propagate_inbound_headers: bool = False,
    title_: str | None = None,
    description_: str | None = None,
    include_in_schema: bool = True,
) -> OutboxSubscriber:
    # Knob validation lives in OutboxSubscriberConfig.__post_init__ — constructing the
    # config below validates it, so every construction path is guarded (not just this one).
    usecase_config = OutboxSubscriberConfig(
        _outer_config=config,
        _ack_policy=ack_policy if ack_policy is not None else EMPTY,
        queues=queues,
        max_workers=max_workers,
        retry_strategy=retry_strategy,
        fetch_batch_size=fetch_batch_size,
        min_fetch_interval=min_fetch_interval,
        max_fetch_interval=max_fetch_interval,
        lease_ttl_seconds=lease_ttl_seconds,
        max_deliveries=max_deliveries,
        propagate_inbound_headers=propagate_inbound_headers,
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
