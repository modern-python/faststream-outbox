import typing
from dataclasses import dataclass

from faststream._internal.configs import SubscriberSpecificationConfig, SubscriberUsecaseConfig
from faststream._internal.constants import EMPTY
from faststream.middlewares import AckPolicy


if typing.TYPE_CHECKING:
    from faststream_outbox.configs import OutboxBrokerConfig
    from faststream_outbox.retry import RetryStrategyProto


@dataclass(kw_only=True)
class OutboxSubscriberConfig(SubscriberUsecaseConfig):
    _outer_config: "OutboxBrokerConfig"
    queues: list[str]
    max_workers: int
    retry_strategy: "RetryStrategyProto | None"
    fetch_batch_size: int
    min_fetch_interval: float
    max_fetch_interval: float
    lease_ttl_seconds: float
    max_deliveries: int | None
    propagate_inbound_headers: bool

    @property
    def ack_policy(self) -> AckPolicy:
        if self._ack_policy is EMPTY:
            return AckPolicy.NACK_ON_ERROR
        return self._ack_policy


@dataclass(kw_only=True)
class OutboxSubscriberSpecificationConfig(SubscriberSpecificationConfig):
    queues: list[str]
