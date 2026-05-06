import datetime as _dt
import typing
from collections.abc import Callable
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
    release_stuck_timeout: float
    release_stuck_interval: float
    max_deliveries: int | None

    @property
    def full_queues(self) -> list[str]:
        prefix = self._outer_config.prefix or ""
        return [f"{prefix}{q}" for q in self.queues]

    @property
    def time_source(self) -> Callable[[], _dt.datetime]:
        return self._outer_config.time_source

    @property
    def ack_policy(self) -> AckPolicy:
        if self._ack_policy is EMPTY:
            return AckPolicy.NACK_ON_ERROR
        return self._ack_policy  # pragma: no cover


@dataclass(kw_only=True)
class OutboxSubscriberSpecificationConfig(SubscriberSpecificationConfig):
    queues: list[str]
