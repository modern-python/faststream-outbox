"""Config dataclasses for the outbox publisher (usecase + AsyncAPI spec)."""

import typing
from dataclasses import dataclass

from faststream._internal.configs import PublisherSpecificationConfig, PublisherUsecaseConfig


if typing.TYPE_CHECKING:
    from faststream_outbox.configs import OutboxBrokerConfig


@dataclass(kw_only=True)
class OutboxPublisherConfig(PublisherUsecaseConfig):
    _outer_config: "OutboxBrokerConfig"
    queue: str
    headers: dict[str, str] | None = None


@dataclass(kw_only=True)
class OutboxPublisherSpecificationConfig(PublisherSpecificationConfig):
    queue: str
