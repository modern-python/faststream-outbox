"""Config dataclasses for the outbox publisher (usecase + AsyncAPI spec)."""

import typing
from collections.abc import Sequence
from dataclasses import dataclass, field

from faststream._internal.configs import PublisherSpecificationConfig, PublisherUsecaseConfig


if typing.TYPE_CHECKING:
    from faststream._internal.types import PublisherMiddleware

    from faststream_outbox.configs import OutboxBrokerConfig


@dataclass(kw_only=True)
class OutboxPublisherConfig(PublisherUsecaseConfig):
    _outer_config: "OutboxBrokerConfig"
    queue: str
    headers: dict[str, str] | None = None
    middlewares: Sequence["PublisherMiddleware[typing.Any]"] = field(default_factory=tuple)


@dataclass(kw_only=True)
class OutboxPublisherSpecificationConfig(PublisherSpecificationConfig):
    queue: str
