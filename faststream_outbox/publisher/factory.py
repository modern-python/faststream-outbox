"""Factory that constructs an ``OutboxPublisher`` + its config + spec."""

import typing

from faststream_outbox.publisher.config import OutboxPublisherConfig, OutboxPublisherSpecificationConfig
from faststream_outbox.publisher.specification import OutboxPublisherSpecification
from faststream_outbox.publisher.usecase import OutboxPublisher


if typing.TYPE_CHECKING:
    from faststream_outbox.configs import OutboxBrokerConfig


def create_publisher(  # noqa: PLR0913
    *,
    queue: str,
    headers: dict[str, str] | None,
    broker_config: "OutboxBrokerConfig",
    title_: str | None,
    description_: str | None,
    schema_: typing.Any | None,
    include_in_schema: bool,
) -> OutboxPublisher:
    publisher_config = OutboxPublisherConfig(
        _outer_config=broker_config,
        queue=queue,
        headers=headers,
    )
    specification = OutboxPublisherSpecification(
        _outer_config=broker_config,
        specification_config=OutboxPublisherSpecificationConfig(
            queue=queue,
            title_=title_,
            description_=description_,
            schema_=schema_,
            include_in_schema=include_in_schema,
        ),
    )
    return OutboxPublisher(config=publisher_config, specification=specification)
