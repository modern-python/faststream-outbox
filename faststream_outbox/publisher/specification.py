"""AsyncAPI specification class for ``OutboxPublisher``."""

from faststream._internal.endpoint.publisher import PublisherSpecification
from faststream.specification.asyncapi.utils import resolve_payloads
from faststream.specification.schema import Message, Operation, PublisherSpec

from faststream_outbox.configs import OutboxBrokerConfig
from faststream_outbox.publisher.config import OutboxPublisherSpecificationConfig


class OutboxPublisherSpecification(
    PublisherSpecification[OutboxBrokerConfig, OutboxPublisherSpecificationConfig],
):
    @property
    def name(self) -> str:
        if self.config.title_:
            return self.config.title_
        return f"{self.config.queue}:Publisher"

    def get_schema(self) -> dict[str, PublisherSpec]:
        payloads = self.get_payloads()
        return {
            self.name: PublisherSpec(
                description=self.config.description_,
                operation=Operation(
                    message=Message(
                        title=f"{self.name}:Message",
                        payload=resolve_payloads(payloads, "Publisher"),
                    ),
                    bindings=None,
                ),
                bindings=None,
            ),
        }
