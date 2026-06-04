from typing import Any

import pytest
from sqlalchemy import MetaData
from faststream.kafka import KafkaBroker, TestKafkaBroker

from faststream_outbox import OutboxBroker, make_outbox_table
from faststream_outbox.testing import TestOutboxBroker


pytestmark = pytest.mark.asyncio


async def test_naked_decorator_chain_relays_plain_return_to_kafka() -> None:
    """A handler decorated `@kafka_pub @outbox.subscriber(...)` returning a plain
    value publishes the value through the Kafka publisher chain."""
    metadata = MetaData()
    outbox_table = make_outbox_table(metadata)
    broker_outbox = OutboxBroker(outbox_table=outbox_table)
    broker_kafka = KafkaBroker("kafka://test:9092")
    publisher_kafka = broker_kafka.publisher("relay_topic")

    @publisher_kafka
    @broker_outbox.subscriber("relay_queue")
    async def relay(body: dict[str, Any]) -> dict[str, Any]:
        return body

    # TestKafkaBroker first, TestOutboxBroker second — see "Context-manager
    # ordering note" at the end of this plan. The foreign broker's mock producer
    # must be wired before our subscriber starts (Task 6 introduces a startup
    # warning that probes foreign producers).
    async with TestKafkaBroker(broker_kafka), TestOutboxBroker(broker_outbox, run_loops=False) as outbox:
        await outbox.publish({"hello": "world"}, queue="relay_queue", session=None)
        publisher_kafka.mock.assert_called_once_with({"hello": "world"})
