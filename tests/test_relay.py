import logging
from typing import Any
from unittest import mock

import pytest
from faststream.kafka import KafkaBroker, KafkaRouter, TestKafkaBroker
from faststream.response import Response
from sqlalchemy import MetaData

from faststream_outbox import OutboxBroker, OutboxResponse, OutboxRouter, make_outbox_table
from faststream_outbox.testing import TestOutboxBroker


pytestmark = pytest.mark.asyncio


async def test_naked_decorator_chain_relays_plain_return_to_kafka() -> None:
    """
    A handler decorated `@kafka_pub @outbox.subscriber(...)` returning plain value.

    This publishes the value through the Kafka publisher chain.
    """
    metadata = MetaData()
    outbox_table = make_outbox_table(metadata)
    broker_outbox = OutboxBroker(outbox_table=outbox_table)
    broker_kafka = KafkaBroker("kafka://test:9092")
    publisher_kafka = broker_kafka.publisher("relay_topic")

    @publisher_kafka
    @broker_outbox.subscriber("relay_queue")
    async def relay(body: dict[str, Any]) -> dict[str, Any]:
        return body

    # TestKafkaBroker first, TestOutboxBroker second: the foreign broker's mock producer
    # must be wired before our subscriber starts, because the outbox's start() probes
    # foreign producers and warns about any whose broker isn't started yet.
    async with TestKafkaBroker(broker_kafka), TestOutboxBroker(broker_outbox, run_loops=False) as outbox:
        await outbox.publish({"hello": "world"}, queue="relay_queue", session=None)  # ty: ignore[invalid-argument-type]
        publisher_kafka.mock.assert_called_once_with({"hello": "world"})


async def test_relay_via_kafka_router_publisher() -> None:
    """
    Relay via KafkaRouter publisher works like broker-direct publisher.

    Confirms ConfigComposition.add_config correctly prepends broker config
    so publisher._outer_config.producer resolves at publish time.
    """
    metadata = MetaData()
    outbox_table = make_outbox_table(metadata)
    broker_outbox = OutboxBroker(outbox_table=outbox_table)
    broker_kafka = KafkaBroker("kafka://test:9092")
    kafka_router = KafkaRouter()
    publisher_kafka = kafka_router.publisher("relay_topic")

    @publisher_kafka
    @broker_outbox.subscriber("relay_queue")
    async def relay(body: dict[str, Any]) -> dict[str, Any]:
        return body

    broker_kafka.include_router(kafka_router)

    async with TestKafkaBroker(broker_kafka), TestOutboxBroker(broker_outbox, run_loops=False) as outbox:
        await outbox.publish({"router": True}, queue="relay_queue", session=None)  # ty: ignore[invalid-argument-type]
        publisher_kafka.mock.assert_called_once_with({"router": True})


async def test_relay_via_outbox_router_subscriber() -> None:
    """
    Relay via OutboxRouter subscriber works like broker-direct subscriber.

    Confirms the symmetric outbox-side router shape with foreign-decorated
    subscribers from an OutboxRouter the same as broker-direct ones.
    """
    metadata = MetaData()
    outbox_table = make_outbox_table(metadata)
    broker_outbox = OutboxBroker(outbox_table=outbox_table)
    broker_kafka = KafkaBroker("kafka://test:9092")
    publisher_kafka = broker_kafka.publisher("relay_topic")
    outbox_router = OutboxRouter()

    @publisher_kafka
    @outbox_router.subscriber("relay_queue")
    async def relay(body: dict[str, Any]) -> dict[str, Any]:
        return body

    broker_outbox.include_router(outbox_router)

    async with TestKafkaBroker(broker_kafka), TestOutboxBroker(broker_outbox, run_loops=False) as outbox:
        await outbox.publish({"outbox_router": True}, queue="relay_queue", session=None)  # ty: ignore[invalid-argument-type]
        publisher_kafka.mock.assert_called_once_with({"outbox_router": True})


async def test_propagate_inbound_headers_true_forwards_outbox_headers_to_kafka() -> None:
    """
    With propagate_inbound_headers=True, inbound headers are forwarded to the relay.

    The headers are placed onto the Response before the foreign-publisher chain fires.
    """
    metadata = MetaData()
    outbox_table = make_outbox_table(metadata)
    broker_outbox = OutboxBroker(outbox_table=outbox_table)
    broker_kafka = KafkaBroker("kafka://test:9092")
    publisher_kafka = broker_kafka.publisher("relay_topic")

    @publisher_kafka
    @broker_outbox.subscriber("relay_queue", propagate_inbound_headers=True)
    async def relay(body: dict[str, Any]) -> dict[str, Any]:
        return body

    captured: list[dict[str, str]] = []
    original_publish = publisher_kafka._publish  # noqa: SLF001

    async def capture_publish(cmd: Any, **kwargs: Any) -> Any:
        captured.append(dict(cmd.headers))
        return await original_publish(cmd, **kwargs)

    async with TestKafkaBroker(broker_kafka), TestOutboxBroker(broker_outbox, run_loops=False) as outbox:
        with mock.patch.object(publisher_kafka, "_publish", side_effect=capture_publish):
            await outbox.publish(
                {"hi": 1},
                queue="relay_queue",
                session=None,  # ty: ignore[invalid-argument-type]
                headers={"x-trace-id": "abc123", "content-type": "application/json"},
            )

    assert len(captured) == 1, f"Expected one publish, got {len(captured)}"
    assert captured[0].get("x-trace-id") == "abc123"
    assert captured[0].get("content-type") == "application/json"


async def test_propagate_inbound_headers_false_drops_inbound_headers() -> None:
    """
    Default propagate_inbound_headers=False drops inbound headers from the relay.

    Response.headers stays empty even when the inbound outbox row carries headers.
    """
    metadata = MetaData()
    outbox_table = make_outbox_table(metadata)
    broker_outbox = OutboxBroker(outbox_table=outbox_table)
    broker_kafka = KafkaBroker("kafka://test:9092")
    publisher_kafka = broker_kafka.publisher("relay_topic")

    @publisher_kafka
    @broker_outbox.subscriber("relay_queue")  # default: propagate_inbound_headers=False
    async def relay(body: dict[str, Any]) -> dict[str, Any]:
        return body

    captured: list[dict[str, str]] = []
    original_publish = publisher_kafka._publish  # noqa: SLF001

    async def capture_publish(cmd: Any, **kwargs: Any) -> Any:
        captured.append(dict(cmd.headers))
        return await original_publish(cmd, **kwargs)

    async with TestKafkaBroker(broker_kafka), TestOutboxBroker(broker_outbox, run_loops=False) as outbox:
        with mock.patch.object(publisher_kafka, "_publish", side_effect=capture_publish):
            await outbox.publish(
                {"hi": 1},
                queue="relay_queue",
                session=None,  # ty: ignore[invalid-argument-type]
                headers={"x-trace-id": "should-be-dropped"},
            )

    assert len(captured) == 1
    assert "x-trace-id" not in captured[0]


async def test_propagate_inbound_headers_true_does_not_override_explicit_response_headers() -> None:
    """
    Even with propagate_inbound_headers=True, explicit user-set Response headers win.

    The subscriber only fills headers when ``result_msg.headers`` is empty;
    a handler that returns ``Response(value, headers=...)`` keeps its choice.
    """
    metadata = MetaData()
    outbox_table = make_outbox_table(metadata)
    broker_outbox = OutboxBroker(outbox_table=outbox_table)
    broker_kafka = KafkaBroker("kafka://test:9092")
    publisher_kafka = broker_kafka.publisher("relay_topic")

    @publisher_kafka
    @broker_outbox.subscriber("relay_queue", propagate_inbound_headers=True)
    async def relay(body: dict[str, Any]) -> Response:
        return Response(body, headers={"x-explicit": "set-by-handler"})

    captured: list[dict[str, str]] = []
    original_publish = publisher_kafka._publish  # noqa: SLF001

    async def capture_publish(cmd: Any, **kwargs: Any) -> Any:
        captured.append(dict(cmd.headers))
        return await original_publish(cmd, **kwargs)

    async with TestKafkaBroker(broker_kafka), TestOutboxBroker(broker_outbox, run_loops=False) as outbox:
        with mock.patch.object(publisher_kafka, "_publish", side_effect=capture_publish):
            await outbox.publish(
                {"hi": 1},
                queue="relay_queue",
                session=None,  # ty: ignore[invalid-argument-type]
                headers={"x-trace-id": "should-not-clobber"},
            )

    assert len(captured) == 1
    assert captured[0].get("x-explicit") == "set-by-handler"
    assert "x-trace-id" not in captured[0]


async def test_outbox_response_with_foreign_publisher_raises() -> None:
    """
    A handler that returns OutboxResponse and is decorated by a foreign publisher raises.

    The guard fires at dispatch time so the user does not silently
    dual-fire (row in outbox + Kafka publish).
    """
    metadata = MetaData()
    outbox_table = make_outbox_table(metadata)
    broker_outbox = OutboxBroker(outbox_table=outbox_table)
    broker_kafka = KafkaBroker("kafka://test:9092")
    publisher_kafka = broker_kafka.publisher("relay_topic")

    @publisher_kafka
    @broker_outbox.subscriber("relay_queue")
    async def relay(body: dict[str, Any]) -> OutboxResponse:
        return OutboxResponse(body=body, queue="next_queue", session=None)  # ty: ignore[invalid-argument-type]

    async with TestKafkaBroker(broker_kafka), TestOutboxBroker(broker_outbox, run_loops=False) as outbox:
        with pytest.raises(RuntimeError, match="OutboxResponse"):
            await outbox.publish({"x": 1}, queue="relay_queue", session=None)  # ty: ignore[invalid-argument-type]


async def test_outbox_response_with_foreign_publisher_fires_guard_before_side_effects() -> None:
    """
    T5: the dual-fire guard must raise BEFORE the publisher chain runs.

    Moving the guard below the chain still raises (so ``pytest.raises`` passes), but the
    Kafka publish AND the follow-on outbox insert have already happened by then. Pin the
    ordering: when the guard fires, neither side effect occurs.
    """
    metadata = MetaData()
    outbox_table = make_outbox_table(metadata)
    broker_outbox = OutboxBroker(outbox_table=outbox_table)
    broker_kafka = KafkaBroker("kafka://test:9092")
    publisher_kafka = broker_kafka.publisher("relay_topic")

    @publisher_kafka
    @broker_outbox.subscriber("relay_queue")
    async def relay(body: dict[str, Any]) -> OutboxResponse:
        return OutboxResponse(body=body, queue="next_queue", session=None)  # ty: ignore[invalid-argument-type]

    test_outbox = TestOutboxBroker(broker_outbox, run_loops=False)
    async with TestKafkaBroker(broker_kafka), test_outbox as outbox:
        with pytest.raises(RuntimeError, match="OutboxResponse"):
            await outbox.publish({"x": 1}, queue="relay_queue", session=None)  # ty: ignore[invalid-argument-type]
        # Guard fired before the chain → the foreign Kafka publish never happened…
        publisher_kafka.mock.assert_not_called()
        # …and no follow-on outbox row was inserted for the relayed body.
        assert not any(r.queue == "next_queue" for r in test_outbox.fake_client.rows)


async def test_unstarted_foreign_broker_warns_on_start(caplog: pytest.LogCaptureFixture) -> None:
    """
    Log one WARNING per unstarted foreign broker at start() time.

    If a foreign-publisher decorator is on an outbox subscriber but the
    foreign broker has not been started, start(broker_outbox) logs a single
    WARNING per unstarted foreign broker.
    """
    metadata = MetaData()
    outbox_table = make_outbox_table(metadata)
    broker_outbox = OutboxBroker(outbox_table=outbox_table)
    broker_kafka = KafkaBroker("kafka://test:9092")
    publisher_kafka = broker_kafka.publisher("relay_topic")

    @publisher_kafka
    @broker_outbox.subscriber("relay_queue")
    async def relay(body: dict[str, Any]) -> dict[str, Any]:
        return body  # pragma: no cover  # handler never invoked — test only exercises start()

    with caplog.at_level(logging.WARNING, logger="faststream_outbox"):
        async with TestOutboxBroker(broker_outbox, run_loops=False):
            pass  # start triggered inside __aenter__

    matching = [r for r in caplog.records if r.levelno == logging.WARNING and "relay_queue" in r.getMessage()]
    assert len(matching) == 1, (
        f"Expected exactly one WARNING referencing relay_queue, got {len(matching)}: "
        f"{[r.getMessage() for r in caplog.records]}"
    )


async def test_started_foreign_broker_does_not_warn_on_start(caplog: pytest.LogCaptureFixture) -> None:
    """Negative of the unstarted-broker warning: a STARTED foreign broker emits no warning at start()."""
    metadata = MetaData()
    outbox_table = make_outbox_table(metadata)
    broker_outbox = OutboxBroker(outbox_table=outbox_table)
    broker_kafka = KafkaBroker("kafka://test:9092")
    publisher_kafka = broker_kafka.publisher("relay_topic")

    @publisher_kafka
    @broker_outbox.subscriber("relay_queue")
    async def relay(body: dict[str, Any]) -> dict[str, Any]:
        return body  # pragma: no cover — never invoked; the test only exercises start()

    with caplog.at_level(logging.WARNING, logger="faststream_outbox"):
        # TestKafkaBroker wires the foreign producer before the outbox starts, so the
        # start()-time probe sees it as started and stays silent.
        async with TestKafkaBroker(broker_kafka), TestOutboxBroker(broker_outbox, run_loops=False):
            pass

    foreign = [r.getMessage() for r in caplog.records if "has not been started" in r.getMessage()]
    assert foreign == []


async def test_foreign_warning_names_only_decorated_subscriber_queues(caplog: pytest.LogCaptureFixture) -> None:
    """P21: the unstarted-foreign-broker warning names the decorated subscriber's queue, not every queue."""
    metadata = MetaData()
    outbox_table = make_outbox_table(metadata)
    broker_outbox = OutboxBroker(outbox_table=outbox_table)
    broker_kafka = KafkaBroker("kafka://test:9092")
    publisher_kafka = broker_kafka.publisher("relay_topic")

    @publisher_kafka
    @broker_outbox.subscriber("relay_queue")
    async def relay(body: dict[str, Any]) -> dict[str, Any]:
        return body  # pragma: no cover  # never invoked — test only exercises start()

    @broker_outbox.subscriber("unrelated_queue")
    async def plain(body: dict[str, Any]) -> None: ...  # a second, foreign-publisher-free subscriber

    with caplog.at_level(logging.WARNING, logger="faststream_outbox"):
        async with TestOutboxBroker(broker_outbox, run_loops=False):
            pass

    foreign = [r.getMessage() for r in caplog.records if "has not been started" in r.getMessage()]
    assert len(foreign) == 1
    assert "relay_queue" in foreign[0]
    assert "unrelated_queue" not in foreign[0]  # the old code listed every subscriber's queues
