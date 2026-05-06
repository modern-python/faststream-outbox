import asyncio
import datetime as _dt
from collections.abc import Callable

import pytest
from sqlalchemy import MetaData

from faststream_outbox import (
    ConstantRetry,
    OutboxBroker,
    TestOutboxBroker,
    encode_payload,
    make_outbox_table,
)


def _make_broker() -> OutboxBroker:
    metadata = MetaData()
    t = make_outbox_table(metadata)
    return OutboxBroker(outbox_table=t)


async def _wait_until(predicate: Callable[[], object], *, timeout: float = 2.0) -> None:  # noqa: ASYNC109
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.05)
    msg = "timed out waiting for predicate"
    raise AssertionError(msg)


async def test_fake_broker_delivers_to_handler() -> None:
    broker = _make_broker()
    received: list[dict] = []

    @broker.subscriber("orders", min_fetch_interval=0.01, max_fetch_interval=0.05)
    async def handle(body: dict) -> None:
        received.append(body)

    test_broker = TestOutboxBroker(broker)
    async with test_broker:
        payload, headers = encode_payload({"order_id": 1})
        test_broker.feed("orders", payload, headers=headers)
        await _wait_until(lambda: len(received) == 1)

    assert received == [{"order_id": 1}]
    assert test_broker.fake_client.rows == []  # row deleted after ack


async def test_fake_broker_multi_queue_subscriber() -> None:
    broker = _make_broker()
    seen: list[str] = []

    @broker.subscriber(["orders", "shipments"], min_fetch_interval=0.01, max_fetch_interval=0.05)
    async def handle(body: str) -> None:
        seen.append(body)

    test_broker = TestOutboxBroker(broker)
    async with test_broker:
        p1, h1 = encode_payload("o-1")
        p2, h2 = encode_payload("s-1")
        test_broker.feed("orders", p1, headers=h1)
        test_broker.feed("shipments", p2, headers=h2)
        await _wait_until(lambda: len(seen) == 2)

    assert sorted(seen) == ["o-1", "s-1"]


async def test_fake_broker_ignores_other_queues() -> None:
    broker = _make_broker()
    received: list[str] = []

    @broker.subscriber("orders", min_fetch_interval=0.01, max_fetch_interval=0.05)
    async def handle(body: str) -> None:
        received.append(body)

    test_broker = TestOutboxBroker(broker)
    async with test_broker:
        p, h = encode_payload("not-mine")
        test_broker.feed("other-queue", p, headers=h)
        await asyncio.sleep(0.2)

    assert received == []
    # Row stays in fake client because no subscriber for that queue
    assert len(test_broker.fake_client.rows) == 1


async def test_fake_broker_failing_handler_with_no_retry_deletes_row() -> None:
    broker = _make_broker()

    @broker.subscriber("orders", min_fetch_interval=0.01, max_fetch_interval=0.05)
    async def handle(body: str) -> None:
        del body
        msg = "boom"
        raise RuntimeError(msg)

    test_broker = TestOutboxBroker(broker)
    async with test_broker:
        p, h = encode_payload("x")
        test_broker.feed("orders", p, headers=h)
        await _wait_until(lambda: not test_broker.fake_client.rows, timeout=2.0)


async def test_fake_broker_failing_handler_with_retry_reschedules() -> None:
    broker = _make_broker()
    attempts: list[str] = []

    @broker.subscriber(
        "orders",
        min_fetch_interval=0.01,
        max_fetch_interval=0.05,
        retry_strategy=ConstantRetry(delay_seconds=0.05, max_attempts=3),
    )
    async def handle(body: str) -> None:
        attempts.append(body)
        if len(attempts) < 3:
            msg = "transient"
            raise RuntimeError(msg)

    test_broker = TestOutboxBroker(broker)
    async with test_broker:
        p, h = encode_payload("retry-me")
        test_broker.feed("orders", p, headers=h)
        await _wait_until(lambda: len(attempts) == 3, timeout=3.0)
        await _wait_until(lambda: not test_broker.fake_client.rows, timeout=1.0)


async def test_fake_broker_max_deliveries_drops_row() -> None:
    broker = _make_broker()
    handler_called: list[str] = []

    @broker.subscriber(
        "orders",
        min_fetch_interval=0.01,
        max_fetch_interval=0.05,
        max_deliveries=1,
    )
    async def handle(body: str) -> None:
        handler_called.append(body)

    test_broker = TestOutboxBroker(broker)
    async with test_broker:
        p, h = encode_payload("never")
        test_broker.feed("orders", p, headers=h)
        # Pre-bump deliveries_count so allow_delivery rejects on first claim
        test_broker.fake_client.rows[0].deliveries_count = 5  # already over the cap
        await _wait_until(lambda: not test_broker.fake_client.rows, timeout=2.0)

    assert handler_called == []


async def test_fake_broker_correlation_id_in_handler_context() -> None:
    from faststream import Context  # noqa: PLC0415

    broker = _make_broker()
    seen: list[str] = []

    @broker.subscriber("orders", min_fetch_interval=0.01, max_fetch_interval=0.05)
    async def handle(body: dict, correlation_id: str = Context("message.correlation_id")) -> None:
        del body
        seen.append(correlation_id)

    test_broker = TestOutboxBroker(broker)
    async with test_broker:
        p, h = encode_payload({"x": 1}, correlation_id="trace-xyz")
        test_broker.feed("orders", p, headers=h)
        await _wait_until(lambda: seen, timeout=2.0)

    assert seen == ["trace-xyz"]


async def test_fake_broker_release_stuck_recovers_processing_row() -> None:
    broker = _make_broker()
    received: list[str] = []

    @broker.subscriber(
        "orders",
        min_fetch_interval=0.01,
        max_fetch_interval=0.05,
        release_stuck_timeout=0.1,
        release_stuck_interval=0.05,
    )
    async def handle(body: str) -> None:
        received.append(body)

    test_broker = TestOutboxBroker(broker)
    async with test_broker:
        # Manually create a "stuck" processing row with old acquired_at
        import uuid as _uuid  # noqa: PLC0415

        from faststream_outbox.schema import OutboxState  # noqa: PLC0415
        from faststream_outbox.testing import _FakeRow  # noqa: PLC0415

        old = _dt.datetime.now(tz=_dt.UTC) - _dt.timedelta(seconds=10)
        test_broker.fake_client._rows.append(  # noqa: SLF001
            _FakeRow(
                id=99,
                queue="orders",
                payload=encode_payload("stuck-payload")[0],
                headers=encode_payload("stuck-payload")[1],
                state=OutboxState.PROCESSING.value,
                acquired_at=old,
                acquired_token=_uuid.uuid4(),
            )
        )
        test_broker.fake_client._next_id = 100  # noqa: SLF001
        await _wait_until(lambda: received, timeout=3.0)


async def test_fake_broker_no_handler_no_dispatch() -> None:
    broker = _make_broker()
    test_broker = TestOutboxBroker(broker)
    async with test_broker:
        # No subscribers registered — feeding a row should leave it in fake client
        p, h = encode_payload("nope")
        test_broker.feed("orders", p, headers=h)
        await asyncio.sleep(0.1)
    assert len(test_broker.fake_client.rows) == 1


async def test_fake_broker_publish_raises() -> None:
    broker = _make_broker()
    test_broker = TestOutboxBroker(broker)
    async with test_broker:
        with pytest.raises(NotImplementedError):
            await broker.publish(b"x")
