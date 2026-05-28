import asyncio
import datetime as _dt
import uuid
import warnings as _warnings
from collections.abc import Callable
from unittest.mock import AsyncMock

import pytest
from faststream.middlewares import AckPolicy
from sqlalchemy import MetaData
from sqlalchemy.ext.asyncio import AsyncSession

from faststream_outbox import (
    ConstantRetry,
    NoRetry,
    OutboxBroker,
    OutboxRouter,
    RetryStrategyProto,
    TestOutboxBroker,
    make_outbox_table,
)
from faststream_outbox.configs import OutboxBrokerConfig
from faststream_outbox.envelope import _encode_payload as encode_payload
from faststream_outbox.subscriber.config import OutboxSubscriberConfig
from faststream_outbox.testing import FakeOutboxClient, _FakeRow


def _fake_session() -> AsyncMock:
    """
    Build an ``AsyncMock(spec=AsyncSession)`` for tests where the session is ignored.

    ``OutboxPublishCommand`` requires an ``AsyncSession``; the fake producer doesn't
    touch it. The mock passes ``isinstance`` and lets publisher tests focus on the
    fake-store side effects.
    """
    return AsyncMock(spec=AsyncSession)


def _make_broker() -> OutboxBroker:
    metadata = MetaData()
    t = make_outbox_table(metadata)
    return OutboxBroker(outbox_table=t)


async def _wait_until(predicate: Callable[[], object], *, timeout: float = 2.0) -> None:  # noqa: ASYNC109
    # Used by run_loops=True tests; sync-mode tests assert directly after publish.
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.05)
    msg = "timed out waiting for predicate"  # pragma: no cover
    raise AssertionError(msg)  # pragma: no cover


# --- Sync-mode tests (default TestOutboxBroker) ---------------------------------------


async def test_fake_broker_publish_triggers_handler() -> None:
    """``broker.publish`` synchronously dispatches the handler, FastStream-test-broker style."""
    broker = _make_broker()
    received: list[dict] = []

    @broker.subscriber("orders")
    async def handle(body: dict) -> None:
        received.append(body)

    test_broker = TestOutboxBroker(broker)
    async with test_broker:
        await broker.publish({"order_id": 1}, queue="orders")  # ty: ignore[missing-argument]

    assert received == [{"order_id": 1}]
    assert test_broker.fake_client.rows == []  # row deleted after ack


async def test_fake_broker_publish_batch_triggers_handler() -> None:
    broker = _make_broker()
    received: list[str] = []

    @broker.subscriber("orders")
    async def handle(body: str) -> None:
        received.append(body)

    test_broker = TestOutboxBroker(broker)
    async with test_broker:
        await broker.publish_batch("a", "b", "c", queue="orders")  # ty: ignore[missing-argument]

    assert received == ["a", "b", "c"]
    assert test_broker.fake_client.rows == []


async def test_fake_broker_multi_queue_subscriber() -> None:
    broker = _make_broker()
    seen: list[str] = []

    @broker.subscriber(["orders", "shipments"])
    async def handle(body: str) -> None:
        seen.append(body)

    test_broker = TestOutboxBroker(broker)
    async with test_broker:
        await broker.publish("o-1", queue="orders")  # ty: ignore[missing-argument]
        await broker.publish("s-1", queue="shipments")  # ty: ignore[missing-argument]

    assert seen == ["o-1", "s-1"]


async def test_fake_broker_publish_to_unhandled_queue_leaves_row() -> None:
    """Publishing to a queue with no matching subscriber leaves the row in the fake client."""
    broker = _make_broker()

    @broker.subscriber("orders")
    async def handle(body: str) -> None: ...

    test_broker = TestOutboxBroker(broker)
    async with test_broker:
        await broker.publish("not-mine", queue="other-queue")  # ty: ignore[missing-argument]

    assert len(test_broker.fake_client.rows) == 1
    assert test_broker.fake_client.rows[0].queue == "other-queue"


async def test_fake_broker_failing_handler_with_no_retry_deletes_row() -> None:
    broker = _make_broker()

    @broker.subscriber("orders", retry_strategy=NoRetry())
    async def handle(body: str) -> None:
        del body
        msg = "boom"
        raise RuntimeError(msg)

    test_broker = TestOutboxBroker(broker)
    async with test_broker:
        await broker.publish("x", queue="orders")  # ty: ignore[missing-argument]

    assert test_broker.fake_client.rows == []


async def test_fake_broker_failing_handler_with_default_retry_keeps_row() -> None:
    """The default retry policy must reschedule (not delete) on a transient handler error."""
    broker = _make_broker()

    @broker.subscriber("orders")
    async def handle(body: str) -> None:
        del body
        msg = "boom"
        raise RuntimeError(msg)

    test_broker = TestOutboxBroker(broker)
    async with test_broker:
        await broker.publish("x", queue="orders")  # ty: ignore[missing-argument]

    assert len(test_broker.fake_client.rows) == 1
    row = test_broker.fake_client.rows[0]
    assert row.attempts_count == 1
    assert row.next_attempt_at > _dt.datetime.now(tz=_dt.UTC)  # rescheduled, not deleted


async def test_fake_broker_correlation_id_in_handler_context() -> None:
    from faststream import Context  # noqa: PLC0415

    broker = _make_broker()
    seen: list[str] = []

    @broker.subscriber("orders")
    async def handle(body: dict, correlation_id: str = Context("message.correlation_id")) -> None:
        del body
        seen.append(correlation_id)

    test_broker = TestOutboxBroker(broker)
    async with test_broker:
        await broker.publish({"x": 1}, queue="orders", correlation_id="trace-xyz")  # ty: ignore[missing-argument]

    assert seen == ["trace-xyz"]


async def test_fake_broker_no_handler_no_dispatch() -> None:
    broker = _make_broker()
    test_broker = TestOutboxBroker(broker)
    async with test_broker:
        # No subscribers registered — publishing should leave the row in the fake client.
        await broker.publish("nope", queue="orders")  # ty: ignore[missing-argument]
    assert len(test_broker.fake_client.rows) == 1


async def test_fake_broker_request_raises() -> None:
    broker = _make_broker()
    test_broker = TestOutboxBroker(broker)
    async with test_broker:
        with pytest.raises(NotImplementedError):
            await broker.request(b"x")


async def test_fake_broker_publish_with_timer_id_dedups() -> None:
    broker = _make_broker()
    # No subscriber for "timers": rows persist so we can observe dedup behavior.
    test_broker = TestOutboxBroker(broker)
    async with test_broker:
        first = await broker.publish("x", queue="timers", timer_id="email-1")  # ty: ignore[missing-argument]
        second = await broker.publish("y", queue="timers", timer_id="email-1")  # ty: ignore[missing-argument]
    assert first is not None
    assert second is None
    assert len(test_broker.fake_client.rows) == 1


async def test_fake_broker_publish_with_activate_in_dispatches_immediately() -> None:
    """Sync mode ignores activate_in — timers fire immediately."""
    broker = _make_broker()
    received: list[str] = []

    @broker.subscriber("orders")
    async def handle(body: str) -> None:
        received.append(body)

    test_broker = TestOutboxBroker(broker)
    async with test_broker:
        await broker.publish(  # ty: ignore[missing-argument]
            "delayed",
            queue="orders",
            activate_in=_dt.timedelta(seconds=5),
        )

    assert received == ["delayed"]
    assert test_broker.fake_client.rows == []


async def test_fake_broker_publish_with_activate_at_dispatches_immediately() -> None:
    broker = _make_broker()
    received: list[str] = []

    @broker.subscriber("orders")
    async def handle(body: str) -> None:
        received.append(body)

    test_broker = TestOutboxBroker(broker)
    async with test_broker:
        future = _dt.datetime.now(tz=_dt.UTC) + _dt.timedelta(seconds=5)
        await broker.publish("at-future", queue="orders", activate_at=future)  # ty: ignore[missing-argument]

    assert received == ["at-future"]
    assert test_broker.fake_client.rows == []


async def test_fake_broker_publish_batch_with_activate_in_dispatches_immediately() -> None:
    broker = _make_broker()
    received: list[str] = []

    @broker.subscriber("orders")
    async def handle(body: str) -> None:
        received.append(body)

    test_broker = TestOutboxBroker(broker)
    async with test_broker:
        await broker.publish_batch(  # ty: ignore[missing-argument]
            "a",
            "b",
            queue="orders",
            activate_in=_dt.timedelta(seconds=5),
        )

    assert received == ["a", "b"]
    assert test_broker.fake_client.rows == []


async def test_fake_broker_publish_batch_with_activate_at_dispatches_immediately() -> None:
    broker = _make_broker()
    received: list[str] = []

    @broker.subscriber("orders")
    async def handle(body: str) -> None:
        received.append(body)

    test_broker = TestOutboxBroker(broker)
    async with test_broker:
        future = _dt.datetime.now(tz=_dt.UTC) + _dt.timedelta(seconds=5)
        await broker.publish_batch("a", "b", queue="orders", activate_at=future)  # ty: ignore[missing-argument]

    assert received == ["a", "b"]
    assert test_broker.fake_client.rows == []


async def test_fake_broker_publish_rejects_both_activate_in_and_activate_at() -> None:
    broker = _make_broker()
    test_broker = TestOutboxBroker(broker)
    async with test_broker:
        with pytest.raises(ValueError, match="at most one of activate_in / activate_at"):
            await broker.publish(  # ty: ignore[missing-argument]
                "x",
                queue="orders",
                activate_in=_dt.timedelta(seconds=1),
                activate_at=_dt.datetime.now(tz=_dt.UTC),
            )


async def test_fake_broker_publish_batch_rejects_both_activate_in_and_activate_at() -> None:
    broker = _make_broker()
    test_broker = TestOutboxBroker(broker)
    async with test_broker:
        with pytest.raises(ValueError, match="at most one of activate_in / activate_at"):
            await broker.publish_batch(  # ty: ignore[missing-argument]
                "x",
                queue="orders",
                activate_in=_dt.timedelta(seconds=1),
                activate_at=_dt.datetime.now(tz=_dt.UTC),
            )


async def test_fake_broker_publish_rejects_naive_activate_at() -> None:
    """Parity with real broker: a naive activate_at must raise so tests catch the bug pre-prod."""
    broker = _make_broker()
    test_broker = TestOutboxBroker(broker)
    async with test_broker:
        with pytest.raises(ValueError, match=r"broker\.publish requires activate_at to be timezone-aware"):
            await broker.publish(  # ty: ignore[missing-argument]
                "x",
                queue="orders",
                activate_at=_dt.datetime.now(),  # noqa: DTZ005
            )


async def test_fake_broker_publish_batch_rejects_naive_activate_at() -> None:
    broker = _make_broker()
    test_broker = TestOutboxBroker(broker)
    async with test_broker:
        with pytest.raises(ValueError, match=r"broker\.publish_batch requires activate_at to be timezone-aware"):
            await broker.publish_batch(  # ty: ignore[missing-argument]
                "x",
                queue="orders",
                activate_at=_dt.datetime.now(),  # noqa: DTZ005
            )


async def test_fake_broker_publish_batch_empty_bodies_is_noop() -> None:
    broker = _make_broker()
    test_broker = TestOutboxBroker(broker)
    async with test_broker:
        await broker.publish_batch(queue="orders")  # ty: ignore[missing-argument]
    assert test_broker.fake_client.rows == []


async def test_fake_broker_cancel_timer_removes_row() -> None:
    broker = _make_broker()
    test_broker = TestOutboxBroker(broker)
    # No handler for "timers" — row persists after publish so we can cancel it.
    async with test_broker:
        await broker.publish("x", queue="timers", timer_id="email-1")  # ty: ignore[missing-argument]
        assert len(test_broker.fake_client.rows) == 1
        cancelled = await broker.cancel_timer(queue="timers", timer_id="email-1")  # ty: ignore[missing-argument]
        assert cancelled is True
        assert test_broker.fake_client.rows == []


async def test_fake_broker_fetch_unprocessed_reads_fake_client() -> None:
    """``broker.fetch_unprocessed`` in test mode reads the in-memory store, not SQLAlchemy."""
    broker = _make_broker()
    test_broker = TestOutboxBroker(broker)
    async with test_broker:
        # No subscriber — rows stay in fake client.
        await broker.publish("a", queue="q1")  # ty: ignore[missing-argument]
        await broker.publish("b", queue="q2")  # ty: ignore[missing-argument]

        all_rows = await broker.fetch_unprocessed()  # ty: ignore[missing-argument]
        assert [r.queue for r in all_rows] == ["q1", "q2"]

        q1_only = await broker.fetch_unprocessed(queue="q1")  # ty: ignore[missing-argument]
        assert [r.queue for r in q1_only] == ["q1"]


async def test_fake_broker_router_subscriber_receives_publish() -> None:
    received: list[str] = []
    router = OutboxRouter()

    @router.subscriber("orders")
    async def handle(body: str) -> None:
        received.append(body)

    broker = _make_broker()
    broker.include_router(router)

    test_broker = TestOutboxBroker(broker)
    async with test_broker:
        await broker.publish("via-router", queue="orders")  # ty: ignore[missing-argument]

    assert received == ["via-router"]


async def test_fake_broker_publish_invokes_flush_terminal_when_lease_lost() -> None:
    """``delete_with_lease`` returning False is logged and skipped, not raised."""

    class LeaseLostClient(FakeOutboxClient):
        async def delete_with_lease(self, conn: object, message_id: int, acquired_token: uuid.UUID) -> bool:  # noqa: ARG002
            return False

    broker = _make_broker()
    received: list[str] = []

    @broker.subscriber("orders")
    async def handle(body: str) -> None:
        received.append(body)

    test_broker = TestOutboxBroker(broker)
    test_broker.fake_client = LeaseLostClient()
    async with test_broker:
        await broker.publish("lease-lost", queue="orders")  # ty: ignore[missing-argument]

    assert received == ["lease-lost"]


async def test_fake_broker_publish_invokes_flush_retry_when_lease_lost() -> None:
    """``mark_pending_with_lease`` returning False is logged and skipped on a nacked handler."""

    class LeaseLostRetryClient(FakeOutboxClient):
        async def mark_pending_with_lease(self, *args: object, **kwargs: object) -> bool:  # noqa: ARG002
            return False

    broker = _make_broker()
    attempts: list[str] = []

    @broker.subscriber("orders", retry_strategy=ConstantRetry(delay_seconds=0.05, max_attempts=10))
    async def handle(body: str) -> None:
        attempts.append(body)
        msg = "always fails"
        raise RuntimeError(msg)

    test_broker = TestOutboxBroker(broker)
    test_broker.fake_client = LeaseLostRetryClient()
    async with test_broker:
        await broker.publish("never-cleared", queue="orders")  # ty: ignore[missing-argument]

    assert attempts == ["never-cleared"]


async def test_fake_broker_publish_swallows_post_consume_failure() -> None:
    """``dispatch_one``'s outer except catches a delete that raises, so the next publish still works."""

    class RaisingDeleteClient(FakeOutboxClient):
        def __init__(self) -> None:
            super().__init__()
            self.calls = 0

        async def delete_with_lease(self, conn: object, message_id: int, acquired_token: uuid.UUID) -> bool:
            self.calls += 1
            if self.calls == 1:
                msg = "delete blew up"
                raise RuntimeError(msg)
            return await super().delete_with_lease(conn, message_id, acquired_token)

    broker = _make_broker()
    received: list[str] = []

    @broker.subscriber("orders")
    async def handle(body: str) -> None:
        received.append(body)

    test_broker = TestOutboxBroker(broker)
    raising = RaisingDeleteClient()
    test_broker.fake_client = raising
    async with test_broker:
        await broker.publish("first-fails-on-delete", queue="orders")  # ty: ignore[missing-argument]
        await broker.publish("second-ok", queue="orders")  # ty: ignore[missing-argument]

    assert raising.calls >= 2
    assert received == ["first-fails-on-delete", "second-ok"]


async def test_fake_broker_retry_strategy_receives_handler_exception() -> None:
    """RetryStrategyProto.get_next_attempt_delay must see the raised exception."""
    seen_exceptions: list[BaseException | None] = []

    class RecordingStrategy(RetryStrategyProto):
        def get_next_attempt_delay(
            self,
            *,
            first_attempt_at: _dt.datetime,  # noqa: ARG002
            last_attempt_at: _dt.datetime,  # noqa: ARG002
            attempts_count: int,  # noqa: ARG002
            exception: BaseException | None = None,
        ) -> float | None:
            seen_exceptions.append(exception)
            return None  # terminal

    broker = _make_broker()

    @broker.subscriber("orders", retry_strategy=RecordingStrategy())
    async def handle(body: str) -> None:
        del body
        msg = "boom-transient"
        raise RuntimeError(msg)

    test_broker = TestOutboxBroker(broker)
    async with test_broker:
        await broker.publish("payload", queue="orders")  # ty: ignore[missing-argument]

    assert len(seen_exceptions) == 1
    exc = seen_exceptions[0]
    assert isinstance(exc, RuntimeError)
    assert str(exc) == "boom-transient"


# --- Loop-mode tests (run_loops=True) -------------------------------------------------


async def test_loop_mode_failing_handler_with_retry_reschedules() -> None:
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

    test_broker = TestOutboxBroker(broker, run_loops=True)
    async with test_broker:
        p, h = encode_payload("retry-me")
        test_broker.feed("orders", p, headers=h)
        await _wait_until(lambda: len(attempts) == 3, timeout=3.0)
        await _wait_until(lambda: not test_broker.fake_client.rows, timeout=1.0)


async def test_loop_mode_max_deliveries_drops_row() -> None:
    broker = _make_broker()

    @broker.subscriber(
        "orders",
        min_fetch_interval=0.01,
        max_fetch_interval=0.05,
        max_deliveries=1,
    )
    async def handle(body: str) -> None: ...

    test_broker = TestOutboxBroker(broker, run_loops=True)
    async with test_broker:
        p, h = encode_payload("never")
        test_broker.feed("orders", p, headers=h)
        # Pre-bump deliveries_count so allow_delivery rejects on first claim — handler
        # never runs, the row is just dropped.
        test_broker.fake_client.rows[0].deliveries_count = 5
        await _wait_until(lambda: not test_broker.fake_client.rows, timeout=2.0)


async def test_loop_mode_expired_lease_is_reclaimed() -> None:
    broker = _make_broker()
    received: list[str] = []

    @broker.subscriber(
        "orders",
        min_fetch_interval=0.01,
        max_fetch_interval=0.05,
        lease_ttl_seconds=0.1,
    )
    async def handle(body: str) -> None:
        received.append(body)

    test_broker = TestOutboxBroker(broker, run_loops=True)
    async with test_broker:
        # Manually create a row with an expired lease — fetch must reclaim it.
        old = _dt.datetime.now(tz=_dt.UTC) - _dt.timedelta(seconds=10)
        test_broker.fake_client._rows.append(  # noqa: SLF001
            _FakeRow(
                id=99,
                queue="orders",
                payload=encode_payload("stuck-payload")[0],
                headers=encode_payload("stuck-payload")[1],
                acquired_at=old,
                acquired_token=uuid.uuid4(),
            ),
        )
        test_broker.fake_client._next_id = 100  # noqa: SLF001
        await _wait_until(lambda: received, timeout=3.0)


async def test_loop_mode_delays_delivery_by_next_attempt_at() -> None:
    broker = _make_broker()
    received: list[str] = []

    @broker.subscriber("orders", min_fetch_interval=0.01, max_fetch_interval=0.05)
    async def handle(body: str) -> None:
        received.append(body)

    test_broker = TestOutboxBroker(broker, run_loops=True)
    async with test_broker:
        future = _dt.datetime.now(tz=_dt.UTC) + _dt.timedelta(milliseconds=300)
        test_broker.feed("orders", b'"delayed"', next_attempt_at=future, headers={"content-type": "application/json"})
        # Before the gate opens: nothing delivered.
        await asyncio.sleep(0.1)
        assert received == []
        # After the gate opens (and at least one fetch tick): delivered.
        await _wait_until(lambda: received, timeout=2.0)
    assert received == ["delayed"]


async def test_loop_mode_fetch_loop_recovers_from_client_error() -> None:
    class FlakyFetchClient(FakeOutboxClient):
        def __init__(self) -> None:
            super().__init__()
            self._raised = False

        async def fetch(self, conn, queues, *, limit, lease_ttl_seconds):
            if not self._raised:
                self._raised = True
                msg = "transient db error"
                raise RuntimeError(msg)
            return await super().fetch(conn, queues, limit=limit, lease_ttl_seconds=lease_ttl_seconds)

    broker = _make_broker()
    received: list[str] = []

    @broker.subscriber("orders", min_fetch_interval=0.01, max_fetch_interval=0.05)
    async def handle(body: str) -> None:
        received.append(body)

    test_broker = TestOutboxBroker(broker, run_loops=True)
    test_broker.fake_client = FlakyFetchClient()
    async with test_broker:
        p, h = encode_payload("after-error")
        test_broker.feed("orders", p, headers=h)
        await _wait_until(lambda: received, timeout=5.0)


async def test_loop_mode_fetch_loop_backs_off_when_inflight_full() -> None:
    broker = _make_broker()
    handler_started = asyncio.Event()
    release_handler = asyncio.Event()
    received: list[str] = []

    @broker.subscriber(
        "orders",
        max_workers=1,
        fetch_batch_size=1,
        min_fetch_interval=0.01,
        max_fetch_interval=0.05,
    )
    async def handle(body: str) -> None:
        handler_started.set()
        await release_handler.wait()
        received.append(body)

    test_broker = TestOutboxBroker(broker, run_loops=True)
    async with test_broker:
        p, h = encode_payload("first")
        test_broker.feed("orders", p, headers=h)
        await asyncio.wait_for(handler_started.wait(), timeout=2.0)
        # Second feed while worker is busy: fetch loop sees inflight queue full,
        # takes the short-sleep branch.
        p2, h2 = encode_payload("second")
        test_broker.feed("orders", p2, headers=h2)
        await asyncio.sleep(0.1)
        release_handler.set()
        await _wait_until(lambda: len(received) == 2, timeout=5.0)


async def test_loop_mode_flush_with_no_lease_token_is_noop() -> None:
    """Fetch strips the lease token → _flush_terminal early-returns. Loop-only path."""

    class TokenStrippingClient(FakeOutboxClient):
        async def fetch(self, conn, queues, *, limit, lease_ttl_seconds):
            rows = await super().fetch(conn, queues, limit=limit, lease_ttl_seconds=lease_ttl_seconds)
            for row in rows:
                row.acquired_token = None
            return rows

    broker = _make_broker()
    received: list[str] = []

    @broker.subscriber("orders", min_fetch_interval=0.01, max_fetch_interval=0.05)
    async def handle(body: str) -> None:
        received.append(body)

    test_broker = TestOutboxBroker(broker, run_loops=True)
    test_broker.fake_client = TokenStrippingClient()
    async with test_broker:
        p, h = encode_payload("no-token")
        test_broker.feed("orders", p, headers=h)
        await _wait_until(lambda: received, timeout=5.0)


async def test_loop_mode_flush_retry_with_no_lease_token_is_noop() -> None:
    class TokenStrippingClient(FakeOutboxClient):
        async def fetch(self, conn, queues, *, limit, lease_ttl_seconds):
            rows = await super().fetch(conn, queues, limit=limit, lease_ttl_seconds=lease_ttl_seconds)
            for row in rows:
                row.acquired_token = None
            return rows

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
        msg = "fail"
        raise RuntimeError(msg)

    test_broker = TestOutboxBroker(broker, run_loops=True)
    test_broker.fake_client = TokenStrippingClient()
    async with test_broker:
        p, h = encode_payload("retry-no-token")
        test_broker.feed("orders", p, headers=h)
        await _wait_until(lambda: attempts, timeout=5.0)


async def test_loop_mode_retry_strategy_can_branch_on_exception_type() -> None:
    """Subclass pattern from retry.py docstring: retry transient, terminate on permanent."""
    attempts: list[str] = []

    class TransientOnlyStrategy(RetryStrategyProto):
        def get_next_attempt_delay(
            self,
            *,
            first_attempt_at: _dt.datetime,  # noqa: ARG002
            last_attempt_at: _dt.datetime,  # noqa: ARG002
            attempts_count: int,  # noqa: ARG002
            exception: BaseException | None = None,
        ) -> float | None:
            if isinstance(exception, ValueError):
                return None  # permanent → terminal
            return 0.05  # transient → retry

    broker = _make_broker()

    @broker.subscriber(
        "orders",
        min_fetch_interval=0.01,
        max_fetch_interval=0.05,
        retry_strategy=TransientOnlyStrategy(),
    )
    async def handle(body: str) -> None:
        attempts.append(body)
        if len(attempts) == 1:
            msg = "transient"
            raise RuntimeError(msg)
        msg = "permanent"
        raise ValueError(msg)

    test_broker = TestOutboxBroker(broker, run_loops=True)
    async with test_broker:
        p, h = encode_payload("body")
        test_broker.feed("orders", p, headers=h)
        await _wait_until(lambda: not test_broker.fake_client.rows, timeout=5.0)

    assert len(attempts) == 2


async def test_subscriber_with_no_handler_skips_loop_setup() -> None:
    """Calling subscriber.start() with no handler attached early-returns; no loops spawn."""
    from faststream_outbox.subscriber.factory import create_subscriber  # noqa: PLC0415

    metadata = MetaData()
    t = make_outbox_table(metadata)
    broker = OutboxBroker(outbox_table=t)
    sub = create_subscriber(
        queues=["empty-queue"],
        max_workers=1,
        retry_strategy=None,
        fetch_batch_size=1,
        min_fetch_interval=1.0,
        max_fetch_interval=10.0,
        lease_ttl_seconds=60.0,
        max_deliveries=None,
        config=broker.config.broker_config,  # type: ignore[arg-type]
    )
    broker._subscribers.add(sub)  # noqa: SLF001  # ty: ignore[unresolved-attribute]
    async with TestOutboxBroker(broker, run_loops=True):
        # Inside the test broker the logger is wired; call start() directly so the
        # ``if not self.calls: return`` branch fires (no add_call() was performed).
        await sub.start()
        assert sub.tasks == [] or all(t.done() for t in sub.tasks)


async def test_sync_publish_skips_callless_subscriber() -> None:
    """``_find_subscriber_for_queue`` must skip subscribers that have no registered call."""
    from faststream_outbox.subscriber.factory import create_subscriber  # noqa: PLC0415

    metadata = MetaData()
    t = make_outbox_table(metadata)
    broker = OutboxBroker(outbox_table=t)
    # Add a call-less subscriber for "orphans" — sync-dispatch's lookup must skip it
    # so the row stays in the fake client instead of crashing on a missing handler.
    sub = create_subscriber(
        queues=["orphans"],
        max_workers=1,
        retry_strategy=None,
        fetch_batch_size=1,
        min_fetch_interval=1.0,
        max_fetch_interval=10.0,
        lease_ttl_seconds=60.0,
        max_deliveries=None,
        config=broker.config.broker_config,  # type: ignore[arg-type]
    )
    broker._subscribers.add(sub)  # noqa: SLF001  # ty: ignore[unresolved-attribute]

    test_broker = TestOutboxBroker(broker)
    async with test_broker:
        await broker.publish("ignored", queue="orphans")  # ty: ignore[missing-argument]

    assert len(test_broker.fake_client.rows) == 1


# --- TestOutboxBroker plumbing --------------------------------------------------------


async def test_fake_connect_is_noop() -> None:
    broker = _make_broker()
    test_broker = TestOutboxBroker(broker)
    await test_broker._fake_connect(broker)  # noqa: SLF001


async def test_test_broker_feed_forwards_timer_id() -> None:
    broker = _make_broker()
    test_broker = TestOutboxBroker(broker)
    async with test_broker:
        first = test_broker.feed("orders", b"x", timer_id="email-1")
        second = test_broker.feed("orders", b"y", timer_id="email-1")
    assert first is not None
    assert second is None
    assert len(test_broker.fake_client.rows) == 1
    assert test_broker.fake_client.rows[0].timer_id == "email-1"


# --- FakeOutboxClient unit tests ------------------------------------------------------


async def test_fake_client_feed_timer_id_dedup() -> None:
    fake = FakeOutboxClient()
    first = fake.feed(queue="q", payload=b"x", timer_id="email-1")
    second = fake.feed(queue="q", payload=b"y", timer_id="email-1")
    assert first is not None
    assert second is None
    assert len(fake.rows) == 1
    assert fake.rows[0].payload == b"x"


async def test_fake_client_feed_timer_id_different_queues_allowed() -> None:
    fake = FakeOutboxClient()
    a = fake.feed(queue="q1", payload=b"x", timer_id="email-1")
    b = fake.feed(queue="q2", payload=b"y", timer_id="email-1")
    assert a is not None
    assert b is not None
    assert len(fake.rows) == 2


async def test_fake_client_future_next_attempt_is_invisible_to_fetch() -> None:
    fake = FakeOutboxClient()
    future = _dt.datetime.now(tz=_dt.UTC) + _dt.timedelta(minutes=5)
    fake.feed(queue="q", payload=b"x", next_attempt_at=future)
    rows = await fake.fetch(None, ["q"], limit=10, lease_ttl_seconds=60.0)
    assert rows == []


async def test_fake_client_cancel_timer_removes_unleased_row() -> None:
    fake = FakeOutboxClient()
    fake.feed(queue="q", payload=b"x", timer_id="email-1")
    assert await fake.cancel_timer(queue="q", timer_id="email-1") is True
    assert fake.rows == []


async def test_fake_client_cancel_timer_unknown_returns_false() -> None:
    fake = FakeOutboxClient()
    assert await fake.cancel_timer(queue="q", timer_id="never-existed") is False


async def test_fake_client_cancel_timer_skips_leased_row() -> None:
    fake = FakeOutboxClient()
    fake.feed(queue="q", payload=b"x", timer_id="email-1")
    fake.rows[0].acquired_token = uuid.uuid4()
    fake.rows[0].acquired_at = _dt.datetime.now(tz=_dt.UTC)
    assert await fake.cancel_timer(queue="q", timer_id="email-1") is False
    assert len(fake.rows) == 1


# --- AckPolicy plumbing (A) ----------------------------------------------------------


async def test_fake_broker_reject_on_error_deletes_row_on_first_failure() -> None:
    """REJECT_ON_ERROR ignores retry strategies and deletes on the first handler error."""
    broker = _make_broker()
    attempts: list[str] = []

    with _warnings.catch_warnings():
        # REJECT_ON_ERROR + the default exponential retry triggers the misconfig warning;
        # this test asserts only the runtime semantics. Coverage for the warning itself
        # lives in tests/test_unit.py::test_subscriber_warns_on_reject_with_retry_strategy.
        _warnings.simplefilter("ignore", UserWarning)

        @broker.subscriber("orders", ack_policy=AckPolicy.REJECT_ON_ERROR)
        async def handle(body: str) -> None:
            attempts.append(body)
            msg = "boom"
            raise RuntimeError(msg)

    test_broker = TestOutboxBroker(broker)
    async with test_broker:
        await broker.publish("x", queue="orders")  # ty: ignore[missing-argument]

    # Handler ran once, then row was deleted — no retry despite default ExponentialRetry.
    assert attempts == ["x"]
    assert test_broker.fake_client.rows == []


async def test_fake_broker_nack_on_error_default_keeps_row_for_retry() -> None:
    """Explicit NACK_ON_ERROR matches the default behavior — row kept for retry."""
    broker = _make_broker()

    @broker.subscriber("orders", ack_policy=AckPolicy.NACK_ON_ERROR)
    async def handle(body: str) -> None:
        del body
        msg = "boom"
        raise RuntimeError(msg)

    test_broker = TestOutboxBroker(broker)
    async with test_broker:
        await broker.publish("x", queue="orders")  # ty: ignore[missing-argument]

    # Row rescheduled, not deleted — same as the no-ack_policy default.
    assert len(test_broker.fake_client.rows) == 1
    row = test_broker.fake_client.rows[0]
    assert row.attempts_count == 1
    assert row.next_attempt_at > _dt.datetime.now(tz=_dt.UTC)


# --- Publisher tests --------------------------------------------------------------------


async def test_publisher_publish_inserts_row_via_fake_producer() -> None:
    """``publisher.publish`` routes through the fake producer → fake client store."""
    broker = _make_broker()
    received: list[dict] = []

    @broker.subscriber("orders")
    async def handle(body: dict) -> None:
        received.append(body)

    pub = broker.publisher("orders")
    test_broker = TestOutboxBroker(broker)
    async with test_broker:
        result = await pub.publish({"id": 1}, session=_fake_session())
    assert isinstance(result, int)
    assert received == [{"id": 1}]


async def test_publisher_static_headers_merge_with_per_call() -> None:
    """Per-call headers override the publisher's static headers (inspect fake row directly)."""
    broker = _make_broker()
    # No subscriber so the row stays in the fake store for inspection — sync
    # dispatch on an ack'ed delivery would delete it before we can read headers.
    pub = broker.publisher("orders", headers={"source": "default", "trace": "abc"})
    test_broker = TestOutboxBroker(broker)
    async with test_broker:
        await pub.publish(b"x", session=_fake_session(), headers={"source": "override"})

    assert len(test_broker.fake_client.rows) == 1
    row_headers = test_broker.fake_client.rows[0].headers
    assert row_headers is not None
    assert row_headers["source"] == "override"  # per-call wins
    assert row_headers["trace"] == "abc"  # static still present


async def test_publisher_with_timer_id_dedups() -> None:
    """Second publish with the same timer_id is a no-op (returns None)."""
    broker = _make_broker()
    # No subscriber — sync dispatch would delete the first row before the second
    # publish runs, masking the dedup behavior. Without a handler the row stays
    # in the fake store and the second publish's ``(queue, timer_id)`` check fires.
    pub = broker.publisher("orders")
    test_broker = TestOutboxBroker(broker)
    async with test_broker:
        first = await pub.publish(b"hello", session=_fake_session(), timer_id="dedup-key")
        second = await pub.publish(b"hello", session=_fake_session(), timer_id="dedup-key")

    assert first is not None
    assert second is None  # dedup'd
    assert len(test_broker.fake_client.rows) == 1


async def test_publisher_with_activate_in_records_next_attempt() -> None:
    """Future-dated rows are stored with next_attempt_at; sync mode dispatches immediately."""
    broker = _make_broker()

    # No subscriber for "backlog" — the row stays in the fake store so we can
    # inspect next_attempt_at without sync dispatch deleting it.
    pub = broker.publisher("backlog")
    test_broker = TestOutboxBroker(broker)
    before = _dt.datetime.now(tz=_dt.UTC)
    async with test_broker:
        await pub.publish(b"x", session=_fake_session(), activate_in=_dt.timedelta(seconds=30))
        assert len(test_broker.fake_client.rows) == 1
        assert test_broker.fake_client.rows[0].next_attempt_at > before


async def test_publisher_decorator_chain_rejected_at_setup() -> None:
    """Stacking ``@publisher @broker.subscriber`` is rejected at decoration time."""
    broker = _make_broker()
    pub = broker.publisher("relay")

    async def handler(body: dict) -> None: ...

    with pytest.raises(NotImplementedError, match="cannot decorate"):
        pub(handler)


async def test_publisher_publish_batch_via_broker_still_works() -> None:
    """Sanity: the publisher's existence doesn't break broker.publish_batch."""
    broker = _make_broker()
    received: list[str] = []

    @broker.subscriber("orders")
    async def handle(body: str) -> None:
        received.append(body)

    broker.publisher("orders")  # registers AsyncAPI spec but isn't called

    test_broker = TestOutboxBroker(broker)
    async with test_broker:
        await broker.publish_batch("a", "b", queue="orders")  # ty: ignore[missing-argument]

    assert received == ["a", "b"]


async def test_subscriber_config_ack_policy_returns_explicit_value() -> None:
    """The non-EMPTY branch of OutboxSubscriberConfig.ack_policy is now reachable."""
    cfg = OutboxSubscriberConfig(
        _outer_config=OutboxBrokerConfig(),
        _ack_policy=AckPolicy.REJECT_ON_ERROR,
        queues=["q"],
        max_workers=1,
        retry_strategy=None,
        fetch_batch_size=10,
        min_fetch_interval=1.0,
        max_fetch_interval=10.0,
        lease_ttl_seconds=60.0,
        max_deliveries=None,
    )
    assert cfg.ack_policy is AckPolicy.REJECT_ON_ERROR
