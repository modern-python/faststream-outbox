import asyncio
import datetime as _dt
from collections.abc import Callable

import pytest
from sqlalchemy import MetaData

from faststream_outbox import (
    ConstantRetry,
    OutboxBroker,
    OutboxRouter,
    RetryStrategyProto,
    TestOutboxBroker,
    make_outbox_table,
)
from faststream_outbox.envelope import _encode_payload as encode_payload


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
    msg = "timed out waiting for predicate"  # pragma: no cover
    raise AssertionError(msg)  # pragma: no cover


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

    @broker.subscriber("orders", min_fetch_interval=0.01, max_fetch_interval=0.05)
    async def handle(body: str) -> None: ...

    test_broker = TestOutboxBroker(broker)
    async with test_broker:
        p, h = encode_payload("not-mine")
        test_broker.feed("other-queue", p, headers=h)
        await asyncio.sleep(0.2)

    # Row stays in fake client because no subscriber matches that queue
    assert len(test_broker.fake_client.rows) == 1
    assert test_broker.fake_client.rows[0].queue == "other-queue"


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

    @broker.subscriber(
        "orders",
        min_fetch_interval=0.01,
        max_fetch_interval=0.05,
        max_deliveries=1,
    )
    async def handle(body: str) -> None: ...

    test_broker = TestOutboxBroker(broker)
    async with test_broker:
        p, h = encode_payload("never")
        test_broker.feed("orders", p, headers=h)
        # Pre-bump deliveries_count so allow_delivery rejects on first claim — handler
        # never runs, the row is just dropped.
        test_broker.fake_client.rows[0].deliveries_count = 5
        await _wait_until(lambda: not test_broker.fake_client.rows, timeout=2.0)


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


async def test_fake_broker_request_raises() -> None:
    broker = _make_broker()
    test_broker = TestOutboxBroker(broker)
    async with test_broker:
        with pytest.raises(NotImplementedError):
            await broker.request(b"x")


async def test_fake_broker_publish_rejects_non_async_session() -> None:
    broker = _make_broker()
    test_broker = TestOutboxBroker(broker)
    async with test_broker:
        with pytest.raises(TypeError, match="AsyncSession"):
            await broker.publish(b"x", queue="orders", session=object())  # ty: ignore[invalid-argument-type]


# --- subscriber error paths via subclassed FakeOutboxClient ---


async def test_fetch_loop_recovers_from_client_error() -> None:
    from faststream_outbox.testing import FakeOutboxClient  # noqa: PLC0415

    class FlakyFetchClient(FakeOutboxClient):
        def __init__(self) -> None:
            super().__init__()
            self._raised = False

        async def fetch(self, queues, *, limit):
            if not self._raised:
                self._raised = True
                msg = "transient db error"
                raise RuntimeError(msg)
            return await super().fetch(queues, limit=limit)

    broker = _make_broker()
    received: list[str] = []

    @broker.subscriber("orders", min_fetch_interval=0.01, max_fetch_interval=0.05)
    async def handle(body: str) -> None:
        received.append(body)

    test_broker = TestOutboxBroker(broker)
    test_broker.fake_client = FlakyFetchClient()
    async with test_broker:
        p, h = encode_payload("after-error")
        test_broker.feed("orders", p, headers=h)
        await _wait_until(lambda: received, timeout=5.0)


async def test_fetch_loop_backs_off_when_inflight_full() -> None:
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

    test_broker = TestOutboxBroker(broker)
    async with test_broker:
        p, h = encode_payload("first")
        test_broker.feed("orders", p, headers=h)
        await asyncio.wait_for(handler_started.wait(), timeout=2.0)
        # Now feed a second row while the worker is busy. The fetch loop must
        # see inflight queue full (free <= 0) and take the short-sleep branch.
        p2, h2 = encode_payload("second")
        test_broker.feed("orders", p2, headers=h2)
        await asyncio.sleep(0.1)  # let the fetch loop spin against a full queue
        release_handler.set()
        await _wait_until(lambda: len(received) == 2, timeout=5.0)


async def test_release_stuck_loop_recovers_from_client_error() -> None:
    from faststream_outbox.testing import FakeOutboxClient  # noqa: PLC0415

    class FlakyReleaseStuckClient(FakeOutboxClient):
        def __init__(self) -> None:
            super().__init__()
            self.release_calls = 0

        async def release_stuck(self, *, timeout_seconds):
            self.release_calls += 1
            if self.release_calls == 1:
                msg = "transient"
                raise RuntimeError(msg)
            return await super().release_stuck(timeout_seconds=timeout_seconds)

    broker = _make_broker()

    @broker.subscriber(
        "orders",
        min_fetch_interval=0.01,
        max_fetch_interval=0.05,
        release_stuck_interval=0.05,
        release_stuck_timeout=0.1,
    )
    async def handle(body: str) -> None: ...

    flaky = FlakyReleaseStuckClient()
    test_broker = TestOutboxBroker(broker)
    test_broker.fake_client = flaky
    async with test_broker:
        await _wait_until(lambda: flaky.release_calls >= 2, timeout=5.0)


async def test_release_stuck_loop_logs_when_rows_released() -> None:
    import uuid as _uuid  # noqa: PLC0415

    from faststream_outbox.schema import OutboxState  # noqa: PLC0415
    from faststream_outbox.testing import _FakeRow  # noqa: PLC0415

    broker = _make_broker()
    received: list[str] = []

    @broker.subscriber(
        "orders",
        min_fetch_interval=0.01,
        max_fetch_interval=0.05,
        release_stuck_interval=0.05,
        release_stuck_timeout=0.1,
    )
    async def handle(body: str) -> None:
        received.append(body)

    test_broker = TestOutboxBroker(broker)
    async with test_broker:
        old = _dt.datetime.now(tz=_dt.UTC) - _dt.timedelta(seconds=10)
        p, h = encode_payload("stuck")
        test_broker.fake_client._rows.append(  # noqa: SLF001
            _FakeRow(
                id=99,
                queue="orders",
                payload=p,
                headers=h,
                state=OutboxState.PROCESSING.value,
                acquired_at=old,
                acquired_token=_uuid.uuid4(),
            )
        )
        test_broker.fake_client._next_id = 100  # noqa: SLF001
        await _wait_until(lambda: received, timeout=5.0)


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
        release_stuck_timeout=300.0,
        release_stuck_interval=150.0,
        max_deliveries=None,
        config=broker.config.broker_config,  # type: ignore[arg-type]
    )
    broker._subscribers.add(sub)  # noqa: SLF001  # ty: ignore[unresolved-attribute]
    async with TestOutboxBroker(broker):
        # Inside the test broker the logger is wired; call start() directly so the
        # ``if not self.calls: return`` branch fires (no add_call() was performed).
        await sub.start()
        # No tasks added because the early-return short-circuits before add_task calls.
        assert sub.tasks == [] or all(t.done() for t in sub.tasks)


async def test_flush_terminal_when_lease_lost_logs_and_skips() -> None:
    """Worker tries to DELETE but acquired_token no longer matches → log and skip."""
    from faststream_outbox.testing import FakeOutboxClient  # noqa: PLC0415

    class LeaseLostClient(FakeOutboxClient):
        async def delete_with_lease(self, message_id, acquired_token):  # noqa: ARG002
            return False  # always pretend the lease is gone

    broker = _make_broker()
    received: list[str] = []

    @broker.subscriber("orders", min_fetch_interval=0.01, max_fetch_interval=0.05)
    async def handle(body: str) -> None:
        received.append(body)

    test_broker = TestOutboxBroker(broker)
    test_broker.fake_client = LeaseLostClient()
    async with test_broker:
        p, h = encode_payload("lease-lost")
        test_broker.feed("orders", p, headers=h)
        await _wait_until(lambda: received, timeout=5.0)


async def test_flush_retry_when_lease_lost_logs_and_skips() -> None:
    """Handler nacks; mark_pending_with_lease returns False → log and skip."""
    from faststream_outbox.testing import FakeOutboxClient  # noqa: PLC0415

    class LeaseLostRetryClient(FakeOutboxClient):
        async def mark_pending_with_lease(self, *args, **kwargs):  # noqa: ARG002
            return False

    broker = _make_broker()
    attempts: list[str] = []

    @broker.subscriber(
        "orders",
        min_fetch_interval=0.01,
        max_fetch_interval=0.05,
        retry_strategy=ConstantRetry(delay_seconds=0.05, max_attempts=10),
    )
    async def handle(body: str) -> None:
        attempts.append(body)
        msg = "always fails"
        raise RuntimeError(msg)

    test_broker = TestOutboxBroker(broker)
    test_broker.fake_client = LeaseLostRetryClient()
    async with test_broker:
        p, h = encode_payload("never-cleared")
        test_broker.feed("orders", p, headers=h)
        await _wait_until(lambda: attempts, timeout=5.0)


async def test_worker_outer_except_catches_post_consume_failure() -> None:
    """If the post-consume terminal write raises, the outer worker except logs and the loop survives."""
    from faststream_outbox.testing import FakeOutboxClient  # noqa: PLC0415

    class RaisingDeleteClient(FakeOutboxClient):
        def __init__(self) -> None:
            super().__init__()
            self.calls = 0

        async def delete_with_lease(self, message_id, acquired_token):
            self.calls += 1
            if self.calls == 1:
                msg = "delete blew up"
                raise RuntimeError(msg)
            return await super().delete_with_lease(message_id, acquired_token)

    broker = _make_broker()
    received: list[str] = []

    @broker.subscriber("orders", min_fetch_interval=0.01, max_fetch_interval=0.05)
    async def handle(body: str) -> None:
        received.append(body)

    test_broker = TestOutboxBroker(broker)
    raising = RaisingDeleteClient()
    test_broker.fake_client = raising
    async with test_broker:
        p1, h1 = encode_payload("first-fails-on-delete")
        test_broker.feed("orders", p1, headers=h1)
        # First delete raises → outer except catches; subscriber loop survives.
        # Second message proves the loop didn't die.
        await _wait_until(lambda: raising.calls >= 1, timeout=2.0)
        p2, h2 = encode_payload("second-ok")
        test_broker.feed("orders", p2, headers=h2)
        await _wait_until(lambda: len(received) == 2, timeout=5.0)


async def test_flush_with_no_lease_token_is_noop() -> None:
    """If acquired_token is somehow None (defensive), _flush_terminal early-returns."""
    from faststream_outbox.testing import FakeOutboxClient  # noqa: PLC0415

    class TokenStrippingClient(FakeOutboxClient):
        async def fetch(self, queues, *, limit):
            rows = await super().fetch(queues, limit=limit)
            for row in rows:
                row.acquired_token = None  # strip the lease
            return rows

    broker = _make_broker()
    received: list[str] = []

    @broker.subscriber("orders", min_fetch_interval=0.01, max_fetch_interval=0.05)
    async def handle(body: str) -> None:
        received.append(body)

    test_broker = TestOutboxBroker(broker)
    test_broker.fake_client = TokenStrippingClient()
    async with test_broker:
        p, h = encode_payload("no-token")
        test_broker.feed("orders", p, headers=h)
        await _wait_until(lambda: received, timeout=5.0)


async def test_flush_retry_with_no_lease_token_is_noop() -> None:
    """If acquired_token is None and the handler nacks, _flush_retry early-returns."""
    from faststream_outbox.testing import FakeOutboxClient  # noqa: PLC0415

    class TokenStrippingClient(FakeOutboxClient):
        async def fetch(self, queues, *, limit):
            rows = await super().fetch(queues, limit=limit)
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

    test_broker = TestOutboxBroker(broker)
    test_broker.fake_client = TokenStrippingClient()
    async with test_broker:
        p, h = encode_payload("retry-no-token")
        test_broker.feed("orders", p, headers=h)
        await _wait_until(lambda: attempts, timeout=5.0)


async def test_fake_connect_is_noop() -> None:
    broker = _make_broker()
    test_broker = TestOutboxBroker(broker)
    # Direct call exercises L226 even though it's also called during __aenter__.
    await test_broker._fake_connect(broker)  # noqa: SLF001


async def test_retry_strategy_receives_handler_exception() -> None:
    """RetryStrategyProto.get_next_attempt_delay must see the raised exception, not None."""
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
            return None  # terminal so the test wraps up promptly

    broker = _make_broker()

    @broker.subscriber(
        "orders",
        min_fetch_interval=0.01,
        max_fetch_interval=0.05,
        retry_strategy=RecordingStrategy(),
    )
    async def handle(body: str) -> None:
        del body
        msg = "boom-transient"
        raise RuntimeError(msg)

    test_broker = TestOutboxBroker(broker)
    async with test_broker:
        p, h = encode_payload("payload")
        test_broker.feed("orders", p, headers=h)
        await _wait_until(lambda: seen_exceptions, timeout=3.0)

    assert len(seen_exceptions) >= 1
    exc = seen_exceptions[0]
    assert isinstance(exc, RuntimeError)
    assert str(exc) == "boom-transient"


async def test_retry_strategy_can_branch_on_exception_type() -> None:
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
        # First call: transient (gets retried via the strategy's retry branch).
        # Second call: permanent (terminates via the strategy's None branch).
        if len(attempts) == 1:
            msg = "transient"
            raise RuntimeError(msg)
        msg = "permanent"
        raise ValueError(msg)

    test_broker = TestOutboxBroker(broker)
    async with test_broker:
        p, h = encode_payload("body")
        test_broker.feed("orders", p, headers=h)
        await _wait_until(lambda: not test_broker.fake_client.rows, timeout=5.0)

    assert len(attempts) == 2  # transient retried once, then permanent terminated


async def test_router_subscriber_receives_plain_queue_publish() -> None:
    """A subscriber registered via OutboxRouter must receive rows whose queue matches literally."""
    received: list[str] = []

    router = OutboxRouter()

    @router.subscriber("orders", min_fetch_interval=0.01, max_fetch_interval=0.05)
    async def handle(body: str) -> None:
        received.append(body)

    broker = _make_broker()
    broker.include_router(router)

    test_broker = TestOutboxBroker(broker)
    async with test_broker:
        p, h = encode_payload("via-router")
        test_broker.feed("orders", p, headers=h)
        await _wait_until(lambda: received, timeout=3.0)

    assert received == ["via-router"]
