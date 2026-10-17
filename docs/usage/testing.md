# Testing

`faststream-outbox` ships `TestOutboxBroker`, a test context manager that
swaps the SQLAlchemy-backed client for an in-memory `FakeOutboxClient` so
unit tests don't need Postgres. Only the storage is fake: publishing runs
the broker's real producer and publish middlewares, as in production.

By default it dispatches handlers synchronously inside `publish`, matching
`TestKafkaBroker` / `TestRabbitBroker`, so tests need no `_wait_until` or
`sleep`.

## Basic test

The examples below assume `asyncio_mode = "auto"` (see
[pytest-asyncio configuration](#pytest-asyncio-configuration) at the
bottom), so async test functions need no `@pytest.mark.asyncio` marker.

```python
from faststream_outbox import OutboxBroker, make_outbox_table
from faststream_outbox.testing import TestOutboxBroker, fake_session
from sqlalchemy import MetaData


async def test_handler() -> None:
    metadata = MetaData()
    outbox_table = make_outbox_table(metadata, table_name="outbox")
    broker = OutboxBroker(None, outbox_table=outbox_table)  # engine not needed
    received: list[int] = []

    @broker.subscriber("orders")
    async def handle(order_id: int) -> None:
        received.append(order_id)

    async with TestOutboxBroker(broker):
        await broker.publish(1, queue="orders", session=fake_session())
        # Handler has already run.

    assert received == [1]
```

Every publish needs a `session` in tests, as in production.
`fake_session()` returns a mock `AsyncSession` that passes the session check
and reports an open transaction, so an `OutboxResponse` accepts it too. Its
methods do nothing: the fake store keeps the row whether you commit or roll
back.

The fake client keeps an in-memory list of rows you can inspect via
`fake_client.rows`. `fake_client` is an attribute of the
`TestOutboxBroker` harness, not the broker, so bind the harness to a name:

```python
tb = TestOutboxBroker(broker)
async with tb:
    # "orders" has a subscriber, so in sync mode the row is dispatched and
    # acked (deleted) before publish returns. Publish to a queue with no
    # subscriber to inspect a row that survives.
    await broker.publish(1, queue="audit", session=fake_session())

assert len(tb.fake_client.rows) == 1
```

An unconsumed queue is used deliberately: a row published to a queue that
*has* a subscriber is dispatched and deleted on ack within `publish`, so
`fake_client.rows` would be empty (`[]`) for it, not length 1.

## Testing publishers

```python
async def test_publisher() -> None:
    metadata = MetaData()
    outbox_table = make_outbox_table(metadata, table_name="outbox")
    broker = OutboxBroker(None, outbox_table=outbox_table)
    received: list[dict] = []

    @broker.subscriber("orders")
    async def handle(body: dict) -> None:
        received.append(body)

    pub = broker.publisher("orders")

    async with TestOutboxBroker(broker):
        await pub.publish({"order_id": 1}, session=fake_session())

    assert received == [{"order_id": 1}]
```

`broker.publisher("q").publish(...)` and `broker.publish(queue="q", ...)`
go through the same producer and land rows in the same fake store.

## Loop-driven mode

For tests that exercise real polling semantics (retry rescheduling, lease
expiry / reclaim, fetch-loop error recovery, or honoring `activate_in`
delays), opt in with `run_loops=True`:

```python
import asyncio
import json

# `broker` is built exactly as in the Basic test above:
#   metadata = MetaData()
#   outbox_table = make_outbox_table(metadata, table_name="outbox")
#   broker = OutboxBroker(None, outbox_table=outbox_table)
received: list[dict] = []


@broker.subscriber("orders")
async def handle(body: dict) -> None:
    received.append(body)


tb = TestOutboxBroker(broker, run_loops=True)
async with tb:
    tb.feed("orders", json.dumps({"order_id": 1}).encode())
    # the real fetch + worker loops pick the row up asynchronously
    async with asyncio.timeout(1.0):  # fail fast instead of hanging forever
        while not received:
            await asyncio.sleep(0.01)

assert received == [{"order_id": 1}]
```

`feed(queue, payload, *, headers=None, next_attempt_at=None, timer_id=None)`
inserts a row straight into the in-memory store and (in loop mode) wakes the
fetch loop like a production NOTIFY would. In loop mode, the real
fetch and worker loops run against the fake client. Subscribers without
registered handlers are not started, matching production.

## Notes

- `activate_in` / `activate_at` are ignored in sync mode. Timers fire
  immediately. The intended firing time is preserved on the harness's
  `fake_client.rows[i].next_attempt_at` for assertions. Use
  `run_loops=True` if you need scheduled delivery to actually wait.
- `cancel_timer` and `fetch_unprocessed` run against the fake client
  (the test broker swaps its client in, so `broker.<method>` reaches it).
  The `session` argument is still required but is ignored in tests.
- Publishing runs the production producer, so encoding, the `published`
  metric and publish middlewares (Prometheus, OpenTelemetry or your own
  `publish_scope`) behave as in production.
- The fake store ignores transactions. A row published in a transaction
  that rolls back is still stored and delivered. Test rollback behaviour
  against a real database.
- `lease_ttl_seconds` and re-delivery are not simulated in sync mode.
  Handlers that exceed the configured TTL in production may be re-delivered
  to another worker, but tests will only invoke the handler once.
  Idempotency must be verified separately. Use `run_loops=True` for tests
  that need to observe lease-expiry behavior.
- `broker.validate_schema()` raises `NotImplementedError` under
  `TestOutboxBroker`: there is no real DB to validate against, and a
  silent pass would let users ship broken schemas while their tests stay
  green. Tests that need real schema validation must call
  `validate_schema()` on an `OutboxBroker(real_engine, outbox_table=...)`
  outside the test broker, against the same DSN the migrations ran against.

## Limitations of the fake broker

`TestOutboxBroker` does not install FastStream's publisher spies (the
fake subscribers that `TestKafkaBroker` and friends attach to each
publisher). Those spies mock the registered handler to forward
`publisher.publish()` calls, which would conflict with the outbox's real
dispatch path: the test broker already lands rows in the fake client
*and* drives the real handler. Publisher-mock assertions such as
`publisher.mock.assert_called_once_with(...)` are therefore not available;
assert on handler side effects or `fake_client.rows` instead.

## pytest-asyncio configuration

Add to `pyproject.toml`:

```toml
[tool.pytest.ini_options]
asyncio_mode = "auto"
asyncio_default_fixture_loop_scope = "function"
```
