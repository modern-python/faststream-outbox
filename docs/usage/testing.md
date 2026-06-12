# Testing

`faststream-outbox` ships `TestOutboxBroker` — a test context manager that
swaps the SQLAlchemy-backed client for an in-memory `FakeOutboxClient` so
unit tests don't need Postgres.

By default it dispatches handlers **synchronously inside `publish`** —
matching `TestKafkaBroker` / `TestRabbitBroker`. No `_wait_until`, no
`sleep`.

## Basic test

```python
import pytest
from faststream_outbox import OutboxBroker, make_outbox_table
from faststream_outbox.testing import TestOutboxBroker
from sqlalchemy import MetaData


@pytest.mark.asyncio
async def test_handler() -> None:
    metadata = MetaData()
    outbox_table = make_outbox_table(metadata, table_name="outbox")
    broker = OutboxBroker(None, outbox_table=outbox_table)  # engine not needed
    received: list[int] = []

    @broker.subscriber("orders")
    async def handle(order_id: int) -> None:
        received.append(order_id)

    async with TestOutboxBroker(broker):
        await broker.publish(1, queue="orders")
        # Handler has already run.

    assert received == [1]
```

In sync mode, `session=` is optional — the test broker patches
`broker.publish` to ignore it. The fake client keeps an in-memory list of
rows you can inspect via `fake_client.rows` — but `fake_client` is an
attribute of the `TestOutboxBroker` harness, not the broker, so bind the
harness to a name:

```python
tb = TestOutboxBroker(broker)
async with tb:
    await broker.publish(1, queue="orders")

assert len(tb.fake_client.rows) == 1
```

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
        await pub.publish({"order_id": 1})

    assert received == [{"order_id": 1}]
```

`broker.publisher("q").publish(...)` works identically to
`broker.publish(queue="q", ...)` — the test broker swaps the producer slot
for a `FakeOutboxProducer` that lands rows in the same fake store via the
FastStream `_basic_publish` flow.

## Loop-driven mode

For tests that exercise real polling semantics — retry rescheduling, lease
expiry / reclaim, `_fetch_loop` error recovery, or honoring `activate_in`
delays — opt in with `run_loops=True`:

```python
async with TestOutboxBroker(broker, run_loops=True):
    ...  # use feed() / poll until handler observes the row
```

In loop mode, the real `_fetch_loop` / `_worker_loop` run against the fake
client. Subscribers without registered handlers are skipped in
`_fake_start` (mirrors `OutboxSubscriber.start`'s `if not self.calls:
return`).

## Notes

- **`activate_in` / `activate_at` are ignored in sync mode.** Timers fire
  immediately. The intended firing time is preserved on the harness's
  `fake_client.rows[i].next_attempt_at` for assertions. Use
  `run_loops=True` if you need scheduled delivery to actually wait.
- **`cancel_timer` and `fetch_unprocessed` are patched** to operate on the
  fake client. The `session` argument is ignored in tests.
- **The fake producer uses the same envelope format as the real one**, so
  all serialization paths are exercised.
- **`lease_ttl_seconds` and re-delivery are not simulated** in sync mode —
  handlers that exceed the configured TTL in production may be re-delivered
  to another worker, but tests will only invoke the handler once.
  Idempotency must be verified separately. Use `run_loops=True` for tests
  that need to observe lease-expiry behavior.
- **`FakeOutboxClient.validate_schema()` raises `NotImplementedError`** —
  there is no real DB to validate against, and a silent pass would let
  users ship broken schemas while their tests stay green. Tests that need
  real schema validation must construct an `OutboxClient(real_engine,
  table)` against the same DSN the migrations ran against.

## Limitations of the fake broker

`TestOutboxBroker._fake_start` deliberately **skips the parent's
publisher-iteration loop** (the one that calls
`create_publisher_fake_subscriber`). FastStream's publisher-spy
infrastructure mocks the registered handler to forward
`publisher.publish()` calls — which conflicts with the outbox's real
dispatch path (the fake producer already lands rows in the fake client
*and* drives the real handler via `_sync_dispatch`).

If you need FastStream's publisher-mock semantics for an outbox test,
swap that override out before re-using the parent's `_fake_start`.

## pytest-asyncio configuration

Add to `pyproject.toml`:

```toml
[tool.pytest.ini_options]
asyncio_mode = "auto"
asyncio_default_fixture_loop_scope = "function"
```
