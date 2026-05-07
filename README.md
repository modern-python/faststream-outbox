faststream-outbox
==

[![Supported versions](https://img.shields.io/pypi/pyversions/faststream-outbox.svg)](https://pypi.python.org/pypi/faststream-outbox)
[![downloads](https://img.shields.io/pypi/dm/faststream-outbox.svg)](https://pypistats.org/packages/faststream-outbox)

`faststream-outbox` is a [FastStream](https://faststream.airt.ai) broker integration for the **transactional outbox pattern** — a Postgres table is the message queue.

A producer writes a domain entity and an outbox row in the *same* SQLAlchemy transaction by calling `broker.publish(body, queue=..., session=session)`. A subscriber polls the table directly with `FOR UPDATE SKIP LOCKED`, runs the handler, and deletes the row on success. No downstream broker, no separate relay process — the table *is* the queue.

```python
from sqlalchemy import MetaData
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from faststream import FastStream
from faststream_outbox import OutboxBroker, make_outbox_table

metadata = MetaData()
outbox_table = make_outbox_table(metadata, table_name="outbox")

engine = create_async_engine("postgresql+asyncpg://localhost/app")
broker = OutboxBroker(engine, outbox_table=outbox_table)
app = FastStream(broker)

@broker.subscriber("orders", max_workers=4)
async def handle(order_id: int) -> None:
    print(f"order {order_id}")

# Producer side — share the caller's open transaction:
session_factory = async_sessionmaker(engine, expire_on_commit=False)
async with session_factory() as session, session.begin():
    session.add(Order(id=1))
    await broker.publish(1, queue="orders", session=session)
```

## How it works

`make_outbox_table(metadata, table_name="outbox")` returns a `sqlalchemy.Table` that you attach to your own `MetaData` and migrate via Alembic. The package does **not** own your schema; it only describes the columns it needs.

`broker.publish(body, *, queue, session, headers=None, correlation_id=None, activate_in=None, activate_at=None, timer_id=None)` inserts one outbox row through the caller's `AsyncSession`. It does not flush, commit, or open its own transaction — the whole point is that the row commits atomically with the caller's domain writes. Use it inside an `async with session.begin():` block. See [Timers](#timers-delayed-delivery) for `activate_in` / `activate_at` / `timer_id`.

`broker.publish_batch(*bodies, queue, session, headers=None, activate_in=None, activate_at=None)` inserts many rows in a single round-trip with the same transactional contract.

A subscriber owns two async loops:

1. **fetch** — claims available rows via `SELECT … FOR UPDATE SKIP LOCKED → UPDATE acquired_token=:uuid, acquired_at=now() RETURNING *` in a single CTE. A row is "available" iff its lease is unset *or* expired (`acquired_at < now() - lease_ttl_seconds`), so the fetch query reclaims stuck rows inline — no separate reaper is needed. With the asyncpg driver, the loop also `LISTEN`s on `outbox_<table>` and `publish` emits `pg_notify(...)`, so idle dispatch latency is sub-100ms instead of up to `max_fetch_interval`. Polling stays as the fallback.
2. **workers** (× `max_workers`) — dispatch to the handler. On success, `DELETE WHERE id=:id AND acquired_token=:token`. On failure, the retry strategy decides: schedule another attempt, or terminal `DELETE`.

The `acquired_token` is critical: a slow handler whose lease expired and was re-claimed by another worker will find its terminal `DELETE`/`UPDATE` to be a no-op (the token no longer matches), preventing it from clobbering the new lease holder's row.

`lease_ttl_seconds` (default `60.0`) **must exceed your handler's P99 duration with margin** — otherwise healthy in-flight handlers race their own lease expiry and the row gets re-claimed by another worker, triggering a duplicate delivery.

## Timers (delayed delivery)

Schedule a publish to fire later by passing `activate_in` (relative) or `activate_at` (absolute, tz-aware) — exactly one. Pass `timer_id` to deduplicate per `(queue, timer_id)`; cancel a not-yet-leased timer with `broker.cancel_timer(...)`.

```python
import datetime as dt

# Fire 30 seconds from now, deduplicated by timer_id:
await broker.publish(
    {"order_id": 1},
    queue="orders",
    session=session,
    activate_in=dt.timedelta(seconds=30),
    timer_id=f"order-confirm-{order.id}",
)

# Fire at a specific UTC instant:
await broker.publish(
    {"x": 1}, queue="orders", session=session,
    activate_at=dt.datetime(2026, 6, 1, 9, tzinfo=dt.UTC),
)

# Cancel before it fires (no-op if the row is already in flight):
await broker.cancel_timer(queue="orders", timer_id="order-confirm-42", session=session)
```

`publish` returns the inserted row's `id`, or `None` if a row with the same `(queue, timer_id)` already exists. `cancel_timer` returns `True` if it deleted the row; `False` means either the timer didn't exist or was already leased to a worker (in which case delivery completes normally).

`publish_batch` accepts `activate_in` / `activate_at` to schedule every row in the batch identically — but no `timer_id` (per-row dedup makes no sense for a batch).

**Latency floor:** firing latency is bounded by the subscriber's `max_fetch_interval` (default 10s) after `next_attempt_at` elapses. Lower it for sub-10s precision; sub-second precision is not a goal of this broker.

## Schema validation

Schema validation is opt-in:

```python
await broker.validate_schema()  # raises if user's table drifts from expected columns
```

Call it from a `/health` endpoint or startup hook — not at `broker.start()`, so Alembic can run migrations against the same DB without a startup loop.

## Retry strategies

```python
from faststream_outbox import ExponentialRetry, ConstantRetry, LinearRetry, NoRetry

@broker.subscriber(
    "orders",
    retry_strategy=ExponentialRetry(
        initial_delay_seconds=1.0,
        max_delay_seconds=300.0,
        max_attempts=5,
        jitter_factor=0.5,
    ),
)
async def handle(order_id: int) -> None: ...
```

Strategies receive the raised `exception` so users may subclass for "retry only on transient errors":

```python
class TransientOnly(ExponentialRetry):
    def get_next_attempt_at(self, *, exception=None, **kw):
        if exception and not isinstance(exception, TransientError):
            return None
        return super().get_next_attempt_at(exception=exception, **kw)
```

## Failure modes

- **Handlers must be idempotent.** Crash between commit-of-handler-side-effects and the broker's `DELETE` re-delivers the message.
- **Best-effort ordering only.** `FOR UPDATE SKIP LOCKED` does not preserve strict order under concurrent workers. If you need strict per-aggregate ordering, route to a single subscriber and run a single worker.
- **No DLQ / archive.** Terminal failures `DELETE` the row. Hook `on_terminal_failure(row)` to capture them in your own table or alerting.

## Connection ownership

`OutboxBroker` does **not** close the `AsyncEngine` you pass in — the caller owns its lifecycle.

## Tuning

Per-subscriber knobs (passed to `@broker.subscriber("…", …)`):

- `max_workers` (default `1`) — concurrent handlers per subscriber.
- `fetch_batch_size` (default `10`) — rows claimed per fetch cycle.
- `min_fetch_interval` / `max_fetch_interval` (default `1.0` / `10.0` s) — base + ceiling for the adaptive idle backoff with jitter.
- `lease_ttl_seconds` (default `60.0` s) — how long a claim is valid before another fetch may reclaim it. **Must exceed your handler's P99 duration with margin.**
- `max_deliveries` (default `None` — unbounded) — total claims (including lease-expiry re-claims) after which the row is dropped without invoking the handler. Defends against handlers that consistently wedge.

## 📝 [License](LICENSE)
