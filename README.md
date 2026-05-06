faststream-outbox
==

[![Supported versions](https://img.shields.io/pypi/pyversions/faststream-outbox.svg)](https://pypi.python.org/pypi/faststream-outbox)
[![downloads](https://img.shields.io/pypi/dm/faststream-outbox.svg)](https://pypistats.org/packages/faststream-outbox)

`faststream-outbox` is a [FastStream](https://faststream.airt.ai) broker integration for the **transactional outbox pattern** — a Postgres table is the message queue.

A producer writes a domain entity and an outbox row in the *same* SQLAlchemy transaction. A subscriber polls the table directly with `FOR UPDATE SKIP LOCKED`, runs the handler, and deletes the row on success. No downstream broker, no separate relay process — the table *is* the queue.

```python
from sqlalchemy import MetaData, insert
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from faststream import FastStream
from faststream_outbox import OutboxBroker, encode_payload, make_outbox_table

metadata = MetaData()
outbox_table = make_outbox_table(metadata, table_name="outbox")

engine = create_async_engine("postgresql+asyncpg://localhost/app")
broker = OutboxBroker(engine, outbox_table=outbox_table)
app = FastStream(broker)

@broker.subscriber("orders", max_workers=4)
async def handle(order_id: int) -> None:
    print(f"order {order_id}")

# Producer side — your code, in your own transaction:
session_factory = async_sessionmaker(engine, expire_on_commit=False)
async with session_factory() as session, session.begin():
    session.add(Order(id=1))
    payload, headers = encode_payload(1)
    await session.execute(
        insert(outbox_table).values(queue="orders", payload=payload, headers=headers)
    )
```

## How it works

`make_outbox_table(metadata, table_name="outbox")` returns a `sqlalchemy.Table` that you attach to your own `MetaData` and migrate via Alembic. The package does **not** own your schema; it only describes the columns it needs.

A subscriber owns three async loops:

1. **fetch** — claims due rows via `SELECT … FOR UPDATE SKIP LOCKED → UPDATE state='processing', acquired_token=:uuid RETURNING *` in a single CTE.
2. **workers** (× `max_workers`) — dispatch to the handler. On success, `DELETE WHERE id=:id AND acquired_token=:token`. On failure, the retry strategy decides: schedule another attempt, or terminal `DELETE`.
3. **release-stuck** — periodically flips `processing` rows back to `pending` if their lease is older than `release_stuck_timeout`. Wrapped in a Postgres advisory lock so multiple processes don't compete.

The `acquired_token` is critical: a slow handler whose lease expired and was re-claimed by another worker will find its terminal `DELETE`/`UPDATE` to be a no-op (the token no longer matches), preventing it from clobbering the new lease holder's row.

## Recommended index

Add this to your Alembic migration alongside the table:

```sql
CREATE INDEX outbox_pending_idx ON outbox (queue, next_attempt_at)
  WHERE state = 'pending';
```

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
- `release_stuck_timeout` (default `300.0` s) — how long a `processing` row may live before being released back to `pending`.
- `release_stuck_interval` (default `release_stuck_timeout / 2`).
- `max_deliveries` (default `None` — unbounded) — total claims (including stuck-recovery re-claims) after which the row is dropped without invoking the handler. Defends against handlers that consistently wedge.

## 📝 [License](LICENSE)
