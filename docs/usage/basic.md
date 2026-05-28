# Basic usage

## 1. Declare the outbox table

The package never creates or migrates your schema — that's Alembic's job.
`make_outbox_table(metadata, table_name="outbox")` returns a
`sqlalchemy.Table` you attach to your own `MetaData`:

```python
from sqlalchemy import MetaData
from faststream_outbox import make_outbox_table

metadata = MetaData()
outbox_table = make_outbox_table(metadata, table_name="outbox")
```

The returned `Table` carries three indexes the broker needs at runtime — a
partial index for the fetch CTE's unleased branch, a partial index for the
expired-lease reclaim branch, and a partial unique index for `timer_id`
deduplication. Alembic autogenerate picks them up alongside the table itself.

## 2. Create the broker and app

```python
from sqlalchemy.ext.asyncio import create_async_engine
from faststream import FastStream
from faststream_outbox import OutboxBroker

engine = create_async_engine("postgresql+asyncpg://outbox:outbox@localhost:5432/outbox")
broker = OutboxBroker(engine, outbox_table=outbox_table)
app = FastStream(broker)
```

## 3. Register a subscriber

Subscribers work like any FastStream subscriber. Decorate a handler with
`@broker.subscriber(queue, ...)`:

```python
@broker.subscriber("orders", max_workers=4)
async def handle(order_id: int) -> None:
    print(f"order {order_id}")
```

See [Subscriber](./subscriber.md) for the full options list, tuning guide,
and retry strategies.

## 4. Publish a message

`broker.publish(body, *, queue, session, ...)` inserts an outbox row through
the caller's `AsyncSession`. It does **not** flush, commit, or open its own
transaction — the row commits with the caller's domain writes:

```python
from sqlalchemy.ext.asyncio import async_sessionmaker

session_factory = async_sessionmaker(engine, expire_on_commit=False)

async with session_factory() as session, session.begin():
    session.add(Order(id=1))
    await broker.publish(1, queue="orders", session=session)
    # session.begin() commits both atomically on exit
```

Passing anything that is not an `AsyncSession` raises `TypeError`. The whole
point of the outbox pattern is that the row commits atomically with your
domain writes; opening a separate session would defeat it.

See [Publisher](./publisher.md) for `publish_batch`, the `OutboxPublisher`
decorator, and chained publishing via `OutboxResponse`.

## Full quickstart

```python
from sqlalchemy import MetaData
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from faststream import FastStream
from faststream_outbox import OutboxBroker, make_outbox_table

metadata = MetaData()
outbox_table = make_outbox_table(metadata, table_name="outbox")

engine = create_async_engine("postgresql+asyncpg://outbox:outbox@localhost:5432/outbox")
broker = OutboxBroker(engine, outbox_table=outbox_table)
app = FastStream(broker)


@broker.subscriber("orders", max_workers=4)
async def handle(order_id: int) -> None:
    print(f"order {order_id}")


session_factory = async_sessionmaker(engine, expire_on_commit=False)


@app.after_startup
async def publish_one() -> None:
    async with session_factory() as session, session.begin():
        await broker.publish(1, queue="orders", session=session)
```

Run with `faststream run app:app`.

## Connection ownership

`OutboxBroker` does **not** close the `AsyncEngine` you pass in — the
caller owns its lifecycle. The same engine can be shared with other
SQLAlchemy users (your FastAPI app, an Alembic upgrade, etc.); closing it
from the broker would surprise them. Manage the engine with `try/finally`
or — when running under FastAPI — let the framework's lifespan handle it
(see [FastAPI integration](./fastapi.md)).

## Where to read next

- [How it works](../introduction/how-it-works.md) — architecture, lease invariant, at-least-once semantics
- [Subscriber](./subscriber.md) — tuning, retry strategies, slow-handler queue segregation
- [Publisher](./publisher.md) — `publish_batch`, `OutboxPublisher`, chained publishing
- [FastAPI integration](./fastapi.md) — `OutboxRouter`, `Depends(get_session)` pattern
