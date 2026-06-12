# Tutorial: Your first outbox app

## What you'll build

A tiny app where calling `broker.publish` inside a database transaction
triggers a handler — no message bus required, just Postgres. By the end
you will have run a single message end-to-end and seen the handler
print it.

## Before you start

- Python 3.13+
- Docker (for a one-line Postgres container)
- [uv](https://docs.astral.sh/uv/) for project setup
- Roughly ten minutes

## Step 1: Install

Start a fresh project directory and add the package. The `asyncpg`
extra brings the async Postgres driver; `validate` is the Alembic-based
schema check helper; `cli` is FastStream's `faststream run` command.

```bash
mkdir outbox-tutorial && cd outbox-tutorial
uv init
uv add 'faststream-outbox[asyncpg,validate]' 'faststream[cli]'
```

You should see:

```text
Initialized project `outbox-tutorial`
Using CPython 3.14.4
Creating virtual environment at: .venv
Resolved 26 packages in 61ms
Installed 24 packages in 37ms
 + alembic==1.18.4
 + annotated-types==0.7.0
 + anyio==4.13.0
 + asyncpg==0.31.0
 + click==8.4.1
 + fast-depends==3.0.8
 + faststream==0.7.1
 + faststream-outbox==0.8.0
 + greenlet==3.5.1
 + idna==3.18
 + mako==1.3.12
 + markdown-it-py==4.2.0
 + markupsafe==3.0.3
 + mdurl==0.1.2
 + pydantic==2.13.4
 + pydantic-core==2.46.4
 + pygments==2.20.0
 + rich==15.0.0
 + shellingham==1.5.4
 + sqlalchemy==2.0.50
 + typer==0.21.1
 + typing-extensions==4.15.0
 + typing-inspection==0.4.2
 + watchfiles==1.1.1
```

Your exact pinned versions will differ; that is fine. The Python version
line will reflect whatever `uv` resolves on your machine — 3.13 or 3.14
are both fine.

## Step 2: Start Postgres

Run a disposable Postgres 17 container with the credentials we'll wire
into the connection string in Step 3.

```bash
docker run --rm -d --name outbox-postgres \
    -e POSTGRES_USER=outbox -e POSTGRES_PASSWORD=outbox -e POSTGRES_DB=outbox \
    -p 5432:5432 postgres:17
```

You should see a container ID printed:

```text
7558ba0b8949e6410415f51152cd2da9b5eaab4ebae092aa14f2a6094f57d98f
```

Give it a couple of seconds and confirm it's ready:

```bash
docker logs outbox-postgres 2>&1 | tail -1
```

You should see:

```text
2026-06-12 05:05:44.529 UTC [1] LOG:  database system is ready to accept connections
```

## Step 3: Declare the outbox table

Create `app.py`. This sets up the SQLAlchemy `MetaData`, declares the
outbox table on it, builds an async engine, and wires the broker and
FastStream app. We'll add the handler in Step 5.

```python title="app.py"
from sqlalchemy import MetaData
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from faststream import FastStream
from faststream_outbox import OutboxBroker, make_outbox_table

metadata = MetaData()
outbox_table = make_outbox_table(metadata, table_name="outbox")

engine = create_async_engine("postgresql+asyncpg://outbox:outbox@localhost:5432/outbox")
broker = OutboxBroker(engine, outbox_table=outbox_table)
app = FastStream(broker)

session_factory = async_sessionmaker(engine, expire_on_commit=False)
```

`make_outbox_table` returns a `sqlalchemy.Table` attached to your
`MetaData`. The package never creates or migrates the schema on its
own — Step 4 is where we run that.

## Step 4: Create the schema

Create a second file, `create_schema.py`, that runs `metadata.create_all`
once. Real apps use Alembic; for a tutorial a one-shot script is the
honest shape.

```python title="create_schema.py"
import asyncio

from sqlalchemy import MetaData
from sqlalchemy.ext.asyncio import create_async_engine

from faststream_outbox import make_outbox_table

metadata = MetaData()
make_outbox_table(metadata, table_name="outbox")


async def main() -> None:
    engine = create_async_engine("postgresql+asyncpg://outbox:outbox@localhost:5432/outbox")
    async with engine.begin() as conn:
        await conn.run_sync(metadata.create_all)
    await engine.dispose()


asyncio.run(main())
```

*Real projects import `metadata` and `outbox_table` from a shared module
rather than redeclaring them here; this script is self-contained for the
tutorial's narrow scope.*

Run it:

```bash
uv run python create_schema.py
```

You should see no output — that is success.

Verify the table landed:

```bash
docker exec outbox-postgres psql -U outbox -d outbox -c '\d outbox'
```

You should see:

```text
                                          Table "public.outbox"
      Column      |           Type           | Collation | Nullable |              Default
------------------+--------------------------+-----------+----------+------------------------------------
 id               | bigint                   |           | not null | nextval('outbox_id_seq'::regclass)
 queue            | character varying(255)   |           | not null |
 payload          | bytea                    |           | not null |
 headers          | jsonb                    |           |          |
 attempts_count   | bigint                   |           | not null | '0'::bigint
 deliveries_count | bigint                   |           | not null | '0'::bigint
 created_at       | timestamp with time zone |           | not null | now()
 next_attempt_at  | timestamp with time zone |           | not null | now()
 first_attempt_at | timestamp with time zone |           |          |
 last_attempt_at  | timestamp with time zone |           |          |
 acquired_at      | timestamp with time zone |           |          |
 acquired_token   | uuid                     |           |          |
 timer_id         | character varying(255)   |           |          |
Indexes:
    "outbox_pkey" PRIMARY KEY, btree (id)
    "outbox_lease_idx" btree (queue, acquired_at) WHERE acquired_token IS NOT NULL
    "outbox_pending_idx" btree (queue, next_attempt_at) WHERE acquired_token IS NULL
    "outbox_timer_id_uq" UNIQUE, btree (queue, timer_id) WHERE timer_id IS NOT NULL
```

Three partial indexes show up alongside the columns — the broker uses
those at runtime; you don't need to think about them.

## Step 5: Define a handler

Add a subscriber to the bottom of `app.py`. The handler will run once
per row published to the `orders` queue.

```python title="app.py (additions)"
@broker.subscriber("orders")
async def handle(order_id: int) -> None:
    print(f"got order {order_id}")
```

No command yet — the handler runs once we publish a row and start the
app.

## Step 6: Publish a row

Add an `@app.after_startup` hook to the bottom of `app.py` that publishes
one row right after the app boots. `broker.publish` inserts an outbox
row through the session you give it — the row commits with the
surrounding transaction. There is no separate "send" step; the commit
is the send.

```python title="app.py (additions)"
@app.after_startup
async def publish_one() -> None:
    async with session_factory() as session, session.begin():
        await broker.publish(1, queue="orders", session=session)
```

The full `app.py` now reads:

```python title="app.py"
from sqlalchemy import MetaData
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from faststream import FastStream
from faststream_outbox import OutboxBroker, make_outbox_table

metadata = MetaData()
outbox_table = make_outbox_table(metadata, table_name="outbox")

engine = create_async_engine("postgresql+asyncpg://outbox:outbox@localhost:5432/outbox")
broker = OutboxBroker(engine, outbox_table=outbox_table)
app = FastStream(broker)

session_factory = async_sessionmaker(engine, expire_on_commit=False)


@broker.subscriber("orders")
async def handle(order_id: int) -> None:
    print(f"got order {order_id}")


@app.after_startup
async def publish_one() -> None:
    async with session_factory() as session, session.begin():
        await broker.publish(1, queue="orders", session=session)
```

## Step 7: Run it

Start the app:

```bash
uv run faststream run app:app
```

You should see:

```text
2026-06-12 08:07:06,179 INFO     - FastStream app starting...
2026-06-12 08:07:06,179 INFO     - orders  |  - `Handle` waiting for messages
2026-06-12 08:07:06,276 INFO     - FastStream app started successfully! To exit, press CTRL+C
2026-06-12 08:07:06,283 INFO     - orders  |  - Received
got order 1
2026-06-12 08:07:06,283 INFO     - orders  |  - Processed
```

That `got order 1` line is your handler firing. The row was inserted by
the `@app.after_startup` hook, the subscriber's fetch loop picked it up,
dispatched it, the handler ran, and the row was deleted.

Press `Ctrl-C`:

```text
2026-06-12 08:07:11,989 INFO     - FastStream app shutting down...
2026-06-12 08:07:11,990 INFO     - FastStream app shut down gracefully.
2026-06-12 08:07:11,990 INFO     -         |  - callback for Task-2 is being executed...
2026-06-12 08:07:11,990 INFO     -         |  - callback for Task-3 is being executed...
```

## What you just built

- An outbox table inside your own Postgres database, owned by your
  schema.
- A FastStream app whose "transport" is rows in that table — no
  external broker.
- A handler that ran exactly once, in-process, against a row committed
  by your own session.

The interesting property is what happened *inside* `publish_one`: the
`broker.publish` call inserted a row into the outbox table through the
session you opened. `session.begin()` committed it. If that commit had
rolled back — say, because a domain write on the same session
failed — the outbox row would have rolled back with it. There is no
universe where the row exists but the domain write doesn't, or vice
versa. That atomicity is the whole point.

## Clean up

```bash
docker stop outbox-postgres
```

## What's next

- [Subscriber reference](../usage/subscriber.md) — tuning, worker
  counts, retry strategies.
- [Publisher reference](../usage/publisher.md) — `publish_batch`, the
  `OutboxPublisher` decorator, chained publishing.
- [FastAPI integration](../usage/fastapi.md) — wire the outbox into
  a real HTTP service with `Depends(get_session)`.
- [Tutorial: Add a Kafka relay](./add-kafka-relay.md) — extend this
  app to forward each row into Kafka with one stacked decorator.
