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

A subscriber owns two async loops: a **fetch** loop claims available rows via a single CTE (`SELECT … FOR UPDATE SKIP LOCKED → UPDATE acquired_token=:uuid, acquired_at=now() RETURNING *`), and `max_workers` **worker** loops dispatch to the handler. On success, `DELETE WHERE id=:id AND acquired_token=:token`; on failure, the retry strategy schedules another attempt or terminally drops the row. Terminal failures `DELETE` by default; pass `dlq_table=make_dlq_table(metadata)` to atomically archive them into a sibling audit table instead — see [Dead-letter queue](https://faststream-outbox.readthedocs.io/en/latest/usage/dlq/).

The `acquired_token` is the load-bearing invariant: a slow handler whose lease expired and was re-claimed by another worker finds its terminal `DELETE` to be a no-op (the token no longer matches), preventing it from clobbering the new lease holder.

With the `asyncpg` driver, the fetch loop also `LISTEN`s on `outbox_<table>` and `publish` emits `pg_notify(...)`, so idle dispatch latency is sub-100ms instead of up to `max_fetch_interval`.

See [How it works](https://faststream-outbox.readthedocs.io/en/latest/introduction/how-it-works/) for the full architecture.

## Optional extras

- `faststream-outbox[asyncpg]` — asyncpg driver (enables `LISTEN/NOTIFY` for sub-100ms idle dispatch)
- `faststream-outbox[fastapi]` — FastAPI integration via `OutboxRouter`
- `faststream-outbox[validate]` — Alembic for `broker.validate_schema()`
- `faststream-outbox[prometheus]` — Prometheus metrics adapter
- `faststream-outbox[opentelemetry]` — OpenTelemetry metrics adapter

## Acknowledgements

The architecture of this package is heavily informed by Arseniy Popov's [PR #2704](https://github.com/ag2ai/faststream/pull/2704) (`feat: add sqla broker`) on upstream FastStream — the FastStream broker/registrator/subscriber wiring, the `SELECT … FOR UPDATE SKIP LOCKED` fetch-and-claim CTE, the retry strategy hierarchy, and the in-transaction publish contract all originate from there. This package is a Postgres-only reimplementation that diverges in storage model (lease tokens instead of an explicit state column, archive table is opt-in), loop structure (two loops instead of four), wake-up mechanism (`LISTEN/NOTIFY`), and adds timer mechanics. Credit for the original design belongs to Arseniy.

## Part of `modern-python`

Browse the full list of templates and libraries in
[`modern-python`](https://github.com/modern-python) — see the org profile for the
categorized index.

## 📚 [Documentation](https://faststream-outbox.readthedocs.io)

## 📦 [PyPi](https://pypi.org/project/faststream-outbox)

## 📝 [License](LICENSE)
