# FastAPI integration

The outbox + FastAPI is the **canonical use case**: HTTP routes and outbox
subscribers share the same `AsyncSession` via FastAPI's dependency
injection, and the outbox row commits with the caller's domain writes —
same transaction, same `session.commit()`.

`faststream_outbox.fastapi.OutboxRouter` subclasses FastStream's
`StreamRouter` (which itself subclasses FastAPI's `APIRouter`), so HTTP
routes and outbox subscribers coexist on a single router.

## Install

```bash
pip install 'faststream-outbox[fastapi]'
```

## Quickstart

```python
from collections.abc import AsyncIterator

from fastapi import Depends, FastAPI
from sqlalchemy import MetaData
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from faststream_outbox import make_outbox_table
from faststream_outbox.fastapi import OutboxBroker, OutboxRouter


metadata = MetaData()
outbox_table = make_outbox_table(metadata, table_name="outbox")
engine = create_async_engine("postgresql+asyncpg://localhost/app")
session_factory = async_sessionmaker(engine, expire_on_commit=False)


async def get_session() -> AsyncIterator[AsyncSession]:
    async with session_factory() as s, s.begin():
        yield s


router = OutboxRouter(engine, outbox_table=outbox_table)


@router.subscriber("orders")
async def handle(
    body: dict,
    session: AsyncSession = Depends(get_session),
) -> None:
    ...  # domain writes on `session` commit with any chained outbox publishes


@router.post("/orders")
async def create_order(
    order: OrderIn,
    broker: OutboxBroker,
    session: AsyncSession = Depends(get_session),
) -> dict:
    session.add(Order(...))
    await broker.publish({"order_id": ...}, queue="orders", session=session)
    return {"ok": True}


app = FastAPI()
app.include_router(router)
```

Mounting the router auto-starts the inner broker via FastAPI's lifespan —
**you do not call `broker.start()`**. HTTP routes (`@router.get`,
`@router.post`, …) and outbox subscribers coexist on one router.

## Why this works

`StreamRouter` uses FastStream's `wrap_callable_to_fastapi_compatible`
bridge to plug FastAPI's dependency resolver into the FastStream consume
pipeline. So `Depends(get_session)` inside a subscriber handler resolves
the same way it would in an HTTP endpoint: a fresh `AsyncSession` per
delivery, opened in a `session.begin()` block, committed on handler return,
rolled back on exception.

A handler can therefore receive the **same** `AsyncSession` it would in an
HTTP route — and `OutboxResponse(session=...)` commits the follow-on row
with the handler's domain writes. See [Chained
publishing](./publisher.md#chained-publishing).

## Annotated context shortcuts

`faststream_outbox.fastapi` re-exports the same Annotated context
shortcuts as `faststream_outbox.annotations`, but FastAPI-aware:

```python
from faststream_outbox.fastapi import OutboxBroker, OutboxMessage


@router.subscriber("orders")
async def handle(
    msg: OutboxMessage,
    broker: OutboxBroker,
    session: AsyncSession = Depends(get_session),
) -> None:
    ...
```

They resolve via FastStream's `Context()` paths but go through FastAPI's
dependency resolver, so `Depends(...)` and these shortcuts can be mixed
freely.

## What's intentionally not exposed

`apply_types` and the broker's FastStream `dependencies` argument are
intentionally **not exposed** on `OutboxRouter.__init__`:

- `StreamRouter` forces `apply_types=False` because FastAPI's FastDepends
  takes over the parameter resolution. Letting the user flip it would
  produce weird half-resolved handlers.
- `dependencies` on the router signature means FastAPI `Depends(...)` only
  — the broker's FastStream `Dependant` list is the wrong shape for this
  flow.

If you need broker-level FastStream middlewares or dependencies, set them
on the broker before mounting the router and use the FastAPI `Depends(...)`
mechanism in handlers.

## Engine ownership

The caller owns the `AsyncEngine`. `OutboxBroker` does **not** close it —
typically your FastAPI app does, via `app.add_event_handler("shutdown",
engine.dispose)` or its lifespan context manager.
