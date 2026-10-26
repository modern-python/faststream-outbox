# Publisher

There are three ways to write an outbox row:

1. `broker.publish(...)`: inline call, one row.
2. `broker.publish_batch(...)`: inline call, many rows in one INSERT.
3. `broker.publisher(queue, ...)`: a typed, queue-scoped wrapper for
   per-queue config and AsyncAPI spec coverage.

All three share the same transactional contract: the caller supplies an
`AsyncSession` or an `AsyncConnection`, and the row commits with the caller's
domain writes. The broker does not flush, commit, or open its own transaction.

For "consume from A → enqueue to B" relay flows, a fourth path is
available: returning `OutboxResponse(...)` from a handler. See [Chained
publishing](#chained-publishing) below.

There is no request/reply: the outbox is fire-and-forget, so `broker.request(...)`
raises `NotImplementedError`.

## `broker.publish`

```python
async with session_factory() as session, session.begin():
    session.add(order)  # domain write
    await broker.publish(
        {"order_id": order.id},
        queue="orders",
        session=session,
    )
```

Full signature:

```python
await broker.publish(
    body,
    *,
    queue: str,
    session: AsyncSession | AsyncConnection,
    headers: dict[str, str] | None = None,
    correlation_id: str | None = None,
    activate_in: timedelta | None = None,
    activate_at: datetime | None = None,
    timer_id: str | None = None,
    timer_conflict: Literal["ignore", "extend"] = "ignore",
) -> int | None
```

Returns the row's `id`, or `None` when a live row already holds the
`(queue, timer_id)`. `timer_conflict` decides what happens on that conflict:

- `"ignore"` (the default) leaves the existing row alone and returns `None`.
- `"extend"` moves an unclaimed timer's deadline later in place and returns
  its `id`. It returns `None` only when a worker has already claimed the
  timer. See [Extending a timer](./timers.md#extending-a-timer).

See [Timers](./timers.md) for `activate_in` / `activate_at` / `timer_id`.

Passing anything other than an `AsyncSession` or `AsyncConnection` raises
`TypeError`.

### Publishing on a Core connection

If your code uses SQLAlchemy Core without the ORM, pass the `AsyncConnection`
your domain writes run on. The parameter is still called `session`:

```python
async with engine.begin() as conn:
    await conn.execute(insert(orders).values(id=1))  # domain write
    await broker.publish({"order_id": 1}, queue="orders", session=conn)
```

Everything else works the same: the row commits or rolls back with `conn`'s
transaction, a savepoint from `conn.begin_nested()` scopes it like a session
savepoint, and a connection opened with `engine.connect()` without `begin()`
starts its transaction on the first publish, so you still have to call
`await conn.commit()`. `publish_batch`, `cancel_timer`, `fetch_unprocessed`,
`OutboxPublisher.publish` and `OutboxResponse` accept a connection too.

## `broker.publish_batch`

Inserts many rows in a single round-trip:

```python
async with session_factory() as session, session.begin():
    await broker.publish_batch(
        {"order_id": 1},
        {"order_id": 2},
        {"order_id": 3},
        queue="orders",
        session=session,
    )
```

Full signature:

```python
await broker.publish_batch(
    *bodies,
    queue: str,
    session: AsyncSession | AsyncConnection,
    headers: dict[str, str] | None = None,
    activate_in: timedelta | None = None,
    activate_at: datetime | None = None,
) -> None
```

`publish_batch` returns nothing and does not accept `timer_id`,
because per-row dedup makes no sense in a batch. It also accepts `activate_in` /
`activate_at` to schedule every row in the batch identically; the schedule
is applied client-side rather than server-side (a few-ms drift vs. the
single-`publish` path).

## `broker.publisher(queue, ...)`

`broker.publisher(queue, ...)` returns an `OutboxPublisher`: a typed,
queue-scoped wrapper around `broker.publish` with the same transactional
contract:

```python
orders_pub = broker.publisher("orders", headers={"source": "checkout"})


async def checkout(order: Order, session: AsyncSession) -> None:
    session.add(order)  # domain write
    await orders_pub.publish({"order_id": order.id}, session=session)
    await session.commit()  # row + domain commit together
```

Per-call `headers` are merged with the publisher's static headers
(per-call wins).

Constructor signature:

```python
broker.publisher(
    queue: str,
    *,
    headers: dict[str, str] | None = None,
    title: str | None = None,            # AsyncAPI operation title
    description: str | None = None,      # AsyncAPI operation description
    schema: Any | None = None,           # AsyncAPI payload schema override
    include_in_schema: bool = True,      # exclude from the AsyncAPI spec when False
) -> OutboxPublisher
```

The publisher exists primarily for AsyncAPI spec coverage and to
encapsulate per-queue config, which is why it has the `title` / `description` / `schema`
/ `include_in_schema` knobs above alongside the static `headers`.

### Not a relay decorator

It is standalone-only: stacking it as a relay decorator on a
subscriber (`@orders_pub @broker.subscriber("inbox", ...)`) raises
`NotImplementedError` at decoration time, because the dispatch loop has
no reachable `AsyncSession` without breaking the outbox transactional
contract.

For "consume from queue A → enqueue to queue B" relays, either call
`broker.publish(value, queue="B", session=session)` directly inside your
handler, on the same session that holds your domain writes, or
`return OutboxResponse(...)` (see below). (The inbound row's own terminal
DELETE runs separately, on the worker's autocommit connection, not this
session.)

## Chained publishing

For "consume from queue A → enqueue to queue B" flows, a handler can
`return OutboxResponse(body=..., queue="B", session=session)` instead of
calling `broker.publish(...)` manually. FastStream's response-publisher
flow routes the returned value through the producer; the same
transactional contract applies (you provide the session, the row commits
with your domain writes):

!!! note "The `session` must outlive the handler return"
    The returned `OutboxResponse` is published after the handler
    returns, so its `session` must still hold an open transaction at that
    point. A session from `Depends(...)` does not: FastStream and FastAPI
    both tear dependencies down inside the handler call, before the
    response is published. The same goes for an
    `async with session_factory() as session:` block inside the handler.
    In those cases the dispatch raises `RuntimeError` naming the missing
    transaction, logs it at ERROR, and retries the inbound row. Either
    open the session in a broker middleware, as below, or call
    `broker.publish(..., session=session)` inside the handler instead
    (see [§ Not a relay decorator](#not-a-relay-decorator)).

A broker middleware's `on_receive` / `after_processed` hooks wrap the
whole dispatch, response publish included, so a session opened there
covers both the handler's writes and the follow-on row. The commit runs
before the ack decision, so a failed commit retries the inbound row:

```python
from faststream import BaseMiddleware, Context
from sqlalchemy.ext.asyncio import AsyncSession

from faststream_outbox import OutboxBroker, OutboxResponse


class SessionPerMessage(BaseMiddleware):
    async def on_receive(self) -> None:
        self._begin = session_factory.begin()
        self.context.set_local("session", await self._begin.__aenter__())
        await super().on_receive()

    async def after_processed(self, exc_type=None, exc_val=None, exc_tb=None) -> bool | None:
        await self._begin.__aexit__(exc_type, exc_val, exc_tb)
        return await super().after_processed(exc_type, exc_val, exc_tb)


broker = OutboxBroker(engine, outbox_table=outbox_table, middlewares=[SessionPerMessage])


@broker.subscriber("orders")
async def handle(body: dict, session: AsyncSession = Context()) -> OutboxResponse:
    ...  # domain writes on `session`
    return OutboxResponse(
        body={"chained": True},
        queue="downstream",
        session=session,
    )
```

`correlation_id` comes from `headers["correlation_id"]` when you set the
header but not the argument, and otherwise from the inbound message, which
is useful for trace stitching. Plain returns (`None`, `dict`,
etc.) are silently skipped, so handlers that don't want to chain just
return normally.

!!! warning "Duplicate delivery on crash"
    The chained `downstream` row commits with the handler's transaction,
    but the inbound `orders` row's terminal `DELETE` runs after the
    handler returns, on the worker's separate autocommit connection. A
    crash between those two points leaves the inbound row undeleted, so it
    is redelivered and produces a second chained row. For non-idempotent
    chains, pass a deterministic `timer_id` derived from the inbound message
    so the duplicate insert is a no-op (see [Timers](./timers.md)).

## Annotated handler params

`faststream_outbox.annotations` exports `Annotated[..., Context(...)]`
shortcuts for the broker, producer, and client, useful when you want to
publish from inside a handler:

```python
from faststream_outbox.annotations import OutboxBroker, OutboxMessage


@broker.subscriber("orders")
async def handle(msg: OutboxMessage, broker: OutboxBroker) -> None:
    async with session_factory() as session, session.begin():
        await broker.publish({"chained": True}, queue="downstream", session=session)
```

For FastAPI handlers, import the same names from `faststream_outbox.fastapi`.
They resolve via the same `Context()` paths but go through FastAPI's
dependency resolver, so `Depends(...)` and these shortcuts can be mixed
freely. See [FastAPI integration](./fastapi.md).
