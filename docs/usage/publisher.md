# Publisher

There are three ways to write an outbox row:

1. **`broker.publish(...)`** — inline call, one row.
2. **`broker.publish_batch(...)`** — inline call, many rows in one INSERT.
3. **`broker.publisher(queue, ...)`** — a typed, queue-scoped wrapper for
   per-queue config and AsyncAPI spec coverage.

All three share the same transactional contract: the caller supplies an
`AsyncSession`, and the row commits with the caller's domain writes — the
broker does not flush, commit, or open its own transaction.

For "consume from A → enqueue to B" relay flows, a fourth path is
available: returning `OutboxResponse(...)` from a handler. See [Chained
publishing](#chained-publishing) below.

## `broker.publish`

```python
async with session_factory() as session, session.begin():
    session.add(order)                                    # domain write
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
    session: AsyncSession,
    headers: dict[str, str] | None = None,
    correlation_id: str | None = None,
    activate_in: timedelta | None = None,
    activate_at: datetime | None = None,
    timer_id: str | None = None,
) -> int | None
```

Returns the inserted row's `id`, or `None` if a `timer_id` conflict made
the insert a no-op. See [Timers](./timers.md) for `activate_in` /
`activate_at` / `timer_id`.

Passing anything that is not an `AsyncSession` raises `TypeError`.

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
    session: AsyncSession,
    headers: dict[str, str] | None = None,
    activate_in: timedelta | None = None,
    activate_at: datetime | None = None,
) -> None
```

`publish_batch` returns nothing and does **not** accept `timer_id` —
per-row dedup makes no sense in a batch. It also accepts `activate_in` /
`activate_at` to schedule every row in the batch identically; the schedule
is applied client-side rather than server-side (a few-ms drift vs. the
single-`publish` path).

## `broker.publisher(queue, ...)`

`broker.publisher(queue, ...)` returns an `OutboxPublisher` — a typed,
queue-scoped wrapper around `broker.publish` with the same transactional
contract:

```python
orders_pub = broker.publisher("orders", headers={"source": "checkout"})


async def checkout(order: Order, session: AsyncSession) -> None:
    session.add(order)                                  # domain write
    await orders_pub.publish({"order_id": order.id}, session=session)
    await session.commit()                              # row + domain commit together
```

Per-call `headers` are merged with the decorator's static headers
(per-call wins).

The publisher exists primarily for AsyncAPI spec coverage and to
encapsulate per-queue config (static headers, etc.).

### Not a relay decorator

It is **standalone-only**: stacking it as a relay decorator on a
subscriber (`@orders_pub @broker.subscriber("inbox", ...)`) raises
`NotImplementedError` at decoration time, because the dispatch loop has
no reachable `AsyncSession` without breaking the outbox transactional
contract.

For "consume from queue A → enqueue to queue B" relays, either call
`broker.publish(value, queue="B", session=session)` directly inside your
handler — on the same session that owns the inbound row's terminal write —
or `return OutboxResponse(...)` (see below).

## Chained publishing

For "consume from queue A → enqueue to queue B" flows, a handler can
`return OutboxResponse(body=..., queue="B", session=session)` instead of
calling `broker.publish(...)` manually. FastStream's response-publisher
flow routes the returned value through the producer; the same
transactional contract applies (you provide the session, the row commits
with your domain writes):

```python
from faststream_outbox import OutboxMessage, OutboxResponse


@broker.subscriber("orders")
async def handle(
    msg: OutboxMessage,
    session: AsyncSession = Depends(get_session),
) -> OutboxResponse:
    ...  # process inbound
    return OutboxResponse(
        body={"chained": True},
        queue="downstream",
        session=session,
    )
```

`correlation_id` propagates from the inbound message if you don't set one
explicitly — useful for trace stitching. Plain returns (`None`, `dict`,
etc.) are silently skipped, so handlers that don't want to chain just
return normally.

## Annotated handler params

`faststream_outbox.annotations` exports `Annotated[..., Context(...)]`
shortcuts for the broker, producer, and client — useful when you want to
publish from inside a handler:

```python
from faststream_outbox.annotations import OutboxBroker, OutboxMessage


@broker.subscriber("orders")
async def handle(msg: OutboxMessage, broker: OutboxBroker) -> None:
    async with session_factory() as session, session.begin():
        await broker.publish({"chained": True}, queue="downstream", session=session)
```

For FastAPI handlers, import the same names from `faststream_outbox.fastapi`
— they resolve via the same `Context()` paths but go through FastAPI's
dependency resolver so `Depends(...)` and these shortcuts can be mixed
freely. See [FastAPI integration](./fastapi.md).
