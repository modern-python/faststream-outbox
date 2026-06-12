# Router

`OutboxRouter` lets you define subscribers in separate modules and attach
them to the broker, the same as FastStream's built-in router pattern. This
page covers the vanilla FastStream router; for the FastAPI variant see
[FastAPI integration](./fastapi.md).

## Creating a router

```python
from faststream_outbox import OutboxRouter


router = OutboxRouter()


@router.subscriber("orders")
async def handle_order(order_id: int) -> None:
    print(f"order {order_id}")
```

## No `prefix`

Unlike some FastStream routers, `OutboxRouter` does **not** accept a
`prefix` argument. Queues are routed by their literal name, so producers
and consumers must agree on the exact string. If you want namespacing
(e.g., one Postgres instance shared across services), put it in the queue
name itself:

```python
@router.subscriber("checkout.orders")
async def handle_order(...): ...
```

The reason is simple: the outbox row's `queue` column is what the fetch
CTE filters on, and adding an implicit prefix would mean producers need to
know which router published the subscriber. Explicit queue names keep that
contract local.

## Including a router in the broker

```python
from faststream import FastStream
from faststream_outbox import OutboxBroker

from myapp.routers import router

broker = OutboxBroker(engine, outbox_table=outbox_table)
broker.include_router(router)
app = FastStream(broker)
```

## Defining routes up-front with `OutboxRoute`

`OutboxRoute` lets you declare handler + queue together without using
decorators, which is useful for code-gen or plugin patterns:

```python
from faststream_outbox import OutboxRouter
from faststream_outbox.router import OutboxRoute


async def handle_order(order_id: int) -> None:
    print(f"order {order_id}")


router = OutboxRouter(
    handlers=[
        OutboxRoute(handle_order, "orders", max_workers=4),
    ],
)
```

All `@broker.subscriber` options (`max_workers`, `retry_strategy`,
`fetch_batch_size`, `lease_ttl_seconds`, `max_deliveries`, `ack_policy`,
…) are accepted by `OutboxRoute` and `router.subscriber` — see the
[subscriber page](./subscriber.md) for the full list.

## Gotcha: walking every subscriber

Subscribers registered via `OutboxRouter` (then
`broker.include_router(router)`) live on the router, not on
`broker._subscribers`. If you need to introspect every subscriber on a
broker — counting active queues, asserting on schema, etc. — walk
`broker.subscribers` (the property):

```python
for sub in broker.subscribers:
    ...
```

The property iterates `[*broker._subscribers,
*(s for r in broker.routers for s in r.subscribers)]`, so it covers both
inline and router-attached subscribers. The bare `broker._subscribers` list
will silently miss everything attached via a router.
