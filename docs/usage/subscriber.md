# Subscriber

Use `@broker.subscriber(queue, ...)` to register a handler for a queue.

## Basic example

```python
from faststream import FastStream
from faststream_outbox import OutboxBroker

broker: OutboxBroker = ...
app = FastStream(broker)


@broker.subscriber("orders")
async def handle(order_id: int) -> None:
    print(f"order {order_id}")
```

## Multiple queues per subscriber

The first argument is `queues: str | list[str]`. Pass a list to fan one
handler across several queues:

```python
@broker.subscriber(["orders", "refunds"])
async def handle(body: dict) -> None: ...
```

The subscriber claims rows from any of its queues in a single fetch. Its
[connection budget](#connection-budget) is unchanged — `max_workers + 1`
pool connections regardless of how many queues it serves.

Do **not** register two subscribers on the **same** queue: they compete for
the same rows, and registration emits a warning to that effect. To run more
than one handler over a queue, attach them to a single subscriber; to scale
throughput, raise `max_workers`.

## Body types

FastStream deserializes the message body into the annotated type. Any
JSON-serializable type works:

```python
from dataclasses import dataclass


@dataclass
class Order:
    order_id: str
    amount: float


@broker.subscriber("orders")
async def handle(body: Order) -> None:
    print(f"order {body.order_id} for {body.amount}")
```

## Annotated handler params

`faststream_outbox.annotations` exports `Annotated[..., Context(...)]`
shortcuts so handler signatures stay concise:

```python
from faststream_outbox.annotations import OutboxBroker, OutboxMessage


@broker.subscriber("orders")
async def handle(msg: OutboxMessage, broker: OutboxBroker) -> None: ...
```

`OutboxMessage`, `OutboxBroker`, `OutboxProducer`, and `OutboxClient` are
all available. For FastAPI handlers, import the same names from
`faststream_outbox.fastapi` — they resolve via the same `Context()` paths
but go through FastAPI's dependency resolver so `Depends(...)` and these
shortcuts can be mixed freely.

## Subscriber options

Per-subscriber knobs, passed to `@broker.subscriber("…", …)`:

| Parameter | Default | Description |
|---|---|---|
| `max_workers` | `1` | Concurrent handlers per subscriber |
| `fetch_batch_size` | `10` | Rows claimed per fetch cycle |
| `min_fetch_interval` | `1.0` s | Base for the adaptive idle backoff (jittered ±50%, so an actual wait can land below it) and the wait when the inflight queue is full; no sleep at all while fetches keep returning rows |
| `max_fetch_interval` | `10.0` s | Ceiling for the adaptive idle backoff (with jitter) |
| `lease_ttl_seconds` | `60.0` s | How long a claim is valid before another fetch may reclaim it. **Must exceed your handler's P99 with margin.** |
| `max_deliveries` | `None` (unbounded) | Total claims (including lease-expiry re-claims) after which the row is dropped without invoking the handler. Defends against handlers that consistently wedge. |
| `ack_policy` | `AckPolicy.NACK_ON_ERROR` | See [Ack policy](#ack-policy) |
| `retry_strategy` | `ExponentialRetry(...)` | See [Retry strategies](#retry-strategies) |

```python
@broker.subscriber(
    "high-priority",
    max_workers=8,
    fetch_batch_size=50,
    min_fetch_interval=0.1,
    max_fetch_interval=1.0,
    lease_ttl_seconds=120.0,
)
async def handle_urgent(body: dict) -> None: ...
```

The factory in `subscriber/factory.py` warns or raises on likely-wrong
combinations (`lease_ttl_seconds <= max_fetch_interval`, `max_deliveries`
without retry, `min_fetch_interval > max_fetch_interval`, etc.).

## Slow handlers — dedicated queue

When a handler's tail latency exceeds the subscriber's `lease_ttl_seconds`,
the row's lease expires mid-flight and another fetch reclaims it →
duplicate delivery. Don't hike `lease_ttl_seconds` globally — that delays
reclaim of *actually* stuck rows everywhere. Instead, segregate slow work
onto its own subscriber with a longer TTL:

```python
@broker.subscriber("slow_q", lease_ttl_seconds=600)   # 10 minutes
async def heavy_job(msg): ...


@broker.subscriber("fast_q", lease_ttl_seconds=30)
async def quick_job(msg): ...
```

Pick `lease_ttl_seconds` strictly greater than that subscriber's P99 handler
duration, with margin for clock skew. The tight TTL on the fast queue keeps
stuck-row reclaim fast; the tall TTL on the slow queue tolerates outliers
without slowing reclaim of genuinely stuck rows elsewhere. Producers route
to the appropriate queue at `publish` time.

*See also [Troubleshooting § `event=lease_lost`](../operations/troubleshooting.md#event-lease_lost-recurring-in-logs).*

## Ack policy

The default is `AckPolicy.NACK_ON_ERROR`: on a handler exception, the retry
strategy decides whether to schedule another attempt or terminally drop the
row.

| Policy | Effect |
|---|---|
| `AckPolicy.NACK_ON_ERROR` (default) | Consult the retry strategy on handler exceptions |
| `AckPolicy.REJECT_ON_ERROR` | Delete on the first failure (the retry strategy is ignored) |
| `AckPolicy.MANUAL` | Handler must call `await msg.ack()` / `nack()` / `reject()` itself |
| `AckPolicy.ACK_FIRST` | **Not supported.** Passing it raises `ValueError` at registration |

`ACK_FIRST` would delete the row *before* the handler runs, so a handler
crash silently drops the message — defeating the outbox reliability
guarantee. The factory rejects it at registration.

```python
from faststream import AckPolicy
from faststream_outbox.annotations import OutboxMessage


@broker.subscriber("audit", ack_policy=AckPolicy.MANUAL)
async def handle(msg: OutboxMessage, body: dict) -> None:
    try:
        await write_audit(body)
        await msg.ack()
    except TransientError:
        await msg.nack()    # retry
    except PermanentError:
        await msg.reject()  # terminal delete
```

!!! warning "MANUAL: returning without acking is a terminal reject"
    Under `AckPolicy.MANUAL`, a handler that returns **without** calling
    `ack()` / `nack()` / `reject()` (and without raising) is treated as a
    terminal **reject** — the row is **deleted** (or written to the DLQ with
    `failure_reason="rejected"` if a `dlq_table` is configured), not retried.
    A handler that *raises* is nacked through the retry strategy instead, so
    only the silent-return path is destructive. Always ack/nack/reject on
    every branch.

## Retry strategies

A subscriber with no explicit `retry_strategy` defaults to
`ExponentialRetry(initial_delay_seconds=1.0, multiplier=2.0,
max_delay_seconds=300.0, max_attempts=10, jitter_factor=0.2)`. Defaulting
to "delete on first error" is the wrong contract for an outbox; users
wanting that behavior must explicitly pass `NoRetry()`.

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


@broker.subscriber("audit", retry_strategy=NoRetry())  # opt out of retries
async def handle_audit(payload: dict) -> None: ...
```

`ConstantRetry` and `LinearRetry` accept `jitter_factor` (default `0.0`);
when non-zero, the computed delay is multiplied by `1 +
U(-jitter_factor/2, +jitter_factor/2)` to spread out retries, matching
`ExponentialRetry`'s shape.

### Retry only on transient errors

Strategies receive the raised `exception` so users may subclass for
"retry only on transient errors":

```python
class TransientOnly(ExponentialRetry):
    def get_next_attempt_delay(self, *, exception=None, **kw):
        if exception and not isinstance(exception, TransientError):
            return None  # terminal — DELETE
        return super().get_next_attempt_delay(exception=exception, **kw)
```

Returning `None` from `get_next_attempt_delay` signals a terminal failure.
`_RetryStrategyTemplate` also enforces `max_attempts` and
`max_total_delay_seconds` for you.

## Connection budget

Each subscriber holds `max_workers + 1` long-lived SQLAlchemy pool
connections (one writer per worker + one fetch), plus one raw asyncpg
connection for `LISTEN` when available. Size your **engine pool** for
`Σ subscribers × (max_workers + 1)`. An undersized pool does **not** block
`broker.start()` — `start()` only schedules the loop tasks and returns;
instead the fetch/worker loops stall on pool checkout and surface as
repeating reconnect ERROR logs with dispatch silently starved. SQLAlchemy's
default `pool_size=5, max_overflow=10` covers a handful of single-worker
subscribers; raise it for larger fleets.

Server-side, the footprint is one larger: the raw asyncpg `LISTEN`
connection lives **outside** the pool, so each subscriber consumes
`max_workers + 2` Postgres connections. The budget is **per process** —
each replica opens its own pool and LISTEN connections, so your Postgres
`max_connections` needs to cover `replicas × Σ subscribers × (max_workers +
2)`, otherwise additional replicas (or rolling deployments) are refused at
startup with `FATAL: too many connections`.

*Operator-side: [Production checklist § Sizing](../operations/checklist.md#sizing).*

## Read-only inspection

`subscriber.get_one()` and `async for msg in subscriber:` are **not
supported** on `OutboxSubscriber` — they would acquire a lease and bump
`deliveries_count`, surprising semantics for a peek API. Use
`broker.fetch_unprocessed(session=..., queue=...)` for lease-free reads of
the current table state.
