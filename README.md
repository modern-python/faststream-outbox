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

**Optional extras:**

- `faststream-outbox[asyncpg]` — asyncpg driver (enables `LISTEN/NOTIFY` for sub-100ms idle dispatch).
- `faststream-outbox[fastapi]` — FastAPI integration via `OutboxRouter` (see [FastAPI integration](#fastapi-integration)).
- `faststream-outbox[validate]` — Alembic for `broker.validate_schema()` (see [Schema validation](#schema-validation)).

## How it works

`make_outbox_table(metadata, table_name="outbox")` returns a `sqlalchemy.Table` that you attach to your own `MetaData` and migrate via Alembic. The package does **not** own your schema; it only describes the columns it needs.

`broker.publish(body, *, queue, session, headers=None, correlation_id=None, activate_in=None, activate_at=None, timer_id=None)` inserts one outbox row through the caller's `AsyncSession`. It does not flush, commit, or open its own transaction — the whole point is that the row commits atomically with the caller's domain writes. Use it inside an `async with session.begin():` block. See [Timers](#timers-delayed-delivery) for `activate_in` / `activate_at` / `timer_id`.

`broker.publish_batch(*bodies, queue, session, headers=None, activate_in=None, activate_at=None)` inserts many rows in a single round-trip with the same transactional contract.

### Publisher decorator

`broker.publisher(queue, *, headers=None, middlewares=(), title=None, description=None, schema=None, include_in_schema=True)` returns an `OutboxPublisher` — a typed, queue-scoped wrapper around `broker.publish` with the same transactional contract:

```python
orders_pub = broker.publisher("orders", headers={"source": "checkout"})

async def checkout(order: Order, session: AsyncSession) -> None:
    session.add(order)                                  # your domain write
    await orders_pub.publish({"order_id": order.id}, session=session)
    await session.commit()                              # row + domain commit together
```

The publisher exists primarily for AsyncAPI spec coverage and to encapsulate per-queue config (static headers, etc.). It is **standalone-only**: stacking it as a relay decorator on a subscriber (`@orders_pub @broker.subscriber("inbox", ...)`) raises `NotImplementedError` at decoration time, because the dispatch loop has no reachable `AsyncSession` without breaking the outbox transactional contract. For "consume from queue A → enqueue to queue B" relays, either call `broker.publish(value, queue="B", session=session)` directly inside your handler — on the same session that owns the inbound row's terminal write — or `return OutboxResponse(...)` (see [Handler return type](#handler-return-type--chained-publishing)).

Per-publisher `middlewares=` wrap every `publisher.publish(...)` call — useful for tracing spans, metrics counters, or audit-log writes scoped to a single queue without affecting other publishers.

### Handler return type — chained publishing

For "consume from queue A → enqueue to queue B" flows, a handler can `return OutboxResponse(body=..., queue="B", session=session)` instead of calling `broker.publish(...)` manually. FastStream's response-publisher flow routes the returned value through the producer; the same transactional contract applies (you provide the session, the row commits with your domain writes):

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

`correlation_id` propagates from the inbound message if you don't set one explicitly — useful for trace stitching. Plain returns (`None`, `dict`, etc.) are silently skipped, so handlers that don't want to chain just return normally.

### Annotated handler params

`faststream_outbox.annotations` exports `Annotated[..., Context(...)]` shortcuts so handler signatures stay concise:

```python
from faststream_outbox.annotations import OutboxBroker, OutboxMessage

@broker.subscriber("orders")
async def handle(msg: OutboxMessage, broker: OutboxBroker) -> None: ...
```

The same names live in `faststream_outbox.fastapi` for FastAPI handlers — they resolve via the same `Context()` paths but go through FastAPI's dependency resolver so `Depends(...)` and these shortcuts can be mixed freely.

A subscriber owns two async loops:

1. **fetch** — claims available rows via `SELECT … FOR UPDATE SKIP LOCKED → UPDATE acquired_token=:uuid, acquired_at=now() RETURNING *` in a single CTE. A row is "available" iff its lease is unset *or* expired (`acquired_at < now() - lease_ttl_seconds`), so the fetch query reclaims stuck rows inline — no separate reaper is needed. With the asyncpg driver, the loop also `LISTEN`s on `outbox_<table>` and `publish` emits `pg_notify(...)`, so idle dispatch latency is sub-100ms instead of up to `max_fetch_interval`. Polling stays as the fallback.
2. **workers** (× `max_workers`) — dispatch to the handler. On success, `DELETE WHERE id=:id AND acquired_token=:token`. On failure, the retry strategy decides: schedule another attempt, or terminal `DELETE`.

The `acquired_token` is critical: a slow handler whose lease expired and was re-claimed by another worker will find its terminal `DELETE`/`UPDATE` to be a no-op (the token no longer matches), preventing it from clobbering the new lease holder's row.

`lease_ttl_seconds` (default `60.0`) **must exceed your handler's P99 duration with margin** — otherwise healthy in-flight handlers race their own lease expiry and the row gets re-claimed by another worker, triggering a duplicate delivery.

When that happens the broker emits a WARNING log record with structured fields (`extra={"event": "lease_lost", "phase": "terminal" | "retry", "row_id": ..., "queue": ..., "deliveries_count": ...}`). Recurring `event=lease_lost` records mean your `lease_ttl_seconds` is below your handler's P99 — raise it. Log-pipeline aggregators can alert on the `event` field directly without regex.

### Slow handlers — dedicated queue

When a handler's tail latency exceeds the subscriber's `lease_ttl_seconds`, the row's lease expires mid-flight and another fetch reclaims it → duplicate delivery. Don't hike `lease_ttl_seconds` globally — that delays reclaim of *actually* stuck rows everywhere. Instead, segregate slow work onto its own subscriber with a longer TTL:

```python
@broker.subscriber("slow_q", lease_ttl_seconds=600)   # 10 minutes
async def heavy_job(msg): ...

@broker.subscriber("fast_q", lease_ttl_seconds=30)
async def quick_job(msg): ...
```

Pick `lease_ttl_seconds` strictly greater than that subscriber's P99 handler duration, with margin for clock skew. The tight TTL on the fast queue keeps stuck-row reclaim fast; the tall TTL on the slow queue tolerates outliers without slowing reclaim of genuinely stuck rows elsewhere. Producers route to the appropriate queue at `publish` time.

## FastAPI integration

The outbox + FastAPI is the canonical use case: handlers receive an `AsyncSession` via FastAPI dependency injection, and the outbox row commits with the caller's domain writes — same transaction.

```bash
pip install faststream-outbox[fastapi]
```

```python
from collections.abc import AsyncIterator

from fastapi import Depends, FastAPI
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from faststream_outbox.fastapi import OutboxBroker, OutboxRouter

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

Mounting the router auto-starts the inner broker via FastAPI lifespan; you do not call `broker.start()`. HTTP routes (`@router.get`, `@router.post`, …) and outbox subscribers coexist on one router. `faststream_outbox.fastapi` re-exports the same Annotated context shortcuts as `faststream_outbox.annotations` (FastAPI-aware, so they cooperate with `Depends(...)`).

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

*In tests using `TestOutboxBroker` (default sync mode), `activate_in` / `activate_at` are ignored and timers fire immediately — see [Testing](#testing).*

## Testing

`TestOutboxBroker` (in `faststream_outbox.testing`) swaps the SQLAlchemy-backed client for an in-memory fake so unit tests don't need Postgres. By default it dispatches handlers **synchronously inside `publish`** — matching `TestKafkaBroker` / `TestRabbitBroker`. No `_wait_until`, no `sleep`.

```python
from faststream_outbox.testing import TestOutboxBroker

async def test_handler() -> None:
    received: list[int] = []

    @broker.subscriber("orders")
    async def handle(order_id: int) -> None:
        received.append(order_id)

    async with TestOutboxBroker(broker):
        await broker.publish(1, queue="orders")
        # Handler has already run.
    assert received == [1]
```

Sync mode ignores `activate_in` / `activate_at` — **timers fire immediately**, so straight-line tests work for scheduled publishes without waiting on wall clock. The schedule is still recorded on the fake row (`broker.fake_client.rows[0].next_attempt_at`) if a test needs to assert on it. `cancel_timer` still works for queues without a registered handler.

For tests that need real polling semantics — retry rescheduling, lease expiry / reclaim, `_fetch_loop` error recovery, or honoring `activate_in` delays — opt in to the loop-driven mode:

```python
async with TestOutboxBroker(broker, run_loops=True):
    ...  # use feed() / _wait_until to drive the real loops
```

## Schema validation

Schema validation is opt-in and delegates to Alembic's autogenerate. **Alembic is an optional dependency** — install it only if you want to call `validate_schema()`. Producers, subscribers, retries, timers, and LISTEN/NOTIFY all work without it.

```bash
pip install faststream-outbox[validate]
```

```python
await broker.validate_schema()  # raises if the live table is missing what the broker needs
```

The validator builds a canonical `Table` from `make_outbox_table` in a throwaway `MetaData`, runs `alembic.autogenerate.compare_metadata` against the live DB scoped to that table, and raises `RuntimeError` listing any **missing** schema — absent table, missing columns, mismatched column types, flipped nullability, missing partial indexes. Extras (your own audit columns, additional indexes for your joins) are intentionally ignored. Calling `validate_schema()` without alembic installed raises `ImportError` with an install hint; not calling it has no effect.

Call it from a `/health` endpoint or startup hook — not at `broker.start()`, so Alembic can run migrations against the same DB without a startup loop.

## Retry strategies

A subscriber with no explicit `retry_strategy` retries on handler exceptions with `ExponentialRetry(initial_delay_seconds=1.0, multiplier=2.0, max_delay_seconds=300.0, max_attempts=10, jitter_factor=0.2)`. An outbox is a reliability primitive — silently dropping a row on the first transient error is the wrong default for one. Pass `NoRetry()` explicitly if you really do want "delete on first error":

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

Strategies receive the raised `exception` so users may subclass for "retry only on transient errors":

```python
class TransientOnly(ExponentialRetry):
    def get_next_attempt_at(self, *, exception=None, **kw):
        if exception and not isinstance(exception, TransientError):
            return None
        return super().get_next_attempt_at(exception=exception, **kw)
```

## Metrics

The broker emits structured events at well-defined instrumentation points via a single callable seam. Plug in either a one-line lambda or one of the bundled adapters for Prometheus / OpenTelemetry.

### Bare seam

```python
from faststream_outbox import MetricsRecorder, OutboxBroker

def recorder(event: str, tags: dict) -> None:
    # event ∈ {fetched, dispatched, acked, nacked_retried, nacked_terminal, lease_lost, published}
    # tags always include "queue"; subscriber-side events also include "subscriber"
    print(event, tags)

broker = OutboxBroker(engine, outbox_table=outbox_table, metrics_recorder=recorder)
```

The recorder is called from the event loop and **must not block**. Synchronous `prometheus_client.Counter.inc()` is fine (microseconds); a blocking HTTP / StatsD call is not. The library does not wrap recorders in `asyncio.to_thread` — that would destroy ordering and explode the task graph.

### Prometheus (drop-in compatible with FastStream's `PrometheusMiddleware`)

```bash
pip install faststream-outbox[prometheus]
```

```python
from fastapi import FastAPI
from prometheus_client import REGISTRY, make_asgi_app
from faststream_outbox import OutboxBroker
from faststream_outbox.metrics.prometheus import PrometheusRecorder

recorder = PrometheusRecorder(app_name="checkout", registry=REGISTRY)
broker = OutboxBroker(engine, outbox_table=outbox_table, metrics_recorder=recorder)

app = FastAPI()
app.mount("/metrics", make_asgi_app(registry=REGISTRY))
```

Metric names, label set (`app_name, broker, handler, *custom_labels`), status enum (`acked, nacked, error`), histogram buckets, and constructor args (`registry, app_name=EMPTY, metrics_prefix, received_messages_size_buckets, custom_labels`) all mirror `faststream.prometheus.PrometheusMiddleware`. The `broker` label is always `"outbox"`; existing FastStream Grafana dashboards keep working — add `broker="outbox"` to the PromQL filter.

```promql
# Handler throughput (acked / sec)
rate(faststream_received_processed_messages_total{broker="outbox",status="acked"}[1m])

# Handler error rate
rate(faststream_received_processed_messages_total{broker="outbox",status!="acked"}[5m])
  /
rate(faststream_received_processed_messages_total{broker="outbox"}[5m])

# P99 handler latency
histogram_quantile(0.99,
  rate(faststream_received_processed_messages_duration_seconds_bucket{broker="outbox"}[5m]))

# In-flight gauge
faststream_received_messages_in_process{broker="outbox"}

# Operator playbook: lease_ttl_seconds is too low for this handler's P99
rate(faststream_outbox_lease_lost_total[5m]) > 0

# Publish throughput per queue (publish-side tags by `destination`, not `handler`)
rate(faststream_published_messages_total{broker="outbox",status="success"}[1m])

# P99 publish (INSERT) latency per queue
histogram_quantile(0.99,
  rate(faststream_published_messages_duration_seconds_bucket{broker="outbox"}[5m]))
```

### OpenTelemetry (drop-in compatible with FastStream's `TelemetryMiddleware`, meter only)

```bash
pip install faststream-outbox[opentelemetry]
```

```python
from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from prometheus_client import start_http_server

reader = PrometheusMetricReader()
metrics.set_meter_provider(MeterProvider(metric_readers=[reader]))
start_http_server(9000)

from faststream_outbox import OutboxBroker
from faststream_outbox.metrics.opentelemetry import OpenTelemetryRecorder

broker = OutboxBroker(
    engine,
    outbox_table=outbox_table,
    metrics_recorder=OpenTelemetryRecorder(),
)
```

Instrument names (`messaging.process.duration`, `messaging.publish.duration`, `messaging.process.messages` when `include_messages_counters=True`), units, and constructor args (`meter_provider`, `meter`, `include_messages_counters`) match `faststream.opentelemetry.TelemetryMiddleware`. The `messaging.system="outbox"` attribute disambiguates outbox traffic from Kafka/Rabbit data on the same instruments. Tracing (spans) is **not** modelled by this adapter — the callable seam can't bracket a span lifecycle. **For spans, use the native middleware integration below.**

### Native middleware integration (spans + middleware-bus parity)

For OTel spans wrapping `consume_scope` / `publish_scope` and the exact upstream label/instrument schema, register the native middleware subclasses via `broker_middlewares=[...]` — same registration pattern as `KafkaPrometheusMiddleware` / `RabbitTelemetryMiddleware`:

```python
from prometheus_client import REGISTRY
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.metrics import MeterProvider

from faststream_outbox import OutboxBroker
from faststream_outbox.prometheus import OutboxPrometheusMiddleware
from faststream_outbox.opentelemetry import OutboxTelemetryMiddleware

broker = OutboxBroker(
    engine,
    outbox_table=outbox_table,
    middlewares=[
        OutboxPrometheusMiddleware(registry=REGISTRY),
        OutboxTelemetryMiddleware(
            tracer_provider=TracerProvider(),
            meter_provider=MeterProvider(),
        ),
    ],
)
```

#### Layering: middleware seam vs. recorder seam

Both can be registered together — each fires for events the other physically cannot observe.

| Concern | Middleware seam | Recorder seam |
|---|---|---|
| Handler duration / status / size | ✅ via `consume_scope` | ✅ via `acked` / `nacked_*` events |
| Publish duration / status / exception | ✅ via `publish_scope` | ✅ via `published` event |
| Span tracing (consume + publish) | ✅ | ❌ (callable can't bracket spans) |
| `fetched` ticks (including empty) | ❌ (no `StreamMessage` at fetch time) | ✅ |
| `lease_lost` after `consume_scope` exits | ❌ | ✅ |
| `nacked_terminal(reason="max_deliveries")` before consume opens | ❌ | ✅ |
| Empty-fetch idle counter | ❌ | ✅ |

The recommended setup for full observability is **both seams together**: middleware for bus-scope metrics + tracing, recorder for outbox-internal events.

#### Test broker note

`TestOutboxBroker` patches `broker.publish` directly, bypassing `_basic_publish` — so middleware-registered **publish-scope** metrics do not fire in test mode. Middleware **consume-scope** metrics still fire (because `dispatch_one` calls `self.consume()` which walks the middleware stack normally). The recorder-seam `published` event provides synthetic publish-side coverage in test mode via `FakeOutboxProducer`. Mirrors `TestKafkaBroker` / `TestRabbitBroker` — same posture for the same reason.

## Failure modes

- **Handlers must be idempotent.** Crash between commit-of-handler-side-effects and the broker's `DELETE` re-delivers the message.
- **Best-effort ordering only.** `FOR UPDATE SKIP LOCKED` does not preserve strict order under concurrent workers. If you need strict per-aggregate ordering, route to a single subscriber and run a single worker.
- **No DLQ / archive.** Terminal failures `DELETE` the row.

## Connection ownership

`OutboxBroker` does **not** close the `AsyncEngine` you pass in — the caller owns its lifecycle.

## Tuning

Per-subscriber knobs (passed to `@broker.subscriber("…", …)`):

- `max_workers` (default `1`) — concurrent handlers per subscriber.
- `fetch_batch_size` (default `10`) — rows claimed per fetch cycle.
- `min_fetch_interval` / `max_fetch_interval` (default `1.0` / `10.0` s) — base + ceiling for the adaptive idle backoff with jitter.
- `lease_ttl_seconds` (default `60.0` s) — how long a claim is valid before another fetch may reclaim it. **Must exceed your handler's P99 duration with margin.** Expired-lease reclaim is covered by a dedicated partial index (`<table>_lease_idx`), so sustained lease loss degrades fetch latency by index-update cost on each claim, not by a seq-scan tail proportional to table size.
- `max_deliveries` (default `None` — unbounded) — total claims (including lease-expiry re-claims) after which the row is dropped without invoking the handler. Defends against handlers that consistently wedge.
- `ack_policy` (default `NACK_ON_ERROR`) — accepts `AckPolicy.NACK_ON_ERROR`, `REJECT_ON_ERROR`, or `MANUAL`. `NACK_ON_ERROR` (default) consults the retry strategy on handler exceptions. `REJECT_ON_ERROR` deletes on the first failure (the retry strategy is ignored). `MANUAL` requires the handler to call `await msg.ack()` / `nack()` / `reject()` itself. `AckPolicy.ACK_FIRST` is **not supported** — it deletes the row before the handler runs, so a handler crash silently drops the message, defeating the outbox reliability guarantee. Passing it raises `ValueError` at registration. The factory also warns or raises on other likely-wrong combinations (e.g. `lease_ttl_seconds <= max_fetch_interval`, `max_deliveries` without retry, `min_fetch_interval > max_fetch_interval`).

`ConstantRetry` and `LinearRetry` also accept `jitter_factor` (default `0.0`); when non-zero, the computed delay is multiplied by `1 + U(-jitter_factor/2, +jitter_factor/2)` to spread out retries, matching `ExponentialRetry`'s shape.

**Engine pool sizing.** Each subscriber holds `max_workers + 1` long-lived SQLAlchemy connections (one writer per worker + one fetch), plus one raw asyncpg connection for `LISTEN` when available. Size your engine for `Σ subscribers × (max_workers + 1)` or `broker.start()` will block on pool checkout. SQLAlchemy's default `pool_size=5, max_overflow=10` covers a handful of single-worker subscribers; raise it for larger fleets.

That formula is **per process**. Each replica opens its own pool, so your Postgres `max_connections` needs to cover `replicas × Σ subscribers × (max_workers + 1)` — otherwise additional replicas (or rolling deployments) will be refused at startup with `FATAL: too many connections`.

**Read-only inspection.** `subscriber.get_one()` and `async for msg in subscriber:` are not supported on `OutboxSubscriber` — they would acquire a lease and bump `deliveries_count`, surprising semantics for a peek API. Use `broker.fetch_unprocessed(session=..., queue=...)` for lease-free reads of the current table state.

## Acknowledgements

The architecture of this package is heavily informed by Arseniy Popov's [PR #2704](https://github.com/ag2ai/faststream/pull/2704) (`feat: add sqla broker`) on upstream FastStream — the FastStream broker/registrator/subscriber wiring, the `SELECT … FOR UPDATE SKIP LOCKED` fetch-and-claim CTE, the retry strategy hierarchy, and the in-transaction publish contract all originate from there. This package is a Postgres-only reimplementation that diverges in storage model (lease tokens instead of an explicit state column, no archive table), loop structure (two loops instead of four), wake-up mechanism (`LISTEN/NOTIFY`), and adds timer mechanics. Credit for the original design belongs to Arseniy.

## Part of `modern-python`

Browse the full list of templates and libraries in
[`modern-python`](https://github.com/modern-python) — see the org profile for the
categorized index.

## 📝 [License](LICENSE)
