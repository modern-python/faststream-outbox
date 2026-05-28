# Observability

The broker exposes **two complementary instrumentation seams**:

1. **Recorder seam** — a single callable invoked at six subscriber events
   (`fetched`, `dispatched`, `acked`, `nacked_retried`, `nacked_terminal`,
   `lease_lost`) and one producer event (`published`). Owns outbox-internal
   events that the FastStream middleware bus physically cannot observe.
2. **Native middleware** — subclasses of upstream FastStream's
   `TelemetryMiddleware` and `PrometheusMiddleware` plug into
   `consume_scope` / `publish_scope` for spans, durations, status, and
   message size — matching upstream Kafka / Rabbit middlewares exactly.

You can use either, both, or neither. The recommended setup for full
observability is **both seams together**: middleware owns bus-scope
metrics + tracing, recorder owns outbox-internal events.

## The recorder seam

`OutboxBroker(..., metrics_recorder=...)` accepts a callable:

```python
MetricsRecorder = Callable[[str, Mapping[str, Any]], None]
```

The default (`_noop_recorder`) lets instrumentation sites call
unconditionally. The recorder threads through `OutboxBrokerConfig` to:

- The subscriber's six emission points via `OutboxSubscriber._emit_metric`
- The producer's single emission point via `OutboxProducer._emit_metric`

### Bare seam

```python
from faststream_outbox import MetricsRecorder, OutboxBroker


def recorder(event: str, tags: dict) -> None:
    # event ∈ {fetched, dispatched, acked, nacked_retried, nacked_terminal,
    #          lease_lost, published}
    # tags always include "queue"; subscriber-side events also include "subscriber"
    print(event, tags)


broker = OutboxBroker(engine, outbox_table=outbox_table, metrics_recorder=recorder)
```

### Recorder must not block

The recorder is called from the event loop. **Do not block in it.**
Synchronous `prometheus_client.Counter.inc()` is fine (microseconds); a
blocking HTTP / StatsD call is not. The library does not wrap recorders in
`asyncio.to_thread` — that would destroy ordering and explode the task
graph.

Every call site wraps the recorder in `try/except` and logs at DEBUG, so a
broken recorder never poisons the dispatch loop.

## Prometheus adapter

Drop-in compatible with FastStream's `PrometheusMiddleware`. Metric names,
label set, status enum, histogram buckets, and constructor args all mirror
upstream.

```bash
pip install 'faststream-outbox[prometheus]' uvicorn
```

```python
# app.py — run with `uvicorn app:app --host 0.0.0.0 --port 8000`
from faststream.asgi import AsgiFastStream, make_ping_asgi
from prometheus_client import REGISTRY, make_asgi_app
from sqlalchemy import MetaData
from sqlalchemy.ext.asyncio import create_async_engine

from faststream_outbox import OutboxBroker, make_outbox_table
from faststream_outbox.metrics.prometheus import PrometheusRecorder


metadata = MetaData()
outbox_table = make_outbox_table(metadata, table_name="outbox")
engine = create_async_engine("postgresql+asyncpg://outbox:outbox@localhost:5432/outbox")

broker = OutboxBroker(
    engine,
    outbox_table=outbox_table,
    metrics_recorder=PrometheusRecorder(app_name="checkout", registry=REGISTRY),
)


@broker.subscriber("orders", max_workers=4)
async def handle_order(body: dict) -> None: ...


app = AsgiFastStream(
    broker,
    asgi_routes=[
        ("/metrics", make_asgi_app(registry=REGISTRY)),
        ("/healthz", make_ping_asgi(broker, timeout=2.0)),
    ],
)
```

`AsgiFastStream` accepts any ASGI sub-app under `asgi_routes`; mount
`make_asgi_app(REGISTRY)` to expose Prometheus exposition without pulling
FastAPI in. `make_ping_asgi(broker)` is FastStream's built-in liveness
probe — handy for Kubernetes.

The `broker` label is always `"outbox"`; existing FastStream Grafana
dashboards keep working — add `broker="outbox"` to the PromQL filter.

### Consume vs publish label set

The adapter uses a different label set for consume vs publish, matching
upstream verbatim:

- Consume tags by `handler` (the subscriber)
- Publish tags by `destination` (the queue)

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

# Publish throughput per queue
rate(faststream_published_messages_total{broker="outbox",status="success"}[1m])

# P99 publish (INSERT) latency per queue
histogram_quantile(0.99,
  rate(faststream_published_messages_duration_seconds_bucket{broker="outbox"}[5m]))
```

## OpenTelemetry adapter

Drop-in compatible with FastStream's `TelemetryMiddleware`, **meter only
— no spans** (see [Native middleware](#native-middleware-spans-bus-parity)
below if you need spans).

```bash
pip install 'faststream-outbox[opentelemetry,prometheus]' \
    opentelemetry-exporter-prometheus uvicorn
```

```python
# app.py — run with `uvicorn app:app --host 0.0.0.0 --port 8000`
from faststream.asgi import AsgiFastStream
from opentelemetry import metrics
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.sdk.metrics import MeterProvider
from prometheus_client import REGISTRY, make_asgi_app
from sqlalchemy import MetaData
from sqlalchemy.ext.asyncio import create_async_engine

from faststream_outbox import OutboxBroker, make_outbox_table
from faststream_outbox.metrics.opentelemetry import OpenTelemetryRecorder


# OTel meters → Prometheus reader (scraped at /metrics below)
prometheus_reader = PrometheusMetricReader()
meter_provider = MeterProvider(metric_readers=[prometheus_reader])
metrics.set_meter_provider(meter_provider)

metadata = MetaData()
outbox_table = make_outbox_table(metadata, table_name="outbox")
engine = create_async_engine("postgresql+asyncpg://outbox:outbox@localhost:5432/outbox")

broker = OutboxBroker(
    engine,
    outbox_table=outbox_table,
    metrics_recorder=OpenTelemetryRecorder(meter_provider=meter_provider),
)


@broker.subscriber("orders", max_workers=4)
async def handle_order(body: dict) -> None: ...


app = AsgiFastStream(broker, asgi_routes=[("/metrics", make_asgi_app(registry=REGISTRY))])
```

The `PrometheusMetricReader` converts OTel meter data points to Prometheus
exposition format on `/metrics`; for OTLP push instead, swap the reader
for `PeriodicExportingMetricReader(OTLPMetricExporter(...))` and drop the
`/metrics` route.

Instrument names (`messaging.process.duration`,
`messaging.publish.duration`, `messaging.process.messages` when
`include_messages_counters=True`), units, and constructor args
(`meter_provider`, `meter`, `include_messages_counters`) match
`faststream.opentelemetry.TelemetryMiddleware`. The
`messaging.system="outbox"` attribute disambiguates outbox traffic from
Kafka / Rabbit data on the same instruments.

**Tracing (spans) is not modelled by this adapter** — the callable seam
can't bracket a span lifecycle. For spans, use the [native middleware
integration](#native-middleware-spans-bus-parity) below.

## Native middleware (spans + bus parity)

For OTel spans wrapping `consume_scope` / `publish_scope` and the exact
upstream label / instrument schema, register the native middleware
subclasses via `broker_middlewares=[...]` — same registration pattern as
`KafkaPrometheusMiddleware` / `RabbitTelemetryMiddleware`.

The recommended setup pairs middleware with the recorder so every event
the bus emits **and** every outbox-internal event lands in one
observability stack:

```bash
pip install 'faststream-outbox[opentelemetry,prometheus]' \
    opentelemetry-exporter-otlp opentelemetry-exporter-prometheus uvicorn
```

```python
# app.py — run with `OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
#                    uvicorn app:app --host 0.0.0.0 --port 8000`
from faststream.asgi import AsgiFastStream
from opentelemetry import metrics, trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from prometheus_client import REGISTRY, make_asgi_app
from sqlalchemy import MetaData
from sqlalchemy.ext.asyncio import create_async_engine

from faststream_outbox import OutboxBroker, make_outbox_table
from faststream_outbox.metrics.prometheus import PrometheusRecorder
from faststream_outbox.opentelemetry import OutboxTelemetryMiddleware
from faststream_outbox.prometheus import OutboxPrometheusMiddleware


# ----- OTel SDK -----
resource = Resource.create({"service.name": "my-outbox-service"})
tracer_provider = TracerProvider(resource=resource)
tracer_provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
trace.set_tracer_provider(tracer_provider)

meter_provider = MeterProvider(resource=resource, metric_readers=[PrometheusMetricReader()])
metrics.set_meter_provider(meter_provider)

# ----- Outbox broker -----
metadata = MetaData()
outbox_table = make_outbox_table(metadata, table_name="outbox")
engine = create_async_engine("postgresql+asyncpg://outbox:outbox@localhost:5432/outbox")

broker = OutboxBroker(
    engine,
    outbox_table=outbox_table,
    middlewares=[
        # Bus-scope spans + meters around consume_scope / publish_scope.
        OutboxTelemetryMiddleware(tracer_provider=tracer_provider, meter_provider=meter_provider),
        OutboxPrometheusMiddleware(registry=REGISTRY, app_name="my-outbox-service"),
    ],
    # Outbox-internal events (fetched, lease_lost, terminal reasons) that have
    # no message context and can't reach the middleware bus.
    metrics_recorder=PrometheusRecorder(registry=REGISTRY, app_name="my-outbox-service"),
)


@broker.subscriber("orders", max_workers=4)
async def handle_order(body: dict) -> None: ...


app = AsgiFastStream(broker, asgi_routes=[("/metrics", make_asgi_app(registry=REGISTRY))])
```

Traces flow to OTLP (Jaeger / Tempo / Honeycomb / collector); meters and
the recorder's outbox-internal counters land on `/metrics` for Prometheus
to scrape. One process, one ASGI app, one scrape endpoint.

The providers set `messaging.system = "outbox"`, matching the recorder-seam
adapters. The OTel provider maps `row.id → messaging.message.id`,
`row.queue → messaging.destination_publish.name`, `correlation_id →
messaging.message.conversation_id`, `len(payload) →
messaging.message.payload_size_bytes`, and `len(cmd.batch_bodies) →
messaging.batch.message_count` when >1.

## Layering: middleware seam vs. recorder seam

Both can be registered together — each fires for events the other
physically cannot observe.

| Concern | Middleware seam | Recorder seam |
|---|---|---|
| Handler duration / status / size | ✅ via `consume_scope` | ✅ via `acked` / `nacked_*` events |
| Publish duration / status / exception | ✅ via `publish_scope` | ✅ via `published` event |
| Span tracing (consume + publish) | ✅ | ❌ (callable can't bracket spans) |
| `fetched` ticks (including empty) | ❌ (no `StreamMessage` at fetch time) | ✅ |
| `lease_lost` after `consume_scope` exits | ❌ | ✅ |
| `nacked_terminal(reason="max_deliveries")` before consume opens | ❌ | ✅ |
| Empty-fetch idle counter | ❌ | ✅ |

The recommended setup for full observability is **both seams together**:
middleware for bus-scope metrics + tracing, recorder for outbox-internal
events.

## Test broker note

`TestOutboxBroker` patches `broker.publish` directly via
`mock.patch.object`, bypassing `_basic_publish` — so middleware-registered
**publish-scope** metrics do **not** fire in test mode. Middleware
**consume-scope** metrics still fire (because `dispatch_one` calls
`self.consume()` which walks the middleware stack normally).

The recorder-seam `published` event provides synthetic publish-side
coverage in test mode via `FakeOutboxProducer`. The synthetic events use
`duration_seconds=0.0` since the in-memory client has no real write to
time.

Mirrors `TestKafkaBroker` / `TestRabbitBroker` — same posture, same reason.
