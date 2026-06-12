# Setup Prometheus and OpenTelemetry

You've decided to wire metrics. This page is the recipe. For the *why
two instrumentation seams*, see [Concepts § Instrumentation
seams](../concepts/instrumentation-seams.md); for the event catalog
and operator PromQL playbook, see [Reference §
Observability](./observability.md).

## Prometheus adapter

Drop-in compatible with FastStream's `PrometheusMiddleware`. Metric
names, label set, status enum, histogram buckets, and constructor args
all mirror upstream.

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
`make_asgi_app(REGISTRY)` to expose Prometheus exposition without
pulling FastAPI in. `make_ping_asgi(broker)` is FastStream's built-in
liveness probe — handy for Kubernetes.

The `broker` label is always `"outbox"`; existing FastStream Grafana
dashboards keep working — add `broker="outbox"` to the PromQL filter.

### Consume vs publish label set

The adapter uses a different label set for consume vs publish,
matching upstream verbatim:

- Consume tags by `handler` (the subscriber)
- Publish tags by `destination` (the queue)

See [Observability § PromQL playbook](./observability.md) for the
operator query catalog.

## OpenTelemetry adapter

Drop-in compatible with FastStream's `TelemetryMiddleware`, **meter
only — no spans** (use the [native middleware](#native-middleware-spans--bus-parity)
section below if you need spans).

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

The `PrometheusMetricReader` converts OTel meter data points to
Prometheus exposition format on `/metrics`; for OTLP push instead,
swap the reader for `PeriodicExportingMetricReader(OTLPMetricExporter(...))`
and drop the `/metrics` route.

Instrument names (`messaging.process.duration`,
`messaging.publish.duration`, `messaging.process.messages` when
`include_messages_counters=True`), units, and constructor args
(`meter_provider`, `meter`, `include_messages_counters`) match
`faststream.opentelemetry.TelemetryMiddleware`. The
`messaging.system="outbox"` attribute disambiguates outbox traffic
from Kafka / Rabbit data on the same instruments.

**Tracing (spans) is not modelled by this adapter** — the callable
seam can't bracket a span lifecycle. For spans, use the [native
middleware](#native-middleware-spans--bus-parity) integration below.

## Native middleware (spans + bus parity) { #native-middleware-spans--bus-parity }

For OTel spans wrapping `consume_scope` / `publish_scope` and the
exact upstream label / instrument schema, register the native
middleware subclasses via `broker_middlewares=[...]` — same
registration pattern as `KafkaPrometheusMiddleware` /
`RabbitTelemetryMiddleware`.

## Both seams together { #both-seams-together }

The recommended setup pairs middleware with the recorder so every
event the bus emits **and** every outbox-internal event lands in one
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
from prometheus_client import CollectorRegistry, make_asgi_app
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

# Two registries: the middleware and the recorder both define the same
# faststream_* consume/publish collectors, so sharing one registry raises
# "Duplicated timeseries in CollectorRegistry" at broker construction.
MIDDLEWARE_REGISTRY = CollectorRegistry()
RECORDER_REGISTRY = CollectorRegistry()

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
        OutboxPrometheusMiddleware(registry=MIDDLEWARE_REGISTRY, app_name="my-outbox-service"),
    ],
    # Outbox-internal events (fetched, lease_lost, terminal reasons, dlq_written)
    # that have no message context and can't reach the middleware bus.
    metrics_recorder=PrometheusRecorder(registry=RECORDER_REGISTRY, app_name="my-outbox-service"),
)


@broker.subscriber("orders", max_workers=4)
async def handle_order(body: dict) -> None: ...


app = AsgiFastStream(
    broker,
    asgi_routes=[
        ("/metrics", make_asgi_app(registry=MIDDLEWARE_REGISTRY)),
        ("/metrics/outbox", make_asgi_app(registry=RECORDER_REGISTRY)),
    ],
)
```

Traces flow to OTLP (Jaeger / Tempo / Honeycomb / collector); the
middleware's meters land on `/metrics` and the recorder's outbox-internal
counters on `/metrics/outbox` for Prometheus to scrape — two scrape targets,
one process.

**The two seams overlap on consume/publish series.** Both the middleware
and the recorder emit the same `faststream_received_*` / `faststream_published_*`
collectors, which is why they must live on **separate registries** (above) —
sharing one raises `Duplicated timeseries in CollectorRegistry` at broker
construction, and summing across both double-counts every consume and
publish. Treat the middleware as the source of truth for consume/publish;
the recorder's unique value is the outbox-internal events the middleware
can't see (`fetched`, `lease_lost`, terminal reasons, `dlq_written`).

The providers set `messaging.system = "outbox"`, matching the
recorder-seam adapters. The OTel provider maps `row.id →
messaging.message.id`, `row.queue → messaging.destination_publish.name`,
`correlation_id → messaging.message.conversation_id`, `len(payload) →
messaging.message.payload_size_bytes`, and `len(cmd.batch_bodies) →
messaging.batch.message_count` when >1.
