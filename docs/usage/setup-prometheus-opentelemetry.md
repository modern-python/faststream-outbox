# Setup Prometheus and OpenTelemetry

This page is the recipe for wiring metrics. For the *why
two instrumentation seams*, see [Concepts § Instrumentation
seams](../concepts/instrumentation-seams.md); for the event catalog
and operator PromQL playbook, see [Reference §
Observability](./observability.md).

## Prometheus adapter

Metric names, label names, status enum, histogram buckets, and
constructor args mirror FastStream's `PrometheusMiddleware`. One label
value differs: `handler` is the subscriber function's name, where the
middleware uses the queue name.

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
liveness probe, handy for Kubernetes.

The `broker` label is always `"outbox"`; add `broker="outbox"` to the
PromQL filter to scope dashboards to outbox traffic.

### Consume vs publish label set

The adapter uses a different label set for consume vs publish,
matching upstream verbatim:

- Consume tags by `handler` (the subscriber)
- Publish tags by `destination` (the queue)

See [Observability § PromQL playbook](./observability.md) for the
operator query catalog.

## OpenTelemetry adapter

Instrument names, units and constructor args follow FastStream's
`TelemetryMiddleware`, but it records meters only, no spans (use the [native middleware](#native-middleware-spans--bus-parity)
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

Instrument names match `faststream.opentelemetry.TelemetryMiddleware` for
the bus-scope metrics: `messaging.process.duration`,
`messaging.publish.duration`, and (when `include_messages_counters=True`)
`messaging.process.messages` / `messaging.publish.messages`. The adapter
adds four outbox-specific counters the middleware can't emit:
`messaging.outbox.fetch.batches`, `messaging.outbox.lease_lost`,
`messaging.outbox.dlq_written`, and `messaging.outbox.drain_timeout`. Units
and constructor args
(`meter_provider`, `meter`, `include_messages_counters`) follow upstream.
When `OutboxTelemetryMiddleware` also records meters, pass
`include_bus_metrics=False` so the `messaging.process.*` /
`messaging.publish.*` instruments are recorded once, by the middleware.
The `messaging.system="outbox"` attribute disambiguates outbox traffic
from Kafka / Rabbit data on the same instruments.

This adapter does not model tracing (spans), because the callable
seam can't bracket a span lifecycle. For spans, use the [native
middleware](#native-middleware-spans--bus-parity) integration below.

## Native middleware (spans + bus parity) { #native-middleware-spans--bus-parity }

For OTel spans wrapping `consume_scope` / `publish_scope` and the
exact upstream label / instrument schema, register the native
middleware subclasses via `middlewares=[...]`, using the same
registration pattern as `KafkaPrometheusMiddleware` /
`RabbitTelemetryMiddleware`.

## Both seams together { #both-seams-together }

The recommended setup pairs middleware with the recorder so every
event the bus emits and every outbox-internal event lands in one
observability stack:

```bash
pip install 'faststream-outbox[opentelemetry,prometheus]' \
    opentelemetry-exporter-otlp uvicorn
```

Here OpenTelemetry supplies spans (exported to OTLP) and Prometheus
supplies all metrics (one registry scraped over HTTP). The OTel
middleware runs span-only: it gets no `meter_provider`, so its meters go to
the global OpenTelemetry meter provider, which is a no-op unless you set one.
Neither endpoint below exposes them.

```python
# app.py — run with `OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
#                    uvicorn app:app --host 0.0.0.0 --port 8000`
from faststream.asgi import AsgiFastStream
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
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

REGISTRY = CollectorRegistry()

# ----- Outbox broker -----
metadata = MetaData()
outbox_table = make_outbox_table(metadata, table_name="outbox")
engine = create_async_engine("postgresql+asyncpg://outbox:outbox@localhost:5432/outbox")

broker = OutboxBroker(
    engine,
    outbox_table=outbox_table,
    middlewares=[
        # Bus-scope spans around consume_scope / publish_scope (span-only; see note above).
        OutboxTelemetryMiddleware(tracer_provider=tracer_provider),
        OutboxPrometheusMiddleware(registry=REGISTRY, app_name="my-outbox-service"),
    ],
    # Outbox-internal events (fetched, lease_lost, terminal reasons, dlq_written)
    # that have no message context and can't reach the middleware bus.
    metrics_recorder=PrometheusRecorder(
        registry=REGISTRY,
        app_name="my-outbox-service",
        include_bus_metrics=False,
    ),
)


@broker.subscriber("orders", max_workers=4)
async def handle_order(body: dict) -> None: ...


app = AsgiFastStream(broker, asgi_routes=[("/metrics", make_asgi_app(registry=REGISTRY))])
```

Traces flow to OTLP (Jaeger / Tempo / Honeycomb / collector), and every
metric lands on `/metrics`. The OTel middleware here contributes spans
only, per the note above.

The two seams overlap on the `faststream_received_*` /
`faststream_published_*` families. `include_bus_metrics=False` leaves
those to the middleware, so each consume and publish is counted once and
the recorder adds only the outbox-internal series the middleware can't
see (`fetched`, `lease_lost`, terminal reasons, `dlq_written`). Without
the flag, both record the same families. On one registry, creating the
middleware first makes the recorder raise `Duplicated timeseries in
CollectorRegistry`, while creating the recorder first makes the
middleware reuse its collectors and count every consume and publish
twice.

The providers set `messaging.system = "outbox"`, matching the
recorder-seam adapters. On consume spans the OTel provider maps `row.id →
messaging.message.id`, `row.queue → messaging.destination_publish.name`,
`correlation_id → messaging.message.conversation_id`, and `len(payload) →
messaging.message.payload_size_bytes`. On publish spans it maps the target
`queue → messaging.destination.name`, `correlation_id →
messaging.message.conversation_id`, and `len(cmd.batch_bodies) →
messaging.batch.message_count` when >1.
