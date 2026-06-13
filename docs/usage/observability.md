# Observability

*Setting it up: [Setup Prometheus and OpenTelemetry](./setup-prometheus-opentelemetry.md).
Why two seams: [Concepts § Instrumentation seams](../concepts/instrumentation-seams.md).*

This page is the **Reference**: the recorder-seam API, the event
catalog, and the operator PromQL playbook.

## The recorder seam

`OutboxBroker(..., metrics_recorder=...)` accepts a callable:

```python
MetricsRecorder = Callable[[str, Mapping[str, Any]], None]
```

The default (`_noop_recorder`) lets instrumentation sites call
unconditionally. The recorder threads through `OutboxBrokerConfig` to:

- The subscriber's seven emission points via `OutboxSubscriber._emit_metric`
- The producer's single emission point via `OutboxProducer._emit_metric`

### Bare seam

```python
from faststream_outbox import MetricsRecorder, OutboxBroker


def recorder(event: str, tags: dict) -> None:
    # event ∈ {fetched, dispatched, acked, nacked_retried, nacked_terminal,
    #          lease_lost, dlq_written, published}
    # tags always include "queue"; subscriber-side events also include "subscriber"
    print(event, tags)


broker = OutboxBroker(engine, outbox_table=outbox_table, metrics_recorder=recorder)
```

### Bundled adapters

You rarely need to hand-write the callable. Two ready-made recorders ship
as optional extras and emit the `faststream_outbox_*` series the PromQL
playbook below keys off:

```python
from prometheus_client import CollectorRegistry
from faststream_outbox.metrics.prometheus import PrometheusRecorder

registry = CollectorRegistry()
broker = OutboxBroker(
    engine,
    outbox_table=outbox_table,
    metrics_recorder=PrometheusRecorder(app_name="checkout", registry=registry),
)
```

`OpenTelemetryRecorder` (`faststream_outbox.metrics.opentelemetry`) is the
OTel equivalent. Full wiring — including running the recorder seam and the
native middleware together — is in
[Setup Prometheus and OpenTelemetry](./setup-prometheus-opentelemetry.md).

### Recorder must not block

The recorder is called from the event loop. **Do not block in it.**
Synchronous `prometheus_client.Counter.inc()` is fine (microseconds); a
blocking HTTP / StatsD call is not. The library does not wrap recorders in
`asyncio.to_thread` — that would destroy ordering and explode the task
graph.

Every call site wraps the recorder in `try/except` and logs at DEBUG, so a
broken recorder never poisons the dispatch loop.

## Event catalog

| Event | Tags (always present) | Tags (situational) | Fired by |
|---|---|---|---|
| `fetched` | `queue`, `subscriber`, `count` | | Fetch loop, once per fetch attempt (`count=0` on an empty fetch) — **skipped** when the in-flight queue is full (no fetch is issued). `queue` is tagged with the subscriber's **first** queue only; multi-queue subscribers should break down by queue using the row-level events instead |
| `dispatched` | `queue`, `subscriber`, `deliveries_count`, `size_bytes` | | Worker loop, before handler runs |
| `acked` | `queue`, `subscriber`, `deliveries_count`, `duration_seconds` | | Handler returned successfully |
| `nacked_retried` | `queue`, `subscriber`, `deliveries_count`, `duration_seconds`, `next_delay_seconds` | `exception_type` | Retry scheduled |
| `nacked_terminal` | `queue`, `subscriber`, `deliveries_count`, `reason` | `duration_seconds`, `exception_type` | Row terminally failed (`duration_seconds` absent for `max_deliveries`, which never ran the handler) |
| `lease_lost` | `queue`, `subscriber`, `phase`, `row_id`, `deliveries_count` | | Terminal or retry write found `rowcount == 0` (`phase` = `terminal` \| `retry`) |
| `published` | `queue`, `status`, `count`, `size_bytes`, `duration_seconds` | `exception_type` | Producer, after the INSERT executes (pre-commit; also fires on error with `status="error"`) |
| `dlq_written` | `queue`, `subscriber`, `deliveries_count`, `failure_reason` | `exception_type` | DLQ CTE wrote an audit row. `exception_type` is **omitted** — not set to `None` — when the terminal had no exception (`max_deliveries`, or a manual `reject()` without one) |

`reason` on `nacked_terminal` is one of `max_deliveries`,
`retry_terminal`, `rejected`. The same value lands in the DLQ
`failure_reason` column when the DLQ is configured.

## PromQL playbook

Operator queries that key off the recorder-side metrics. The
`faststream_outbox_*` series below (`_lease_lost_total`,
`_terminal_total`, `_dlq_written_total`) are emitted by
**`PrometheusRecorder`** (`faststream_outbox.metrics.prometheus`), wired
via `metrics_recorder=…` — see [Setup](./setup-prometheus-opentelemetry.md);
the native `OutboxPrometheusMiddleware` does **not** emit them. The
`broker` label is always `"outbox"`; add the filter to disambiguate from
upstream FastStream services.

```promql
# Handler throughput (acked / sec)
rate(faststream_received_processed_messages_total{broker="outbox",status="acked"}[1m])

# Handler error rate. NB: status="error" also counts lease losses (the
# recorder maps lease_lost onto the error status), so this includes an
# operational, not handler, failure mode. To isolate handler failures use
# status="nacked"; track lease loss separately via the query below.
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

# Publish throughput per queue (publish metrics are tagged by `destination`)
sum by (destination) (
  rate(faststream_published_messages_total{broker="outbox",status="success"}[1m]))

# P99 publish (INSERT) latency per queue
histogram_quantile(0.99,
  sum by (destination, le) (
    rate(faststream_published_messages_duration_seconds_bucket{broker="outbox"}[5m])))

# DLQ misconfiguration: terminal-failure rate diverges from DLQ-write rate
rate(faststream_outbox_terminal_total[5m])
  -
rate(faststream_outbox_dlq_written_total[5m])
  > 0
```

The first seven are direct ports of the recorder-side metrics into
operator-actionable PromQL. The last one is the
DLQ-misconfiguration-detection alert covered in [DLQ § Metric:
dlq_written](./dlq.md#metric-dlq_written).

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
