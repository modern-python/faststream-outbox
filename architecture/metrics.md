# Metrics + native middleware — implementation detail

User-facing: `docs/usage/observability.md`. Invariant summary: `CLAUDE.md` § Metrics seam / Native middleware.

## Recorder seam (`metrics/__init__.py`)

`OutboxBroker(..., metrics_recorder=...)` accepts a `MetricsRecorder = Callable[[str, Mapping[str, Any]], None]`. The default (`_noop_recorder`) lets instrumentation sites call unconditionally. The recorder threads through `OutboxBrokerConfig.metrics_recorder` to two places:

- **Subscriber emission points** (`OutboxSubscriber._emit_metric`): `fetched`, `dispatched`, `acked`, `nacked_retried`, `nacked_terminal`, `lease_lost`, `drain_timeout` (a `stop()` drain that exceeded `graceful_timeout`), plus `dlq_written` when `dlq_table` is configured.
- **Producer emission point** (`OutboxProducer._emit_metric`): `published`.

The producer reads the recorder from its own constructor kwarg (passed in alongside the config field) so the canonical insert path doesn't have to reach through the broker config at call time.

`dlq_written` and `nacked_terminal` are complementary — alert on a divergence between the two rates to catch DLQ misconfiguration without silent audit loss.

### Safety + no-block rule

Every call site wraps the recorder in `try/except` and logs at DEBUG — a broken recorder never poisons the dispatch loop. The recorder is called from the event loop and **must not block**; sync `Counter.inc()` is fine, blocking HTTP/StatsD calls are not. The library does not wrap user recorders in `asyncio.to_thread` — that would destroy ordering and create per-event task explosion.

## Bundled adapters

`metrics/prometheus.py` and `metrics/opentelemetry.py` are optional extras (`pip install faststream-outbox[prometheus]` / `[opentelemetry]`) — both modules guard their imports so importing `faststream_outbox` without the extras stays clean.

Metric names, status values (`acked, nacked, error`), histogram buckets, and constructor argument names mirror upstream FastStream's `PrometheusMiddleware` / `TelemetryMiddleware` so users running other brokers see consistent dashboards.

**Prometheus uses a different label set for consume vs publish, matching upstream verbatim**: consume tags by `handler` (the subscriber); publish tags by `destination` (the queue). The canonical `messaging.system` / `broker` label value is `"outbox"` — shared by `PrometheusRecorder`, `OpenTelemetryRecorder`, and the native middleware providers below.

OTel adapter is **meter-only** — the callable seam can't bracket span lifecycles; spans land via the native middleware path instead.

## Test broker publish-side coverage

`testing.py` mirrors the producer-side `published` emission in `_build_fake_publish` / `_build_fake_publish_batch` (and `FakeOutboxProducer.publish` / `.publish_batch`) so test code can assert on publish-side metrics without exercising the real producer path; the synthetic events use `duration_seconds=0.0` since the in-memory client has no real write to time.

## Native middleware integration (`opentelemetry/`, `prometheus/`)

Thin subclasses of upstream FastStream's `TelemetryMiddleware[OutboxPublishCommand]` and `PrometheusMiddleware[OutboxInnerMessage, OutboxPublishCommand]` register via `broker_middlewares=[...]`. They fire on `consume_scope` (via `OutboxSubscriber.dispatch_one → self.consume(row)`) and `publish_scope` (via `OutboxBroker.publish → _basic_publish` in `faststream/_internal/broker/pub_base.py:39-51`) — both work without modifying `dispatch_one` or `OutboxProducer`. Empirically verified.

Providers (`opentelemetry/provider.py`, `prometheus/provider.py`) set `messaging_system = "outbox"` — the canonical value shared with the recorder-seam adapters above. The OTel provider maps:

- `row.id → messaging.message.id`
- `row.queue → messaging.destination_publish.name`
- `correlation_id → messaging.message.conversation_id`
- `len(payload) → messaging.message.payload_size_bytes`
- `len(cmd.batch_bodies) → messaging.batch.message_count` when >1

Attribute keys are baked as string literals to avoid the deprecated `SpanAttributes` enum from upstream `opentelemetry.semconv.trace`.

### Test broker quirk

`TestOutboxBroker._patch_broker` replaces `broker.publish` directly via `mock.patch.object`, bypassing `_basic_publish` — so middleware-registered publish metrics do **not** fire in test mode. Consume metrics still fire (`dispatch_one` walks middleware normally). The recorder-seam `published` event provides synthetic publish-side coverage in test mode via the fake producer.

### Two-seam layering — load-bearing

Middleware and recorder are complementary, not redundant. Middleware owns `consume_scope` / `publish_scope` (spans, durations, status, message size). Recorder owns events outside the bus:

- `fetched` — no `StreamMessage` exists at fetch time
- `lease_lost` — fires after `consume_scope` exits
- `nacked_terminal(reason="max_deliveries")` — fires before consume opens

Each seam fires for events the other physically cannot observe. Don't collapse them.
