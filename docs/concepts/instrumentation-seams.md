# Instrumentation seams

`faststream-outbox` exposes **two complementary instrumentation seams** —
a *recorder* (callable) and a *native middleware* — and recommends
running both. This page explains why two; the practical setup recipes
live in [Setup Prometheus and OpenTelemetry](../usage/setup-prometheus-opentelemetry.md),
and the event catalog and PromQL playbook in
[Observability](../usage/observability.md).

## The fundamental tension

A FastStream broker emits two natural observation moments:

- `consume_scope` — wraps a single handler invocation. The middleware
  bus surfaces handler duration, message size, exception status, span
  context.
- `publish_scope` — wraps a single producer call. Same idea on the
  outbound side.

Upstream FastStream middlewares (`TelemetryMiddleware`,
`PrometheusMiddleware`) hook into these two scopes. For Kafka, Rabbit,
NATS, that's the entire surface area — those buses don't have
outbox-internal events because they don't *have* an outbox.

`faststream-outbox` does have outbox-internal events, and the middleware
bus physically cannot observe them.

## What the middleware seam observes naturally

Wrap `consume_scope` and `publish_scope` and you get:

- Handler duration / status / message size.
- Span tracing across the handler invocation and the publish call.
- The exact label / instrument schema upstream Kafka and Rabbit users
  already have dashboards for.

This is the "spans + bus parity" mode the native middleware
(`OutboxTelemetryMiddleware`, `OutboxPrometheusMiddleware`) provides.

## What the middleware seam *can't* observe

Three events fire **outside** the handler invocation, with no
`StreamMessage` in scope:

- **`fetched` ticks (including empty fetches).** Emitted by the fetch
  loop every time it claims rows from the table, *before* any handler
  runs. The middleware bus has no `consume_scope` to wrap yet — there
  is no message. Empty-fetch ticks are also load-bearing for
  detecting "polling but the queue is empty" patterns; the middleware
  bus never sees them.
- **`lease_lost` events.** Fired after `consume_scope` has already
  closed (the handler returned successfully but its terminal `DELETE`
  matched zero rows because the lease expired). By the time we know
  the row was lost, the middleware has long since recorded a normal
  `acked`. The recorder catches the truth.
- **`nacked_terminal(reason="max_deliveries")`.** This row exceeded
  the `max_deliveries` ceiling and was dropped *without invoking the
  handler*. No handler call = no `consume_scope`. The middleware has
  nothing to wrap.

## What the recorder seam observes naturally

The recorder is a `Callable[[str, Mapping[str, Any]], None]` invoked at
six core subscriber events (`fetched`, `dispatched`, `acked`,
`nacked_retried`, `nacked_terminal`, `lease_lost`), a conditional
`dlq_written` when the DLQ is configured, and one producer event
(`published`). It fires whether or not a handler is in scope:

- All three bus-invisible events above.
- Plus `acked` / `nacked_retried` / `nacked_terminal` / `dispatched` /
  `published` from inside the handler-execution paths, with explicit
  `subscriber` and `queue` tags.

The recorder cannot bracket span lifecycles (it's a callable, not a
context manager), so tracing belongs to the middleware seam. It also
runs **on the dispatch event loop and must not block** — a synchronous
`Counter.inc()` is fine; an HTTP / StatsD push is not. See
[Observability § Recorder must not block](../usage/observability.md#recorder-must-not-block)
for the full contract.

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

## Operator implication

**Run both.** Middleware for bus-scope metrics, distributed tracing,
and label parity with the rest of your FastStream services. Recorder
for the outbox-internal events that don't have a `StreamMessage` to
attach to.

The "Both seams together" recipe in [Setup Prometheus and OpenTelemetry
](../usage/setup-prometheus-opentelemetry.md#both-seams-together)
wires the recommended layout: native middleware on the broker, plus a
`metrics_recorder` for the outbox-internal events.

This isn't redundancy — each seam fires for events the other can't see.
A service that registers only the middleware seam loses every
`lease_lost`, `fetched`, and `max_deliveries`-terminal signal. A
service that registers only the recorder seam loses tracing.
