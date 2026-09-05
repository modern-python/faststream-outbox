# Metrics recorders stay separate; no shared event→metric table

**Decision:** the two `MetricsRecorder` adapters (`metrics/prometheus.py`,
`metrics/opentelemetry.py`) keep their own hand-written `__call__` event switches. There is no
declarative event→metric table both adapters consume.

The 2026-06-23 architecture review flagged the two adapters as duplication: both `__call__` methods
branch on the same event names (`fetched`, `dispatched`, `acked`/`nacked_*`, `lease_lost`,
`dlq_written`, `drain_timeout`, `published`) in ~80-line switches, and proposed a shared table
mapping each event to `(metric kind, name, label set)`. On inspection the shared surface is only the
event-name dispatch ladder plus the tag keys read; the per-event bodies are irreducibly
backend-specific. `dispatched` is three Prometheus operations and an OpenTelemetry no-op.
`acked`/`nacked_*` are separate labeled Prometheus counters plus a gauge decrement, against one
shared OTel duration histogram with `reason`/`exception` folded into attributes. `lease_lost` reuses
Prometheus' `processed_total{status=error}` and has no OTel analogue. A `(metric kind, name, label
set)` table cannot express that: the same event maps to a different *number* of instruments per
backend, the same tag is a labeled counter in one and an attribute in the other, and Prometheus has
cross-event reuse OTel does not — so nearly every row would need a per-backend escape hatch.
Deleting the hypothetical table pushes the per-event bodies back exactly where they are now; only
the trivial dispatch ladder disappears. That is a shallow abstraction, and it would worsen locality:
a reader of one metric would bounce between the table and per-backend overrides.

- **A parity contract test** — feeding each event to both adapters — addresses the review's real
  underlying worry ("add an event, forget one adapter"), which is parity, not deduplication. Also
  declined for now: the event vocabulary is stable and additive, each adapter has thorough
  independent tests, and a new event is emitted from a new call site the author touches anyway.

**Revisit trigger:** the event vocabulary starts changing often (≥3 new events within a release
cycle), at which point a machine-readable `EVENTS` registry plus the parity test earns its keep; or
a third `MetricsRecorder` adapter is added, where three hand-written switches may shift the
cost/benefit toward a shared dispatch skeleton with per-backend emit hooks.
