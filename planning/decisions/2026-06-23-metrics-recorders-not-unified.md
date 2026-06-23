---
status: accepted
date: 2026-06-23
slug: metrics-recorders-not-unified
summary: Keep PrometheusRecorder and OpenTelemetryRecorder as separate hand-written event switches — do not factor them behind a shared data-driven event→metric table.
supersedes: null
superseded_by: null
pr: 112
---

# Metrics recorders stay separate; no shared event→metric table

**Decision:** The two `MetricsRecorder` adapters (`metrics/prometheus.py`,
`metrics/opentelemetry.py`) keep their own hand-written `__call__` event switches.
We will **not** introduce a declarative event→metric table both adapters consume.

## Context

The 2026-06-23 architecture review (candidate #4) flagged the two adapters as
duplication: both `__call__` methods branch on the same event names (`fetched`,
`dispatched`, `acked`/`nacked_*`, `lease_lost`, `dlq_written`, `drain_timeout`,
`published`) in ~80-line switches, and proposed a shared table mapping each event to
`(metric kind, name, label set)` so the vocabulary lives once. The review marked it
*Speculative* and noted the emit primitives differ.

## Decision & rationale

On inspection the shared surface is only the **event-name dispatch ladder** plus the
tag keys read. The per-event bodies are irreducibly backend-specific:

- `dispatched` → Prometheus does three operations (received counter + size histogram +
  in-process **gauge.inc**); OpenTelemetry is a **no-op**.
- `acked`/`nacked_*` → Prometheus uses *separate labeled counters* (`processed_total`,
  `terminal_reason`, `processed_exceptions`) plus a gauge `.dec`; OTel folds `reason` /
  `exception` into **attributes** on one shared duration histogram.
- `lease_lost` → Prometheus *reuses* `processed_total{status=error}` and adds a
  `lease_lost` counter; OTel only touches `lease_lost`.
- `fetched` → Prometheus encodes `non_empty` as a label; OTel has no such attribute.
- `published` → different count-vs-error gating per backend.

A `(metric kind, name, label set)` table cannot express this: the same event maps to a
*different number of instruments* per backend, the same tag is a *labeled counter* in
Prometheus but an *attribute* in OTel, and Prometheus has cross-event reuse OTel does
not. A table would need a per-backend escape hatch on nearly every row.

**Deletion test:** deleting a hypothetical shared table pushes the per-event bodies back
exactly where they are now — only the trivial dispatch ladder disappears. That is a
*shallow* abstraction; building it would worsen locality (a reader of one metric would
bounce between the table and per-backend overrides). The two-seam split (recorder vs
native middleware) is deliberate depth and already documented in
`architecture/metrics.md`; the per-adapter switches are backend-specific *translation*,
not duplication worth abstracting.

The review's real underlying worry — "add an event, forget one adapter, no test catches
it" — is a parity concern, not a dedup one. We are **not** adding a parity contract test
either, for now: the event vocabulary is stable/additive (changes are rare), each adapter
has thorough independent tests (23 + 17), and a new event is emitted from a new call site
the author touches anyway. If that calculus changes, see the revisit trigger.

## Revisit trigger

Reopen if **either**:

- the event vocabulary starts changing often (≥3 new events within a release cycle), at
  which point a machine-readable `EVENTS` registry + a parity test that feeds each event
  to both adapters (the candidate-#1 "co-verify, don't share" pattern) becomes worth it; or
- a third `MetricsRecorder` adapter is added (StatsD, etc.) — three hand-written switches
  may shift the cost/benefit toward a shared dispatch skeleton with per-backend emit hooks.
