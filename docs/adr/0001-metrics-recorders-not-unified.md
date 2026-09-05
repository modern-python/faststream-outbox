# Metrics recorders stay separate; no shared event→metric table

**Decision:** the two `MetricsRecorder` adapters (`metrics/prometheus.py`,
`metrics/opentelemetry.py`) keep their own hand-written `__call__` event switches. There is no
declarative event→metric table both adapters consume.

The two switches branch on the same event names, which reads as duplication, but only the dispatch
ladder is actually shared. The same event maps to a *different number of instruments* per backend —
`dispatched` is three Prometheus operations and an OpenTelemetry no-op — and the same tag is a
labeled counter in one and a span attribute in the other. A `(kind, name, label set)` table would
need a per-backend escape hatch on nearly every row, and deleting it would push the per-event bodies
back exactly where they are now.

- **A parity contract test**, feeding each event to both adapters, addresses the real worry ("add an
  event, forget one adapter") — which is parity, not duplication. Declined for now: the vocabulary
  is stable and additive, both adapters are independently tested, and a new event is emitted from a
  call site the author is already editing.
