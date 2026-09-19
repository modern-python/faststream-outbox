# Metrics recorders stay separate, with no shared event-to-metric table

The two `MetricsRecorder` adapters, `metrics/prometheus.py` and `metrics/opentelemetry.py`, keep
their own hand-written `__call__` event switches, and no declarative event-to-metric table is shared
between them. Only the dispatch ladder is common: the same event maps to a different number of
instruments per backend, so `dispatched` is three Prometheus operations and an OpenTelemetry no-op,
and the same tag is a labeled counter in one and a span attribute in the other. A
`(kind, name, label set)` table would need a per-backend escape hatch on nearly every row, and
deleting it would push the per-event bodies back exactly where they are now. A parity contract test
feeding each event to both adapters addresses the real worry, which is parity rather than
duplication, and is declined for now because the event vocabulary is stable and additive and both
adapters are independently tested.
