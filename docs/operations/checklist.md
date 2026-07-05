# Production checklist

Scannable scaffold of pre-launch checks. Each item is one to two lines;
the link points at the existing reference page that owns the full
story.

## Sizing

- [ ] **Engine pool ≥ `Σ subs × (max_workers + 1)`** — every
  subscriber holds `max_workers + 1` SQLAlchemy pool connections (one
  writer per worker + one fetch) plus one raw asyncpg connection for
  `LISTEN`. Sub-budget formula in [Subscriber § Connection
  budget](../usage/subscriber.md#connection-budget).
- [ ] **Postgres `max_connections` ≥ `replicas × Σ subs × (max_workers + 2)`**
  — `max_workers + 1` pool connections **plus** the raw asyncpg `LISTEN`
  connection per subscriber; the formula is per-process and rolling deploys
  multiply it. Failure mode: pods refuse with `FATAL: too many connections`.

## Subscribers

- [ ] **`lease_ttl_seconds` > handler P99 with margin** — otherwise
  healthy in-flight handlers race their own lease expiry. The lease
  cutoff is server-side `make_interval(...)`, immune to clock skew.
  Tuning: [Subscriber § Slow handlers — dedicated
  queue](../usage/subscriber.md#slow-handlers-dedicated-queue).
- [ ] **Slow handlers segregated** onto their own subscriber with a
  taller `lease_ttl_seconds`. Don't raise it globally — that delays
  reclaim of *actually* stuck rows everywhere.
- [ ] **`max_deliveries` set** (or knowingly unbounded). Default is
  unbounded; pair with a non-`NoRetry()` retry strategy or
  wedge-prone handlers can replay forever.
- [ ] **Retry strategy chosen.** Default
  `ExponentialRetry(initial_delay_seconds=1.0, multiplier=2.0,
  max_delay_seconds=300.0, max_attempts=10, jitter_factor=0.2)` is fine
  for most. Opt into `NoRetry()` explicitly for an audit feed.

## DLQ

- [ ] **`dlq_table=` configured** — opt-in but recommended for any
  service where terminal failures need forensic recovery. See
  [Dead-letter queue](../usage/dlq.md).
- [ ] **Alert on `nacked_terminal` rate vs `dlq_written` divergence**
  — persistent divergence means either DLQ schema drift (CTE rolls
  back) or `lease_ttl_seconds` too low. See [DLQ § Metric:
  dlq_written](../usage/dlq.md#metric-dlq_written).
- [ ] **DLQ retention plan.** Partition by `failed_at` + cron-drop old
  partitions, or a simple `DELETE … WHERE failed_at < interval` cron
  for low volume. Walk-through: [Alembic migrations § DLQ retention via
  partition drop](./alembic.md#dlq-retention-via-partition-drop).

## Drain & lifecycle

- [ ] **`graceful_timeout` ≥ handler P99 + margin** — otherwise
  `OutboxSubscriber.stop()` cancels in-flight work and rows are
  reclaimed mid-handler.
- [ ] **Kubernetes `terminationGracePeriodSeconds` ≥ broker
  `graceful_timeout`** with margin for the parallel-subscriber drain.
  The broker gathers subscriber drains in parallel, but k8s
  `SIGKILL`s after the grace period regardless.

## Schema

- [ ] **`/health` calls `validate_schema()`** — opt-in; requires the
  `[validate]` extra. Do **not** call at `broker.start()` — that
  would crash-loop on a pending migration. See [Schema validation §
  Where to call it](../usage/schema-validation.md#where-to-call-it).
- [ ] **Outbox `table_name` short enough for every derived identifier** —
  `make_outbox_table` raises `ValueError` at table-build time when the
  *longest* identifier it derives exceeds Postgres' 63-**byte** limit. That
  longest identifier is usually an index/constraint name
  (`<table_name>_pending_idx`, `<table_name>_timer_id_uq`), which is longer
  than the NOTIFY channel `outbox_<table_name>` — so a name that fits the
  channel can still overflow an index name. There is no silent truncation or
  polling fallback; the guard makes an over-long name impossible to ship.

## Observability

- [ ] **`metrics_recorder` set, native middleware registered, or
  both** — the recommended setup is both. See [Instrumentation seams §
  Layering](../concepts/instrumentation-seams.md#layering-middleware-seam-vs-recorder-seam).
- [ ] **Alert on `lease_lost` rate** — non-zero means
  `lease_ttl_seconds < handler P99` for at least one subscriber. See
  [Troubleshooting § `event=lease_lost`](./troubleshooting.md#event-lease_lost-recurring-in-logs).
- [ ] **`LISTEN/NOTIFY` fallback understood** — a *connection* or
  *permission* failure (`asyncpg.connect` / `add_listener` raising) logs a
  WARNING once and falls back to polling. A **missing asyncpg driver or a
  non-asyncpg engine URL falls back silently** (no log) — diagnose those
  from the engine URL, not the logs. Either way the subscriber lives with
  up-to-`max_fetch_interval` idle latency.
