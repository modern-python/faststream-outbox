# Troubleshooting

Symptom → likely cause → fix. Each section below is the same shape:
what you see, what's probably wrong, how to confirm, what to change,
and a link into the reference page that owns the underlying design.

| Symptom | Likely cause |
|---|---|
| [`event=lease_lost` recurring in logs](#event-lease_lost-recurring-in-logs) | Handler P99 > `lease_ttl_seconds` |
| [Outbox row count grows + `lease_lost` spike](#outbox-row-count-grows-lease_lost-spike) | DLQ CTE failing (DLQ schema drift) |
| [Outbox row count grows, no `lease_lost`](#outbox-row-count-grows-no-lease_lost) | Fetch loop not running, or rows future-dated |
| [Idle dispatch latency > `max_fetch_interval`](#idle-dispatch-latency-max_fetch_interval) | LISTEN setup failed → polling fallback |
| [Subscriber blocks at `broker.start()`](#subscriber-blocks-at-brokerstart) | Engine pool exhausted on writer-connection checkout |
| [Duplicate handler invocations](#duplicate-handler-invocations) | Lease expired before handler returned, or handler not idempotent |
| [Rolling deploy leaks rows](#rolling-deploy-leaks-rows) | `graceful_timeout` < handler P99, or k8s grace too short |
| [`activate_in` / `activate_at` fires immediately in tests](#activate_in-activate_at-fires-immediately-in-tests) | `TestOutboxBroker(run_loops=False)` ignores scheduling |
| [`AckPolicy.ACK_FIRST` raises `ValueError` at registration](#ackpolicyack_first-raises-valueerror-at-registration) | By design (would defeat outbox reliability) |
| [`OutboxResponse(...)` + foreign-publisher decorator gets nacked](#outboxresponse-foreign-publisher-decorator-gets-nacked) | By design (dual-fire footgun) |
| [`validate_schema()` raises `ImportError`](#validate_schema-raises-importerror) | `[validate]` extra not installed |

## `event=lease_lost` recurring in logs { #event-lease_lost-recurring-in-logs }

**Symptom.** WARNING-level logs with structured field
`event=lease_lost`, typically with `phase=terminal` or `phase=retry`,
one per affected row.

**Likely cause.** The subscriber's `lease_ttl_seconds` is shorter than
the handler's P99 duration. A handler took longer than the lease,
another fetch reclaimed the row mid-flight, and the original handler's
terminal `DELETE` / `UPDATE` matched zero rows.

**Diagnose.** Grep for `event=lease_lost` over the last hour and
compare the rate against `dispatched`. A non-zero baseline rate
(rather than occasional spikes) confirms TTL is the issue.

**Fix.** Raise `lease_ttl_seconds` for the affected subscriber, OR
segregate slow work onto its own subscriber with a taller TTL
(recommended — keeps the fast queue's reclaim tight). TTL must exceed
handler P99 with margin.

**Reference.** [Subscriber § Slow handlers — dedicated
queue](../usage/subscriber.md#slow-handlers-dedicated-queue).

## Outbox row count grows + `lease_lost` spike { #outbox-row-count-grows-lease_lost-spike }

**Symptom.** Two things at once: row count in the outbox table grows
without bound, *and* `event=lease_lost` log rate spikes.

**Likely cause.** The DLQ CTE is failing on every terminal flush —
DLQ schema drift means the `INSERT INTO <dlq>` clause inside the
`WITH deleted AS (DELETE … RETURNING …)` statement rolls back the
DELETE too. Rows stay in the outbox, leases keep expiring, the
pattern compounds.

**Diagnose.** Run `await broker.validate_schema()` against the live
DB (the `[validate]` extra is required). It will surface missing
columns / indexes on the DLQ table.

**Fix.** Bring the DLQ schema up to spec (apply the missing migration,
or rename / drop the drifted column / index). After the schema is
correct, the next claim of each stuck row flushes through the CTE
and the outbox drains naturally.

**Reference.** [DLQ § Atomicity](../usage/dlq.md#atomicity), [Schema
validation](../usage/schema-validation.md).

## Outbox row count grows, no `lease_lost` { #outbox-row-count-grows-no-lease_lost }

**Symptom.** Outbox rows accumulate, but logs are clean — no
`lease_lost`, no exceptions.

**Likely cause.** Either no subscriber is registered for that queue,
or the rows are future-dated (`activate_in` / `activate_at` set) and
genuinely waiting to fire.

**Diagnose.** Inspect a stuck row's `next_attempt_at` — if it's in the
future, the row is correctly waiting. Otherwise check whether a
subscriber is registered: walk `broker.subscribers` (the *property*,
which covers router-attached subscribers — `broker._subscribers` will
miss them).

**Fix.** Register the subscriber, or adjust the producer's `activate_*`
arg if the future date was unintentional.

**Reference.** [Subscriber](../usage/subscriber.md), [Router § Gotcha:
walking every subscriber](../usage/router.md#gotcha-walking-every-subscriber),
[Timers](../usage/timers.md).

## Idle dispatch latency > `max_fetch_interval` { #idle-dispatch-latency-max_fetch_interval }

**Symptom.** Rows arrive but take up to `max_fetch_interval` (default
10 s) to dispatch, even though no other rows are in flight. NOTIFY
should short-circuit the idle wait to ~10 ms.

**Likely cause.** `LISTEN` setup failed at subscriber start. The raw
asyncpg connection that owns `LISTEN outbox_<table>` is separate from
the SQLAlchemy fetch connection; common failure modes are: the
asyncpg driver isn't installed (no `[asyncpg]` extra), the engine URL
is not asyncpg, or Postgres user lacks `LISTEN` permission.

**Diagnose.** Check startup logs for a WARNING noting NOTIFY fallback
to polling. The subscriber logs it once and continues without crashing.

**Fix.** Install the `[asyncpg]` extra and use an asyncpg-driven
engine URL (`postgresql+asyncpg://...`). Restart the subscriber.

**Reference.** [Installation § Optional extras
](../introduction/installation.md#optional-extras), [How it works §
Fetch loop](../introduction/how-it-works.md#subscriber-two-async-loops).

## Subscriber blocks at `broker.start()` { #subscriber-blocks-at-brokerstart }

**Symptom.** Process hangs on `broker.start()` (or the FastAPI
`include_router` lifespan) and never completes startup.

**Likely cause.** SQLAlchemy pool exhausted on the per-worker writer
connection checkout. Each subscriber needs `max_workers + 1` pool
connections; the default pool is `pool_size=5, max_overflow=10`. A
handful of single-worker subscribers fits, but a fleet of high-
`max_workers` subscribers does not.

**Diagnose.** Inspect the engine pool. Compute `Σ subs × (max_workers
+ 1)` from your subscriber registrations and compare to
`pool_size + max_overflow`.

**Fix.** Raise `pool_size` / `max_overflow` on the engine, OR lower
`max_workers` per subscriber. Also confirm Postgres
`max_connections ≥ replicas × Σ subs × (max_workers + 1)` — rolling
deploys multiply the demand.

**Reference.** [Subscriber § Connection
budget](../usage/subscriber.md#connection-budget), [Production
checklist § Sizing](./checklist.md#sizing).

## Duplicate handler invocations

**Symptom.** The same outbox row's handler runs more than once. Side
effects double up if the handler isn't idempotent.

**Likely cause.** Either the handler's wall-clock duration exceeded
`lease_ttl_seconds` and another fetch reclaimed the row mid-flight,
or the worker crashed between the handler's external side effect and
the terminal `DELETE`. Both are at-least-once-delivery edge cases.

**Diagnose.** Cross-reference handler-side logs (the side effect)
with `event=lease_lost` logs. Matching row IDs confirm TTL is too
short. Crash-induced duplicates correlate with worker-process
restarts.

**Fix.** Two layers: (a) make handlers idempotent — this is a
contract of the outbox pattern, not a knob, and (b) tune
`lease_ttl_seconds` above handler P99 so healthy handlers don't
race their lease.

**Reference.** [How it works § At-least-once
delivery](../introduction/how-it-works.md#at-least-once-delivery),
[Subscriber § Slow handlers — dedicated
queue](../usage/subscriber.md#slow-handlers-dedicated-queue).

## Rolling deploy leaks rows

**Symptom.** During a rolling restart, outbox rows are left in the
"acquired" state until lease expiry, even though handlers were
nominally healthy. Drain duration appears longer than expected.

**Likely cause.** Either the broker's `graceful_timeout` is shorter
than the in-flight handler's remaining work, or Kubernetes
`terminationGracePeriodSeconds` is shorter than the broker's
`graceful_timeout` × parallel-drain factor — `SIGKILL` arrives mid-
drain.

**Diagnose.** Time a clean shutdown locally (`docker compose kill -s
SIGTERM application`) and compare to your k8s grace period. Look for
log lines indicating drain abandonment.

**Fix.** Raise `graceful_timeout` past handler P99 + margin. Raise
`terminationGracePeriodSeconds` past `graceful_timeout` + buffer for
parallel-subscriber drain. The `dispatch_one` shutdown-race guard is
always on; you don't need to opt into it.

**Reference.** [Production checklist § Drain &
lifecycle](./checklist.md#drain-lifecycle).

## `activate_in` / `activate_at` fires immediately in tests { #activate_in-activate_at-fires-immediately-in-tests }

**Symptom.** A unit test publishes a row with `activate_in=30s` and
the handler runs synchronously inside `await broker.publish(...)`.

**Likely cause.** By design. `TestOutboxBroker(run_loops=False)`
(the default) drives handlers synchronously through `dispatch_one`,
which ignores `next_attempt_at`. This is the documented test-broker
contract — trades production parity for test ergonomics.

**Diagnose.** Check the call site: `TestOutboxBroker(broker)` →
sync mode, expected immediate firing.

**Fix.** Opt into `TestOutboxBroker(broker, run_loops=True)` for
tests that need scheduled delivery to actually wait. Loop mode runs
the real `_fetch_loop` / `_worker_loop` against the fake client.

**Reference.** [Testing § Loop-driven
mode](../usage/testing.md#loop-driven-mode), [Timers § Test broker
note](../usage/timers.md#test-broker-note).

## `AckPolicy.ACK_FIRST` raises `ValueError` at registration { #ackpolicyack_first-raises-valueerror-at-registration }

**Symptom.** `@broker.subscriber("q", ack_policy=AckPolicy.ACK_FIRST)`
fails with `ValueError` at decoration time.

**Likely cause.** By design. `ACK_FIRST` would delete the outbox row
*before* the handler runs, so a handler crash would silently drop
the message — exactly the failure mode the outbox pattern exists to
prevent.

**Diagnose.** None needed; the message identifies the policy.

**Fix.** Use the default `AckPolicy.NACK_ON_ERROR` (retry on handler
exception via the configured retry strategy), or
`AckPolicy.REJECT_ON_ERROR` (delete on first failure), or
`AckPolicy.MANUAL` (handler calls `ack` / `nack` / `reject`).

**Reference.** [Subscriber § Ack
policy](../usage/subscriber.md#ack-policy).

## `OutboxResponse(...)` + foreign-publisher decorator gets nacked { #outboxresponse-foreign-publisher-decorator-gets-nacked }

**Symptom.** A handler with both `@kafka_pub` and an
`OutboxResponse(...)` return value gets nacked on every dispatch, with
a `_OutboxConfigError` logged.

**Likely cause.** By design. The combination would both insert a row
into the outbox *and* publish to Kafka — a dual-fire that doubles
delivery. The subscriber refuses the chain composition by raising
`_OutboxConfigError` via the `process_message` override; it rides the
normal nack path so the row is retried (and logged) until the
configuration is fixed.

**Diagnose.** Inspect the handler decorator stack and return type.

**Fix.** Pick one path. Either `return body` plain (the foreign
publisher picks it up) or `return OutboxResponse(body, queue="...",
session=...)` (an outbox-internal chain) but not both.

**Reference.** [Relay § What not to do](../usage/relay.md#what-not-to-do),
[Publisher § Chained
publishing](../usage/publisher.md#chained-publishing).

## `validate_schema()` raises `ImportError` { #validate_schema-raises-importerror }

**Symptom.** Calling `await broker.validate_schema()` raises
`ImportError("requires alembic")`.

**Likely cause.** The `[validate]` extra isn't installed. Alembic is
an optional dependency by design — every other code path works
without it, but the schema validator delegates to Alembic's
`autogenerate.compare_metadata` and so requires it.

**Diagnose.** `pip show alembic` returns nothing, or
`pip list | grep alembic` is empty.

**Fix.** `pip install 'faststream-outbox[validate]'`. The validator
runs unchanged after that; nothing else in the package needs to
change.

**Reference.** [Schema validation](../usage/schema-validation.md).
