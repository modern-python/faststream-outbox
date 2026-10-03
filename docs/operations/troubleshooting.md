# Troubleshooting

Each entry below starts with what you see, then gives the probable cause,
how to confirm it, what to change, and a link to the reference page that
owns the underlying design.

| Symptom | Likely cause |
|---|---|
| [`event=lease_lost` recurring in logs](#event-lease_lost-recurring-in-logs) | Handler P99 > `lease_ttl_seconds` |
| [Outbox row count grows + `lease_lost` spike](#outbox-row-count-grows-lease_lost-spike) | DLQ CTE failing (DLQ schema drift) |
| [Outbox row count grows, no `lease_lost`](#outbox-row-count-grows-no-lease_lost) | Fetch loop not running, or rows future-dated |
| [Idle dispatch latency > `max_fetch_interval`](#idle-dispatch-latency-max_fetch_interval) | LISTEN setup failed, so the subscriber falls back to polling |
| [Subscriber dispatch never starts; rows pile up](#subscriber-blocks-at-brokerstart) | Engine pool exhausted on writer-connection checkout |
| [Duplicate handler invocations](#duplicate-handler-invocations) | Lease expired before handler returned, or handler not idempotent |
| [Rolling deploy leaks rows](#rolling-deploy-leaks-rows) | `graceful_timeout` < handler P99, or k8s grace too short |
| [`activate_in` / `activate_at` fires immediately in tests](#activate_in-activate_at-fires-immediately-in-tests) | `TestOutboxBroker(run_loops=False)` ignores scheduling |
| [`AckPolicy.ACK_FIRST` raises `ValueError` at registration](#ackpolicyack_first-raises-valueerror-at-registration) | By design (would defeat outbox reliability) |
| [`OutboxResponse(...)` + foreign-publisher decorator logs a configuration error](#outboxresponse-foreign-publisher-decorator-config-error) | By design (dual-fire footgun) |
| [Chained `OutboxResponse` retries after the handler "succeeded"](#outboxresponse-relay-publish-failure) | Follow-on publish fails post-handler; nacks the inbound row |
| [`validate_schema()` raises `ImportError`](#validate_schema-raises-importerror) | `[validate]` extra not installed |

## `event=lease_lost` recurring in logs { #event-lease_lost-recurring-in-logs }

You see WARNING-level logs with the message text
`lease expired before terminal write` or `lease expired before retry write`,
one per affected row. The record also carries `event=lease_lost` and
`phase=terminal` / `phase=retry` as structured extras, visible when your log
formatter renders extras (for example a JSON formatter).

The likely cause is a subscriber `lease_ttl_seconds` shorter than the
handler's P99 duration. A handler took longer than the lease, another
fetch reclaimed the row mid-flight, and the original handler's terminal
`DELETE` / `UPDATE` matched zero rows.

To confirm, grep for `lease expired before` (or, with an
extras-rendering formatter, `event=lease_lost`) over the last hour and
compare the rate against `dispatched`. A steady non-zero rate, as
opposed to occasional spikes, confirms that the TTL is the issue.

To fix it, raise `lease_ttl_seconds` for the affected subscriber, or move
slow work onto its own subscriber with a taller TTL. The second option is
recommended because it keeps the fast queue's reclaim tight. Either way,
the TTL must exceed handler P99 with margin.

See [Subscriber § Slow handlers: dedicated
queue](../usage/subscriber.md#slow-handlers-dedicated-queue).

## Outbox row count grows + `lease_lost` spike { #outbox-row-count-grows-lease_lost-spike }

Two things happen at once: the row count in the outbox table grows
without bound, *and* the `event=lease_lost` log rate spikes.

The likely cause is a DLQ CTE that fails on every terminal flush. With
DLQ schema drift, the `INSERT INTO <dlq>` clause inside the
`WITH deleted AS (DELETE … RETURNING …)` statement fails and rolls back
the DELETE too. Rows stay in the outbox, leases keep expiring, and the
pattern compounds.

To confirm, run `await broker.validate_schema()` against the live
DB (the `[validate]` extra is required). It reports missing
columns and indexes on the DLQ table. A frequent cause on older
deployments is a hand-written DLQ migration without the `timer_id`
column, which `validate_schema()` reports as a missing column on the DLQ
table. The [Alembic guide](../operations/alembic.md#adding-the-dlq-after-the-fact)
includes it.

To fix it, bring the DLQ schema up to spec: apply the missing migration,
or rename or drop the drifted column or index. Once the schema is
correct, the next claim of each stuck row flushes through the CTE
and the outbox drains on its own.

A persistent DLQ misconfiguration (or a permanent relay config error) is
the one way a config bug degrades into a storage-exhaustion outage: the
affected rows cycle through fetch and fail forever while new rows
accumulate. There is no built-in circuit breaker, so alert on the outbox
row count (trend and absolute ceiling) and on the `lease_lost` rate. Also
watch for divergence between `dlq_written` and `nacked_terminal`; a gap
means terminal failures aren't reaching the DLQ.

See [DLQ § Atomicity](../usage/dlq.md#atomicity) and [Schema
validation](../usage/schema-validation.md).

## Outbox row count grows, no `lease_lost` { #outbox-row-count-grows-no-lease_lost }

Outbox rows accumulate, but the logs are clean, with no `lease_lost` and
no exceptions.

Either no subscriber is registered for that queue, or the rows are
future-dated (`activate_in` / `activate_at` set) and are waiting to fire.

To tell the two apart, inspect a stuck row's `next_attempt_at`. If it's
in the future, the row is correctly waiting. Otherwise check whether a
subscriber is registered by walking `broker.subscribers`, which covers
router-attached subscribers too.

To fix it, register the subscriber, or adjust the producer's `activate_*`
argument if the future date was unintentional.

See [Subscriber](../usage/subscriber.md), [Router § Gotcha:
walking every subscriber](../usage/router.md#gotcha-walking-every-subscriber),
and [Timers](../usage/timers.md).

## Idle dispatch latency > `max_fetch_interval` { #idle-dispatch-latency-max_fetch_interval }

Rows arrive but take up to `max_fetch_interval` (default 10 s) to
dispatch, even though no other rows are in flight. NOTIFY should cut the
idle wait short to about 10 ms.

The likely cause is a `LISTEN` setup failure at subscriber start. The raw
asyncpg connection that owns `LISTEN outbox_<table>` is separate from
the SQLAlchemy fetch connection. Common failure modes are a missing
asyncpg driver (no `[asyncpg]` extra), an engine URL that is not asyncpg,
and a Postgres user without `LISTEN` permission.

A connection or permission failure (`asyncpg.connect` or `add_listener`
raising) logs a WARNING once at startup noting the NOTIFY fallback to
polling. A missing asyncpg driver or a non-asyncpg engine URL falls back
silently, with no log line. In that case, check that the engine URL's
`drivername` is `postgresql+asyncpg` and that the `[asyncpg]` extra is
installed.

To fix it, install the `[asyncpg]` extra, use an asyncpg-driven engine
URL (`postgresql+asyncpg://...`), and restart the subscriber.

See [Installation § Optional extras
](../introduction/installation.md#optional-extras) and [How it works §
Fetch loop](../introduction/how-it-works.md#subscriber-two-async-loops).

## Subscriber dispatch never starts; rows pile up { #subscriber-blocks-at-brokerstart }

Rows are published but never dispatched (the table grows), and the
subscriber's loops emit repeating reconnect ERROR logs. `broker.start()`
(or the FastAPI `include_router` lifespan) returns normally because it
only schedules the loop tasks, so the failure shows up *after* startup
and does not look like a hang.

The likely cause is an exhausted SQLAlchemy pool on the per-worker writer
connection checkout. The fetch and worker loops can't acquire their
connections, so each cycle errors and backs off. Each subscriber needs
`max_workers + 1` pool connections, and the default pool is `pool_size=5,
max_overflow=10`. A handful of single-worker subscribers fits; a fleet
of high-`max_workers` subscribers does not.

To confirm, inspect the engine pool. Compute `Σ subs × (max_workers
+ 1)` from your subscriber registrations and compare it to
`pool_size + max_overflow`.

To fix it, raise `pool_size` / `max_overflow` on the engine, or lower
`max_workers` per subscriber. Also confirm that Postgres has
`max_connections ≥ replicas × Σ subs × (max_workers + 2)` (the pool's
`max_workers + 1` plus the raw `LISTEN` connection). Rolling deploys
multiply the demand.

See [Subscriber § Connection
budget](../usage/subscriber.md#connection-budget) and [Production
checklist § Sizing](./checklist.md#sizing).

## Duplicate handler invocations

The same outbox row's handler runs more than once, and side effects
double up if the handler isn't idempotent.

There are two likely causes, both edge cases of at-least-once delivery.
Either the handler's wall-clock duration exceeded `lease_ttl_seconds`
and another fetch reclaimed the row mid-flight, or the worker crashed
between the handler's external side effect and the terminal `DELETE`.

To tell them apart, cross-reference handler-side logs (the side effect)
with `lease expired before` warnings (`event=lease_lost` with an
extras-rendering formatter). Matching row IDs confirm that the TTL is too
short. Crash-induced duplicates correlate with worker-process restarts.

Delivery is at-least-once, so handlers must be idempotent. Also tune
`lease_ttl_seconds` above handler P99 so healthy handlers don't race
their lease.

See [How it works § At-least-once
delivery](../introduction/how-it-works.md#at-least-once-delivery) and
[Subscriber § Slow handlers: dedicated
queue](../usage/subscriber.md#slow-handlers-dedicated-queue).

## Rolling deploy leaks rows

During a rolling restart, outbox rows stay in the "acquired" state until
lease expiry, even though handlers were nominally healthy. Draining takes
longer than expected.

Either the broker's `graceful_timeout` is shorter than the in-flight
handler's remaining work, or Kubernetes `terminationGracePeriodSeconds`
is shorter than the broker's `graceful_timeout` and `SIGKILL` arrives
mid-drain. Subscribers drain concurrently, so a clean shutdown takes
about one `graceful_timeout`.

To confirm, time a clean shutdown locally (`docker compose kill -s
SIGTERM application`) and compare it to your k8s grace period. Look for
log lines indicating drain abandonment.

To fix it, raise `graceful_timeout` past handler P99 plus margin, and
raise `terminationGracePeriodSeconds` past `graceful_timeout` plus a
buffer. The `dispatch_one` shutdown-race guard is always on; you don't
need to opt into it.

See [Production checklist § Drain &
lifecycle](./checklist.md#drain-lifecycle).

## `activate_in` / `activate_at` fires immediately in tests { #activate_in-activate_at-fires-immediately-in-tests }

A unit test publishes a row with `activate_in=30s`, and the handler runs
synchronously inside `await broker.publish(...)`.

This is by design. `TestOutboxBroker(run_loops=False)` (the default)
drives handlers synchronously through `dispatch_one`, which ignores
`next_attempt_at`. That is the documented test-broker contract: it
trades production parity for test ergonomics.

Check the call site. `TestOutboxBroker(broker)` runs in sync mode, where
immediate firing is expected.

For tests that need scheduled delivery to wait, opt into
`TestOutboxBroker(broker, run_loops=True)`. Loop mode runs the real
fetch and worker loops against the fake client.

See [Testing § Loop-driven
mode](../usage/testing.md#loop-driven-mode) and [Timers § Test broker
note](../usage/timers.md#test-broker-note).

## `AckPolicy.ACK_FIRST` raises `ValueError` at registration { #ackpolicyack_first-raises-valueerror-at-registration }

`@broker.subscriber("q", ack_policy=AckPolicy.ACK_FIRST)` fails with
`ValueError` at decoration time.

This is by design. `ACK_FIRST` would delete the outbox row *before* the
handler runs, so a handler crash would silently drop the message, which
is exactly the failure the outbox pattern exists to prevent. The error
message names the policy, so there is nothing else to diagnose.

Use the default `AckPolicy.NACK_ON_ERROR` (retry on handler exception
via the configured retry strategy), `AckPolicy.REJECT_ON_ERROR` (delete
on first failure), or `AckPolicy.MANUAL` (the handler calls `ack` /
`nack` / `reject`).

See [Subscriber § Ack policy](../usage/subscriber.md#ack-policy).

## `OutboxResponse(...)` + foreign-publisher decorator logs a configuration error { #outboxresponse-foreign-publisher-decorator-config-error }

A handler with both `@kafka_pub` and an `OutboxResponse(...)` return
value logs an ERROR on every dispatch:
`Outbox configuration error (fix required; row left to lease-expiry retry)`.

This is by design. The combination would both insert a row into the
outbox *and* publish to Kafka, a dual-fire that doubles delivery. The
subscriber refuses the chain composition after the handler returns. The
worker logs the error and moves on without nacking, so the retry
strategy is not consulted. The row's lease expires, a later fetch
reclaims it, and the cycle repeats until the configuration is fixed.

To confirm, inspect the handler's decorator stack and return type.

To fix it, pick one path: either `return body` plain (the foreign
publisher picks it up) or `return OutboxResponse(body, queue="...",
session=...)` (an outbox-internal chain), but not both.

See [Relay § What not to do](../usage/relay.md#what-not-to-do) and
[Publisher § Chained
publishing](../usage/publisher.md#chained-publishing).

## A chained `OutboxResponse` row's handler keeps retrying after the handler "succeeded" { #outboxresponse-relay-publish-failure }

A handler that returns `OutboxResponse(...)` completes its own logic,
yet the inbound row keeps nacking and retrying (and may go to the DLQ as
`retry_terminal`), with an exception about the *publish* and not about
the handler's work.

The follow-on `OutboxResponse` row is published after the handler
returns, inside the same consume scope. A failure there (for example a
DB error on the follow-on insert) unwinds through the
`AcknowledgementMiddleware` and nacks the inbound row. No distinct
signal separates "handler OK, relay-publish failed" from an ordinary
handler exception: the metric reads as a normal
`nacked_retried`/`retry_terminal`, and the ERROR log shows the publish
exception, not a handler one.

To confirm, read the logged exception. A `sqlalchemy`/`asyncpg` error or
an envelope `ValueError` naming `content-type`/`correlation_id` points at
the relay publish, not the handler body.

To fix it, resolve the underlying publish failure: the schema or
connection for the follow-on insert, or conflicting headers that need
dropping. For non-idempotent chains, pass a deterministic `timer_id` so
a redelivery's insert is a no-op.

See [Publisher § Chained publishing](../usage/publisher.md#chained-publishing).

## `validate_schema()` raises `ImportError` { #validate_schema-raises-importerror }

Calling `await broker.validate_schema()` raises:

```text
ImportError: validate_schema() requires alembic. Install with `pip install 'faststream-outbox[validate]'`.
```

The `[validate]` extra isn't installed. Alembic is an optional
dependency by design. Every other code path works without it, but the
schema validator delegates to Alembic's `autogenerate.compare_metadata`
and so requires it.

To confirm, run `pip show alembic` (it returns nothing) or
`pip list | grep alembic` (empty output).

To fix it, run `pip install 'faststream-outbox[validate]'`. The
validator works after that, and nothing else in the package needs to
change.

See [Schema validation](../usage/schema-validation.md).
