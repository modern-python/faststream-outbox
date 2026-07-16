# Performance

The outbox transport is Postgres rows, so its performance is governed by two
things: **round-trips** (every publish and every terminal completion is a
statement against the database) and **table churn** (every message is one
`INSERT`, one lease `UPDATE`, and one terminal `DELETE`, so dead tuples pile up at
roughly twice the message rate and autovacuum has to reclaim them). Three levers
address those. Two are automatic or opt-in code knobs; one is table hygiene you
apply once in a migration.

This page is the map: what each lever buys, which one your workload needs, and
where the mechanics live. If you already know which knob you want, jump to its
detail page from the [three levers](#the-three-levers) below.

## What each lever buys

The left column is what the [benchmark harness](#measuring-it-yourself) locks in
CI — deterministic statement counts, identical on any machine. The right column
is illustrative wall-clock from a one-off run on loopback Postgres: real, but
**machine-dependent**, so treat it as a shape, not a promise.

| Lever | CI-gated (deterministic) | Illustrative (loopback, one-off) |
|---|---|---|
| Producer NOTIFY dedup | `pg_notify` statements per bulk publish: **5000 → 1** | ~1.9× bulk-publish throughput; grows with DB round-trip latency |
| Batched terminal flush | terminal `DELETE`s: **5000 → 50** (100×) at `terminal_flush_batch_size=100` | one batched worker out-throughputs a four-worker per-row subscriber |
| Autovacuum tuning | *(no benchmark — time-and-throughput dependent)* | churn demo: throughput knob bounds bloat under sustained load |

**Gated vs illustrative.** The gated counts come from `benchmarks/baseline.json`
and fail CI if they regress, so they are guarantees about *statement count*, not
speed. The wall-clock figures were measured once and depend on your CPU, disk, and
especially network latency to Postgres — reproduce them on your own hardware
before quoting them. Autovacuum has no gated number at all: its benefit is
time-and-throughput dependent and cannot be captured as a deterministic count (see
[Autovacuum tuning](../operations/alembic.md#autovacuum-tuning-recommended) for
why).

## Which knob for which workload

| Your workload | Reach for | Why |
|---|---|---|
| Many rows to one queue in one transaction (bulk / `publish_batch`) | Nothing — **NOTIFY dedup is automatic** | The redundant `pg_notify` round-trips are already gone; delivery is unchanged. |
| High terminal throughput, **idempotent** handlers, few workers | `terminal_flush_batch_size` > 1 | Collapses per-message `DELETE`s into one per batch; reaches high throughput without spending more workers' connections. |
| Exactly-once-sensitive or low-volume queue | Leave `terminal_flush_batch_size=1` | Batching widens the crash-redelivery window — not worth it here. |
| Table bloating / vacuum can't keep up under sustained churn | `outbox_autovacuum_ddl(...)`, and raise the throughput knob | Eligibility settings fire vacuum sooner; `vacuum_cost_delay` lets it keep pace. |
| Need more parallel handler capacity | `max_workers` / `fetch_batch_size` | More concurrency — but mind the [connection budget](../usage/subscriber.md#connection-budget). |

Single-publish-per-transaction workloads see no change from NOTIFY dedup (one row
still emits one NOTIFY), and the batched-flush win is largest at low `max_workers`
(where per-row deletes serialise) and narrows as worker parallelism rises.

## The three levers

### Producer: NOTIFY dedup (automatic)

`broker.publish` / `publish_batch` used to emit one `SELECT pg_notify(...)` per
call, so N publishes to the same queue in one transaction cost N NOTIFY
round-trips — N−1 of them pure waste, since Postgres already coalesces identical
notifications per transaction at delivery. Since 0.11.0 the producer emits **one
`pg_notify` per (transaction, queue)**. It is default-on, has no knob, and is
behavior-preserving: the subscriber still gets its wake, fired inline at the first
publish. You do not configure this — it just makes bulk publishing cheaper. See
[How it works](../introduction/how-it-works.md) for the write path.

### Subscriber: batched terminal flush (opt-in)

By default each processed row is deleted with its own `DELETE` — one round-trip
per message, which is the throughput ceiling at low `max_workers`. Set
`terminal_flush_batch_size` above `1` to coalesce completed rows into one
`DELETE … RETURNING` per batch. The tradeoff is a **wider crash-redelivery
window**: on an ungraceful crash, up to a full batch of already-handled rows are
redelivered, so handlers must be idempotent. Enable it per subscriber for
high-throughput idempotent queues; leave it off otherwise. Full mechanics,
lease-ceiling sizing, and backlog-depth effects are in
[Batching terminal deletes](../usage/subscriber.md#batching-terminal-deletes).

### Storage: autovacuum tuning (apply once)

A high-churn queue table defeats Postgres' default autovacuum: the
`scale_factor = 0.2` bar is size-dependent and, on a table whose `reltuples`
estimate has gone stale, fires rarely — the classic queue-table death-spiral.
`outbox_autovacuum_ddl("outbox")` renders the `ALTER TABLE … SET (autovacuum_*)`
statement to drop into a migration; it sets `scale_factor = 0` with a constant
threshold (**eligibility** — fire on a fixed dead-tuple count, size-independent).
Under heavy sustained churn the binding constraint is instead vacuum **throughput**
— pass `vacuum_cost_delay` / `vacuum_cost_limit` to let vacuum keep pace. A
`validate_schema(check_autovacuum=True)` probe can gate that the eligibility
settings are applied. Full guidance, the eligibility-vs-throughput split, and the
I/O caution on `vacuum_cost_delay=0` are in
[Autovacuum tuning](../operations/alembic.md#autovacuum-tuning-recommended).

## Measuring it yourself

The gated numbers above come from the repository's benchmark harness. Contributors
can run it against a local Postgres:

- `just bench` — runs the producer/consumer workload sweep and prints per-message
  DB counters (split by leading SQL keyword).
- `just bench-check` — gates the deterministic counts against
  `benchmarks/baseline.json`; this is what CI runs.

The statement-count reductions (`select_calls`, `delete_calls`, `insert_calls`,
…) are machine-independent and exact. Wall-clock throughput is not — it scales
with your hardware and, most of all, round-trip latency to the database, so a
networked Postgres will show a larger absolute win from the round-trip reductions
than the loopback figures here.
