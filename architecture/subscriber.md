# Two-loop subscriber + lease-token invariant — implementation detail

User-facing docs do not cover this directly. Invariant summary: `CLAUDE.md` § Subscriber.

## The two loops

The subscriber runtime lives in `subscriber/usecase.py`. Per subscriber, two kinds of async task run:

**1. `_fetch_loop`** — owns a long-lived `AsyncConnection` for the fetch CTE plus a separate raw asyncpg connection for `LISTEN outbox_<table>`. It runs a single CTE:

```
SELECT … FOR UPDATE SKIP LOCKED → UPDATE acquired_token=:uuid, acquired_at=now() RETURNING *
```

The `WHERE` clause reclaims both unleased rows and expired leases (`acquired_at < now() - make_interval(secs => :lease_ttl)`) — so there is no separate reaper. A `NOTIFY` short-circuits the idle sleep via an `asyncio.Event`, dropping idle latency from `max_fetch_interval` to ~10ms. `LISTEN` failures log once and fall back to polling. On any DB error the connections close, the loop backs off exponentially (`_BACKOFF_EXP_CAP=30`), and reopens.

**2. `_worker_loop`** — one per `max_workers`. Each pulls from an `asyncio.Queue(maxsize=fetch_batch_size)`, dispatches via `consume()`, and flushes the terminal state. Each worker owns a long-lived `AsyncConnection` (held across reconnect) and routes terminal writes through `delete_with_lease(conn, …)` / `mark_pending_with_lease(conn, …)`, so a drain of N rows costs O(workers) pool checkouts. Flush exceptions propagate (the outer loop rebuilds the connection); the inflight slot still releases in `finally`.

The default ack policy is `AckPolicy.NACK_ON_ERROR`; `REJECT_ON_ERROR` and `MANUAL` are allowed. `AckPolicy.ACK_FIRST` is rejected at registration with `ValueError` — it would delete before the handler runs, defeating the outbox contract. `subscriber/factory.py` raises or warns on other footguns (`lease_ttl_seconds <= max_fetch_interval`, `max_deliveries` without retry, etc.).

`OutboxSubscriber.get_one()` and `__aiter__()` are explicit `NotImplementedError`s — they point operators at `broker.fetch_unprocessed(session=..., queue=...)`. A peek that acquires a lease has surprising `deliveries_count` semantics; lease-free reads belong on `fetch_unprocessed`.

## Lease bound

A subscriber can hold up to `fetch_batch_size + max_workers` leases at once, not `fetch_batch_size` (F1-01). The free-slot computation `free = _inflight.maxsize - qsize()` counts only queued rows, so once `max_workers` rows are checked out for processing, the loop can claim another full batch. Leases are bounded and self-expire via TTL, but when sizing `lease_ttl_seconds` and reasoning about cross-replica contention, reason against `fetch_batch_size + max_workers`, not `fetch_batch_size`.

## Connection budget

Each subscriber holds `max_workers + 1` SQLAlchemy pool connections steady-state, plus one raw asyncpg connection for `LISTEN`.

- Size the pool for `Σ subscribers × (max_workers + 1)`. Undersizing does **not** block `broker.start()` — `start()` only schedules the loop tasks via `add_task` and returns; instead the fetch/worker loops stall on pool checkout at runtime. The asyncpg `LISTEN` connection lives outside the pool, so it does not count toward pool sizing.
- Per process, Postgres `max_connections` must cover `replicas × Σ subscribers × (max_workers + 2)`: the `max_workers + 1` pool connections plus the out-of-pool asyncpg `LISTEN` connection. Undersize it and rolling deploys hit `FATAL: too many connections`.

## NOTIFY semantics

`broker.publish` / `publish_batch` emit `SELECT pg_notify('outbox_<table>', queue)` on the caller's session right after the `INSERT` — except when a future-dated insert or a `timer_id` conflict no-op'd the insert.

`NOTIFY` is transactional, so atomicity with the row is automatic; rolled-back transactions silently drop it. The future-dated decision is one shared `is_future_dated(activate_in, activate_at, now)` in the stdlib-only leaf `_scheduling.py`, alongside `resolve_next_attempt_client_side` and `validate_activate_args`. `activate_at`'s comparison and the `publish_batch` `next_attempt_at` are worker-clock-relative (unlike `activate_in`'s server-side `make_interval`), so under worker/DB clock skew `NOTIFY` may fire slightly early or late — polling backstops it (F2-04 / F2-05).

`NOTIFY`s emitted during a fetch-loop reconnect/backoff window are lost (`LISTEN` is not durable); latency degrades to the poll interval until the next tick — a latency, not a correctness, gap (F1-07).

Channel naming is `outbox_<table_name>`. Postgres limits identifiers to 63 bytes; `make_outbox_table` raises `ValueError` when the longest derived identifier — an index name like `<table>_pending_idx` — would exceed it, so over-long table names (~>51 bytes) are rejected at construction, not silently degraded to polling.

## The lease-token invariant

Load-bearing. Every terminal write (`delete_with_lease`, `mark_pending_with_lease`) filters on `acquired_token`. If a slow handler's lease expired and a newer fetch reclaimed the row, the slow handler's `DELETE`/`UPDATE` finds `rowcount == 0` and is silently dropped — preventing it from clobbering the new lease holder. Any new fetch or terminal path must preserve this.

Lease-loss logs at `WARNING` with `extra={"event": "lease_lost", "phase": "terminal"|"retry", "row_id": …, "queue": …, "deliveries_count": …}`. Recurring `event=lease_lost` means `lease_ttl_seconds < handler P99`.

## lease_ttl sizing

`lease_ttl_seconds` (default `60.0`, per-subscriber) must exceed handler P99 with margin or healthy handlers race their own expiry. The lease cutoff uses server-side `make_interval(secs => :lease_ttl)`, so it is immune to clock skew.

Sizing tip: route occasional slow work onto its own subscriber with a tall TTL; keep the fast subscriber's tight. TTL is per-subscriber.

## deliveries_count vs attempts_count

`deliveries_count` counts claims, not completed handler runs — the fetch CTE increments it on every claim, including expired-lease reclaims (F2-07). Under lease churn a row can cross `max_deliveries` after fewer than N successful handler invocations, so set `max_deliveries` with margin.

`attempts_count` (via `_record_attempt`) is the handler-run-scoped counter.

## Writer-connection autocommit

`_open_worker_resources` sets the per-worker writer to `isolation_level="AUTOCOMMIT"`. Terminal writes are single statements; an explicit `BEGIN`/`COMMIT` would add two round-trips per row. The `WHERE acquired_token = …` clause enforces the invariant, not the transaction wrapping.

The fetch connection is not autocommit — it owns `LISTEN`/`NOTIFY` and amortizes `BEGIN`/`COMMIT` across the batch.

## Shutdown race in dispatch_one

If `stop()` flips `running=False` between a worker pulling a row from `_inflight` and entering `consume()`, base `SubscriberUsecase.consume()` early-exits without running the handler. `dispatch_one` detects this (`not row.state_set and not self.running` after `consume()` returns without raising) and returns before `assert_state_set → reject() → _safe_flush` would silently `DELETE`. The lease lives until expiry; another replica reclaims the row. No metric fires. Without this guard, busy subscribers leak rows on every rolling deploy.

See also [`architecture/drain.md`](drain.md) for the drain-phase interaction.
