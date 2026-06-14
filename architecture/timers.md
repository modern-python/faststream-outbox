# Timers (delayed delivery) — implementation detail

User-facing: `docs/usage/timers.md`. Invariant summary: `CLAUDE.md` § Timers.

## How `activate_in` / `activate_at` work

`activate_in: timedelta` / `activate_at: datetime` (mutually exclusive) set `next_attempt_at` so the row is invisible to fetch until the gate opens — the `next_attempt_at <= now()` predicate in the fetch CTE is what gates eligibility, so no subscriber-side change is needed for scheduling.

- `publish` + `activate_in`: `next_attempt_at` is computed **server-side** via `now() + make_interval(secs => :s)` to stay clock-skew-safe.
- `publish` + `activate_at`: bound as the caller's **absolute literal** datetime (worker clock) — there's nothing to make skew-safe, it's an exact instant the user supplied. Note the NOTIFY future-dating decision for `activate_at` therefore compares against the worker clock, not the DB's.
- `publish_batch`: fully client-side (`datetime.now(UTC) + activate_in`, or the `activate_at` literal) because executemany doesn't compose cleanly with column-level SQL expressions. The few-ms drift is harmless for user-supplied scheduling.

## `timer_id` dedup

`timer_id` (single `publish` only) flows into a `String(255)` column with a partial unique index on `(queue, timer_id) WHERE timer_id IS NOT NULL`. The producer switches to `pg_insert(...).on_conflict_do_nothing(index_elements=[queue, timer_id], index_where=timer_id IS NOT NULL)` so re-publishing the same id is a silent no-op (returns `None`).

The dedup window is **one *live* row per `(queue, timer_id)`** — the partial unique index constrains only rows still in the outbox. Once a timer row is delivered (DELETEd) or terminally fails, a later `publish` with the same id inserts a fresh row. So `timer_id` is "at most one in flight", not a global once-ever idempotency key; the DLQ keeps `timer_id` non-unique, so audit consumers can see repeats.

## NOTIFY-skip conditions

NOTIFY is skipped when the row is **genuinely future-dated** (`activate_in > 0`, or `activate_at` resolves to a time after `now()`) OR the `on_conflict_do_nothing` path returned no row — both cases would either wake listeners that find nothing, or wake them prematurely. A past/zero `activate_at`/`activate_in` is immediately eligible, so it **does** fire NOTIFY.

## `cancel_timer` lease guard

`broker.cancel_timer(*, queue, timer_id, session)` issues `DELETE WHERE queue=? AND timer_id=? AND acquired_token IS NULL` on the caller's session — the `acquired_token IS NULL` guard is **load-bearing**: it preserves the lease-token invariant by refusing to clobber a row whose handler is already in flight (returns `False` in that case; the delivery completes normally).

## Latency floor

Timer firing latency is bounded by `max_fetch_interval` (default 10s) after `next_attempt_at` elapses. NOTIFY does not help here — listeners can't act on a future row. Sub-second precision is not a goal of this broker.
