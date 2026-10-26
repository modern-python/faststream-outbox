# Timers

Schedule an outbox row to fire later by passing exactly one of
`activate_in` (relative) or `activate_at` (absolute, tz-aware). Pass `timer_id` to
deduplicate per `(queue, timer_id)`; cancel a not-yet-leased timer with
`broker.cancel_timer(...)`.

## Scheduling

```python
import datetime as dt


# Fire 30 seconds from now, deduplicated by timer_id:
order_id = 1
await broker.publish(
    {"order_id": order_id},
    queue="orders",
    session=session,
    activate_in=dt.timedelta(seconds=30),
    timer_id=f"order-confirm-{order_id}",
)

# Fire at a specific UTC instant:
await broker.publish(
    {"x": 1},
    queue="orders",
    session=session,
    activate_at=dt.datetime(2027, 6, 1, 9, tzinfo=dt.UTC),
)
```

`publish` returns the inserted row's `id`, or `None` if a row with the
same `(queue, timer_id)` already exists.

### Mutually exclusive

Passing both `activate_in` and `activate_at` raises `ValueError`. They are
two ways to say the same thing: "make this row invisible to fetch until
the given moment".

### Timezone-aware `activate_at`

`activate_at` must be timezone-aware. A naive `datetime` raises an
explicit `ValueError` rather than guessing your intended zone.

### Server-side vs client-side scheduling

For `publish` with `activate_in`, `next_attempt_at` is computed server-side
via `now() + make_interval(secs => :s)` to stay clock-skew-safe. With
`activate_at`, you supply an absolute instant, so it is stored verbatim and
compared against the *worker's* clock at fetch time. Only `activate_in` is
skew-safe; `activate_at` is as accurate as your producers' and workers'
clocks agree. For `publish_batch`, `activate_in` is also client-side
(`datetime.now(UTC) + activate_in`) because executemany doesn't compose
cleanly with column-level SQL expressions, and the few-ms drift is harmless
for user-supplied scheduling.

## Deduplication with `timer_id`

`timer_id` flows into a `String(255)` column with a partial unique index
on `(queue, timer_id) WHERE timer_id IS NOT NULL`. The insert switches
to `pg_insert(...).on_conflict_do_nothing(...)` so re-publishing the same
id is a silent no-op (returns `None`):

```python
first = await broker.publish(
    {"order_id": 1},
    queue="orders",
    session=session,
    activate_in=dt.timedelta(seconds=30),
    timer_id="order-confirm-1",
)
assert first is not None

# Re-publish — no row inserted, no NOTIFY emitted, returns None.
second = await broker.publish(
    {"order_id": 1},
    queue="orders",
    session=session,
    activate_in=dt.timedelta(seconds=30),
    timer_id="order-confirm-1",
)
assert second is None
```

NOTIFY is skipped when the row is genuinely future-dated (a *future*
`activate_in` / `activate_at`) or the conflict path returned no row. In both cases a NOTIFY would either
wake listeners that find nothing or wake them prematurely. A *past* `activate_at` is already eligible, so it still
notifies.

`timer_id` is only available on single `publish`, not on `publish_batch`
(per-row dedup makes no sense for a batch).

### `timer_id` dedups only *live* rows

The unique index is partial: `(queue, timer_id) WHERE timer_id IS NOT
NULL`. It constrains only rows currently in the table. Once a timer fires
(the row is deleted) or is cancelled, the same `timer_id` can be inserted
fresh. So `timer_id` is a dedup key for in-flight / pending timers, not
a permanent idempotency key: it won't stop a value from being re-published
after the original delivery has completed.

## Extending a timer

Pass `timer_conflict="extend"` to push a pending timer's deadline later
instead of leaving it alone. A heartbeat that keeps a per-operator timeout
alive looks like this:

```python
rid = await broker.publish(
    {"operator_id": operator_id},
    queue="operator-timeouts",
    session=session,
    activate_in=dt.timedelta(minutes=10),
    timer_id=f"operator-{operator_id}",
    timer_conflict="extend",
)
```

The first call inserts the timer. Later calls update the same row in one
`INSERT ... ON CONFLICT DO UPDATE` statement, so the row id stays the same and
there is never more than one live row per `(queue, timer_id)`. The default,
`timer_conflict="ignore"`, keeps the behavior described above.

The deadline only moves later. An extend sets `next_attempt_at` to the later
of the stored deadline and the requested one, so heartbeats that commit out
of order cannot pull a timer in. When the requested deadline is later, the
row also takes the new payload and headers, and its retry history
(`attempts_count`, `first_attempt_at`, `last_attempt_at`) is cleared. When
it is not later, the row keeps everything it had and the call still returns
its id. `deliveries_count` is never reset.

An extend never touches a row a worker has leased, and returns `None` for
it.

- An id means that once your transaction commits, the timer will not fire
  before the deadline you asked for. Until the commit, workers skip the
  locked row; after it, they see the new deadline.
- `None` means a worker claimed the timer first. Its handler is running, or
  will run when an expired lease is reclaimed, for the deadline it claimed.
  Publishing again does not help while that row exists.

If a late heartbeat must win over an expiry that is already running, the
library cannot decide that for you. Have the handler re-check, in the same
transaction as its side effects, whether the timeout still applies (for
example against a last-activity column the heartbeat writes), and publish a
fresh timer from the heartbeat once the old row is gone.

Every extend writes a new row version, even when the deadline does not
move. `next_attempt_at` is indexed, so these updates are not HOT updates:
each one adds index entries and a dead tuple for autovacuum to clean up.
Size autovacuum for your heartbeat rate (see
[Alembic migrations](../operations/alembic.md), `outbox_autovacuum_ddl`).

## Cancellation

`broker.cancel_timer(*, queue, timer_id, session)` issues a `DELETE` on
the caller's session, but only if the row is not yet leased:

```python
deleted = await broker.cancel_timer(
    queue="orders",
    timer_id="order-confirm-42",
    session=session,
)
# True if a row was deleted; False if it didn't exist or was already in flight.
```

The underlying SQL is `DELETE WHERE queue=? AND timer_id=? AND
acquired_token IS NULL`. The `acquired_token IS NULL` guard is
load-bearing: it preserves the lease-token invariant by refusing to
clobber a row whose handler is already running. If the timer fired in the
race window between your application logic deciding to cancel and the
`DELETE` landing, the delivery completes normally and `cancel_timer`
returns `False`.

## Latency floor

Timer firing latency is bounded by the subscriber's `max_fetch_interval`
(default `10` seconds) after `next_attempt_at` elapses. NOTIFY does not
help here: listeners can't act on a future row, so the fetch loop has to
poll for it.

Lower `max_fetch_interval` for sub-10s precision. Sub-second precision is
not a goal of this broker; if you need it, schedule the row early and
sleep inside the handler, or use a different scheduler.

## Test broker note

In tests using `TestOutboxBroker` (default `run_loops=False` mode),
`activate_in` / `activate_at` are ignored and timers fire immediately,
because sync dispatch ignores `next_attempt_at`. This trades production parity
for test ergonomics: tests can assert handler effects without time travel.

The schedule is still recorded on the fake row. If a test needs to assert
on it, bind the `TestOutboxBroker` to a name and read
`tb.fake_client.rows[0].next_attempt_at`.
Pass `run_loops=True` if you need scheduled delivery to actually
wait. See [Testing](./testing.md).
