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
its id. `deliveries_count` is not reset.

Once your transaction commits, the timer will not fire before the deadline
you asked for. Until the commit, workers skip the locked row; after it, they
see the new deadline.

### Extending a timer that is already running

When a worker has claimed the timer, its handler runs for the deadline it
claimed, and the extend leaves that run alone. It queues a rearm on the row
instead and still returns the row id. When the run ends, the rearm turns the
row back into a pending timer with the queued deadline, payload and headers,
so every accepted heartbeat leads to a firing at or after its deadline:

- If the handler succeeds, the timer starts over at the rearm deadline, with
  its retry history and `deliveries_count` cleared.
- If the handler fails and would be retried, the rearm replaces the retry.
  `deliveries_count` is kept, so `max_deliveries` still stops a handler that
  fails on every run.
- If the handler fails terminally, the failed run goes to the DLQ (when one
  is configured) and the timer starts over at the rearm deadline.
- If the worker dies, the expired lease is reclaimed and the run is
  delivered again as usual; the rearm applies when that run ends.

Queued rearms follow the same rule as pending timers: a rearm only moves
later, and an extend whose deadline is not later than the running deadline
queues nothing.

A handler can rearm its own timer the same way, by publishing its own
`timer_id` with `timer_conflict="extend"`, directly or through an
`OutboxResponse`.

The rearm doesn't undo what the running handler does. If a heartbeat must
win over an expiry that is already running, have the handler re-check, in
the same transaction as its side effects, whether the timeout still applies
(for example against a last-activity column the heartbeat writes). The
rearm then fires the handler again for the newer deadline.

A rearm that becomes due when the run ends is picked up by polling, within
the subscriber's `max_fetch_interval`. No NOTIFY is sent for it.

Every extend writes a new row version, even when the deadline does not
move. `next_attempt_at` is indexed, so these updates are not HOT updates:
each one adds index entries and a dead tuple for autovacuum to clean up.
Size autovacuum for your heartbeat rate (see
[Alembic migrations](../operations/alembic.md), `outbox_autovacuum_ddl`).

## Cancellation

`broker.cancel_timer(*, queue, timer_id, session)` cancels a timer on the
caller's session:

```python
cancelled = await broker.cancel_timer(
    queue="orders",
    timer_id="order-confirm-42",
    session=session,
)
# True if a pending timer or a queued rearm was dropped.
```

It locks the `(queue, timer_id)` row first. An unleased row is deleted. A
leased row is never deleted, because its handler is already running: the
cancel only drops a queued rearm, so the timer does not come back after the
run, and returns `False` when there was none. If the timer fired in the race
window between your application logic deciding to cancel and the cancel
landing, the delivery completes normally.

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

A rearm applied when a run ends is not dispatched again in sync mode, so a
handler that rearms its own timer runs once per publish. The rearmed row
stays in `tb.fake_client.rows`.
