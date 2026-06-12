# Timers

Schedule an outbox row to fire later by passing `activate_in` (relative)
or `activate_at` (absolute, tz-aware) — exactly one. Pass `timer_id` to
deduplicate per `(queue, timer_id)`; cancel a not-yet-leased timer with
`broker.cancel_timer(...)`.

## Scheduling

```python
import datetime as dt


# Fire 30 seconds from now, deduplicated by timer_id:
await broker.publish(
    {"order_id": 1},
    queue="orders",
    session=session,
    activate_in=dt.timedelta(seconds=30),
    timer_id=f"order-confirm-{order.id}",
)

# Fire at a specific UTC instant:
await broker.publish(
    {"x": 1}, queue="orders", session=session,
    activate_at=dt.datetime(2026, 6, 1, 9, tzinfo=dt.UTC),
)
```

`publish` returns the inserted row's `id`, or `None` if a row with the
same `(queue, timer_id)` already exists.

### Mutually exclusive

Passing both `activate_in` and `activate_at` raises `ValueError`. They are
two ways to say the same thing — "make this row invisible to fetch until
the given moment".

### Timezone-aware `activate_at`

`activate_at` must be timezone-aware. A naive `datetime` raises an
explicit `ValueError` rather than guessing your intended zone.

### Server-side vs client-side scheduling

For `publish`, `next_attempt_at` is computed server-side via `now() +
make_interval(secs => :s)` to stay clock-skew-safe. For `publish_batch`,
it's client-side (`datetime.now(UTC) + activate_in`) because executemany
doesn't compose cleanly with column-level SQL expressions, and the few-ms
drift is harmless for user-supplied scheduling.

## Deduplication with `timer_id`

`timer_id` flows into a `String(255)` column with a partial unique index
on `(queue, timer_id) WHERE timer_id IS NOT NULL`. The producer switches
to `pg_insert(...).on_conflict_do_nothing(...)` so re-publishing the same
id is a silent no-op (returns `None`):

```python
first = await broker.publish(
    {"order_id": 1}, queue="orders", session=session,
    activate_in=dt.timedelta(seconds=30),
    timer_id="order-confirm-1",
)
assert first is not None

# Re-publish — no row inserted, no NOTIFY emitted, returns None.
second = await broker.publish(
    {"order_id": 1}, queue="orders", session=session,
    activate_in=dt.timedelta(seconds=30),
    timer_id="order-confirm-1",
)
assert second is None
```

NOTIFY is skipped when the row is genuinely future-dated (a *future*
`activate_in` / `activate_at`) OR the conflict path returned no row — both
cases would either wake listeners that find nothing, or wake them
prematurely. A *past* `activate_at` is already eligible, so it still
notifies.

`timer_id` is only available on single `publish`, not on `publish_batch`
(per-row dedup makes no sense for a batch).

## Cancellation

`broker.cancel_timer(*, queue, timer_id, session)` issues a `DELETE` on
the caller's session, but only if the row is **not yet leased**:

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
help here — listeners can't act on a future row, so the fetch loop has to
poll for it.

Lower `max_fetch_interval` for sub-10s precision. Sub-second precision is
not a goal of this broker; if you need it, schedule the row early and
sleep inside the handler, or use a different scheduler.

## Test broker note

In tests using `TestOutboxBroker` (default `run_loops=False` mode),
`activate_in` / `activate_at` are **ignored** and timers fire immediately
— sync dispatch ignores `next_attempt_at`. This trades production parity
for test ergonomics: tests can assert handler effects without time travel.

The schedule is still recorded on the fake row — bind the
`TestOutboxBroker` to a name and read
`tb.fake_client.rows[0].next_attempt_at` — if a test needs to assert on it.
Pass `run_loops=True` if you need scheduled delivery to actually
wait. See [Testing](./testing.md).
