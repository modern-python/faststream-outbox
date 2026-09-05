# faststream-outbox

A FastStream broker whose transport is a Postgres table: publishers insert rows
inside the caller's transaction, subscribers poll and lease those rows, and
LISTEN/NOTIFY short-circuits the idle wait. Postgres-only at v0.

## Language

A term is listed only when there is a synonym to reject, or a meaning subtle enough that code and
docs must agree on it. General programming vocabulary does not belong here, however heavily this
project uses it.

**Row**:
One outbox record — the unit the transport moves. There is no `state` column: a row exists until it
is delivered or terminally fails.
_Avoid_: job, task, event

**Queue**:
The `queue` column value a subscriber filters on. Not a separate object; there is nothing to declare
or create.
_Avoid_: topic, channel (reserve *channel* for the `outbox_<table>` LISTEN/NOTIFY channel)

**Lease**:
A time-bounded claim on a row, held as the `(acquired_token, acquired_at)` pair. It expires on its
own; nothing releases it.
_Avoid_: lock, reservation

**Lease token**:
The per-claim UUID in `acquired_token`. Every terminal write filters on it, so a stale writer's
statement matches nothing.
_Avoid_: lease id, owner id

**Claim**:
Acquiring a lease on a row via the fetch CTE. Increments `deliveries_count` — including on an
expired-lease reclaim.
_Avoid_: fetch (that is the loop and the client method), dequeue

**Fetch loop**:
The single per-subscriber task that runs the fetch CTE and owns the LISTEN connection.
_Avoid_: poller, reader

**Worker loop**:
One of `max_workers` per-subscriber tasks that dispatch claimed rows and write their terminal state.
_Avoid_: consumer, executor

**Drain**:
The bounded wait on stop for in-flight rows to reach a terminal write.
_Avoid_: graceful shutdown (that names the whole phase, not this wait)

**Outcome**:
The single disjoint result `dispatch_one` matches on — `Ack`, `Retry(delay_seconds)`, or
`Terminal(reason)`.
_Avoid_: status, result, state

**Retry strategy**:
The object that turns an attempt history into the next delay, or `None` for terminal.
_Avoid_: backoff policy, retry policy

**Timer**:
A row whose `next_attempt_at` gates it into the future via `activate_in` / `activate_at`.
_Avoid_: schedule, delayed message

**`timer_id`**:
The dedup key for at most one *live* row per `(queue, timer_id)`. It is not a global once-ever
idempotency key — a delivered row's id is reusable.
_Avoid_: idempotency key, message key

**Relay**:
Forwarding a delivered row onward through a native FastStream publisher chain to a foreign broker.
_Avoid_: bridge, forwarder

**DLQ**:
The opt-in sibling audit table terminal failures are archived to. Off by default.
_Avoid_: dead-letter queue as a transport — nothing consumes it

**Recorder seam**:
The `MetricsRecorder` callable the library invokes for events outside the FastStream bus.
Complementary to the native middleware seam, never a replacement for it.
_Avoid_: metrics hook, callback

**Envelope**:
The `(payload_bytes, headers)` encoding of a body, plus the header keys the library manages
(`content-type`, `correlation_id`).
_Avoid_: message (that is the FastStream object), wrapper
