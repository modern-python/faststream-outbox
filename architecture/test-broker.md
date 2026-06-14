# Test broker — implementation detail

User-facing: `docs/usage/testing.md`. Invariant summary: `CLAUDE.md` § Test broker.

## `FakeOutboxClient` + two dispatch modes

`TestOutboxBroker` (`testing.py`) swaps in a `FakeOutboxClient` (in-memory list of `_FakeRow` dicts).

### Sync (default, `run_loops=False`)

`broker.publish` synchronously routes through `OutboxSubscriber.dispatch_one` — matches the FastStream test-broker idiom (`TestKafkaBroker` / `TestRabbitBroker`). The handler runs before `publish` returns; no background loops. `broker.publish_batch`, `cancel_timer`, and `fetch_unprocessed` are also patched to operate on the fake client (the `session` argument is **ignored** — `del session` — which diverges from production's `isinstance(session, AsyncSession)` `TypeError`; tests needing the session contract must use a real `OutboxClient`). `OutboxResponse` is *not* faked, so its eager session/queue/activate validation still fires under the test broker.

`_sync_dispatch` claims the just-fed row via the shared `_claim_fake_row` (the same lease + `deliveries_count++` mechanics `FakeOutboxClient.fetch` uses), so the `max_deliveries` boundary runs on one path; only the eligibility gate (`next_attempt_at <= now`) lives in `fetch`, which is why sync mode fires future-dated rows immediately.

The broker's `producer` slot is swapped for `FakeOutboxProducer` (`testing.py`) so `publisher.publish()` lands rows in the same fake store via the FastStream `_basic_publish` flow — tests using `broker.publisher("q").publish(...)` work identically to `broker.publish(queue="q", ...)`.

Future-dated rows (`activate_in` / `activate_at`) **fire immediately** in sync mode — sync dispatch ignores `next_attempt_at`. This trades production parity for test ergonomics: tests can assert handler effects without time travel. `next_attempt_at` is still recorded on the fake row for inspection. Use `run_loops=True` if you need scheduled delivery to actually wait.

### Loop (`run_loops=True`)

Spins up the real `_fetch_loop` / `_worker_loop` against the fake client. Required for tests that exercise retry rescheduling, lease-expiry reclaim, fetch-loop error recovery, or scheduled delivery firing. Subscribers without registered handlers are skipped in `_fake_start` (mirrors `OutboxSubscriber.start`'s `if not self.calls: return`).

## `dispatch_one` is the public entry

`OutboxSubscriber.dispatch_one(row)` is the public per-row dispatch entry point. The worker loop calls it; the test broker calls it directly. Caller must have already acquired the row's lease.

## `FakeOutboxClient.validate_schema()` raises

`FakeOutboxClient.validate_schema()` raises `NotImplementedError` — there is no real DB to validate against, and a silent pass would let users ship broken schemas while their `TestOutboxBroker`-backed tests stay green. Tests that need real schema validation must construct an `OutboxClient(real_engine, table)` against the same DSN the migrations ran against.

## `_fake_start` skips the parent publisher-iteration loop

`TestOutboxBroker._fake_start` deliberately **skips the parent's publisher-iteration loop** (the one that calls `create_publisher_fake_subscriber`). Reason: FastStream's publisher-spy infrastructure mocks the registered handler to forward `publisher.publish()` calls — which conflicts with the outbox's real dispatch path (the fake producer already lands rows in the fake client *and* drives the real handler via `_sync_dispatch`).

The required abstract `create_publisher_fake_subscriber` is therefore implemented as `raise NotImplementedError(...)` — unreachable in normal use. If you ever need FastStream's publisher mock for outbox tests, swap that override out before re-using the parent's `_fake_start`.

## Router subscriber gotcha

Subscribers registered via `OutboxRouter` (then `broker.include_router(router)`) live on the router, not on `broker._subscribers`. Walk `broker.subscribers` (the property) — it iterates `[*self._subscribers, *(s for r in self.routers for s in r.subscribers)]` — when you need to introspect every subscriber.
