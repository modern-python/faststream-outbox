# Test broker — implementation detail

User-facing: `docs/usage/testing.md`. Invariant summary: `CLAUDE.md` § Test broker.

## `FakeOutboxClient` + two dispatch modes

`TestOutboxBroker` (`testing.py`) swaps in a `FakeOutboxClient` (in-memory list of `_FakeRow` dicts).

### Sync (default, `run_loops=False`)

`broker.publish` synchronously routes through `OutboxSubscriber.dispatch_one` — matches the FastStream test-broker idiom (`TestKafkaBroker` / `TestRabbitBroker`). The handler runs before `publish` returns; no background loops. `broker.publish_batch` is also patched to operate on the fake client. `cancel_timer` and `fetch_unprocessed` are **not** patched — they are methods on `AbstractOutboxClient`, so `broker.<method>` delegates through the `FakeOutboxClient` the test broker swaps in as `broker.config.broker_config.client`; the fake requires the `session` argument (matching the client signature) but **ignores** it — `del session` — which diverges from production's `isinstance(session, AsyncSession)` `TypeError`; tests needing the session contract must use a real `OutboxClient`). `OutboxResponse` is *not* faked, so its eager session/queue/activate validation still fires under the test broker.

`_sync_dispatch` claims the just-fed row via the shared `_claim_fake_row` (the same lease + `deliveries_count++` mechanics `FakeOutboxClient.fetch` uses), so the `max_deliveries` boundary runs on one path; only the eligibility gate (`next_attempt_at <= now`) lives in `fetch`, which is why sync mode fires future-dated rows immediately.

The broker's `producer` slot is swapped for `FakeOutboxProducer` (`testing.py`) so `publisher.publish()` lands rows in the same fake store via the FastStream `_basic_publish` flow — tests using `broker.publisher("q").publish(...)` work identically to `broker.publish(queue="q", ...)`.

Future-dated rows (`activate_in` / `activate_at`) **fire immediately** in sync mode — sync dispatch ignores `next_attempt_at`. This trades production parity for test ergonomics: tests can assert handler effects without time travel. `next_attempt_at` is still recorded on the fake row for inspection. Use `run_loops=True` if you need scheduled delivery to actually wait.

### Loop (`run_loops=True`)

Spins up the real `_fetch_loop` / `_worker_loop` against the fake client. Required for tests that exercise retry rescheduling, lease-expiry reclaim, fetch-loop error recovery, or scheduled delivery firing. Subscribers without registered handlers are skipped in `_fake_start` (mirrors `OutboxSubscriber.start`'s `if not self.calls: return`).

## `dispatch_one` is the public entry

`OutboxSubscriber.dispatch_one(row)` is the public per-row dispatch entry point. The worker loop calls it; the test broker calls it directly. Caller must have already acquired the row's lease.

## `FakeOutboxClient.validate_schema()` raises

`FakeOutboxClient.validate_schema()` raises `NotImplementedError` — there is no real DB to validate against, and a silent pass would let users ship broken schemas while their `TestOutboxBroker`-backed tests stay green. Tests that need real schema validation must construct an `OutboxClient(real_engine, table)` against the same DSN the migrations ran against.

## The client contract keeps the fake honest

`FakeOutboxClient` re-implements the outbox rules in Python because there is no in-process Postgres — eligibility, lease cutoff, retry timing, the NULL-token guard, and the DLQ projection all exist twice (SQL in `OutboxClient`, Python here). The two can't share an implementation (one runs in the database, one in the process), so `tests/test_client_contract.py` couples them by **behaviour** instead: one parametrized scenario module asserts the shared `AbstractOutboxClient` surface (`fetch` / `delete_with_lease` / `mark_pending_with_lease` + DLQ) against *both* adapters — the fake everywhere, real Postgres auto-skipped when unreachable. A per-adapter harness hides substrate differences (how a row is seeded, which connection a terminal write needs); scheduling is seeded as server-side `make_interval` offsets so the comparison is clock-skew-free; expectations are hand-specified so neither adapter passes trivially against itself.

What it pins is *structural* drift (eligibility states, FIFO selection under contention, the token guard, the DLQ projection). What it deliberately cannot pin: cross-host DB-vs-worker clock skew — an in-process test can't manufacture it — so the real client's server-side clock authority stays a documented invariant. `timer_id` insert-dedup is a broker/producer concern (not on the client interface). `cancel_timer` and `fetch_unprocessed` are now on the client interface, but their cross-adapter parity is covered by `test_integration.py` / `test_fake.py` rather than the parametrized contract suite (which pins the fetch/terminal/DLQ surface); folding them into the contract suite is a possible future tightening. The pure helpers that *can* be shared — the DLQ projection (`_DLQ_PROJECTION` in `schema.py`) and the activate-args resolution (`_scheduling.py`) — are extracted so the fake consumes the same code as production rather than a parallel copy.

## `_fake_start` skips the parent publisher-iteration loop

`TestOutboxBroker._fake_start` deliberately **skips the parent's publisher-iteration loop** (the one that calls `create_publisher_fake_subscriber`). Reason: FastStream's publisher-spy infrastructure mocks the registered handler to forward `publisher.publish()` calls — which conflicts with the outbox's real dispatch path (the fake producer already lands rows in the fake client *and* drives the real handler via `_sync_dispatch`).

The required abstract `create_publisher_fake_subscriber` is therefore implemented as `raise NotImplementedError(...)` — unreachable in normal use. If you ever need FastStream's publisher mock for outbox tests, swap that override out before re-using the parent's `_fake_start`.

## Router subscriber gotcha

Subscribers registered via `OutboxRouter` (then `broker.include_router(router)`) live on the router, not on `broker._subscribers`. Walk `broker.subscribers` (the property) — it iterates `[*self._subscribers, *(s for r in self.routers for s in r.subscribers)]` — when you need to introspect every subscriber.
