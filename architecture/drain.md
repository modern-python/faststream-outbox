# Drain on stop — implementation detail

User-facing docs do not cover this directly. Invariant summary: `CLAUDE.md` § Drain on stop.

`OutboxSubscriber.stop()` and `OutboxBroker.stop()` both override FastStream parents to remove regressions that the naive composition would introduce. Both upstream bodies are trivial and the divergence is documented in the override comments (`# Upstream equivalent (replaced): …`).

## Subscriber: `_stopping` flag + strict-bound drain

The subscriber carries two flags during shutdown: `self.running` (FastStream's existing "actively dispatching" signal) and `self._stopping` (new, "stop was requested; no new claims").

**Why two flags.** Flipping `running=False` first would defeat drain via `SubscriberUsecase.consume()`'s early-exit (every queued row's handler would be skipped — the `dispatch_one` shutdown guard would preserve the row, but the drain would do nothing). So `running` stays True for the duration of drain.

- `_fetch_inner`'s loop guard checks both: `while self.running and not self._stopping:`
- The worker loop only checks `running`.
- `stop()` flips `_stopping`, kicks the fetch loop awake via `_notify_event` (in case it's parked in an idle `_wait_for_notify_or_timeout`), waits up to `graceful_timeout` for `_inflight.join()`, then flips `running=False` and cancels the spawned tasks.
- **A timed-out drain is observable.** If `_inflight.join()` doesn't finish within the budget (`move_on_after`'s `cancelled_caught`), `stop()` emits a `WARNING` and a `drain_timeout` recorder metric (`faststream_outbox_drain_timeout_total` / `messaging.outbox.drain_timeout`) before cancelling — abandoned in-flight rows are left to lease-expiry retry, and an operator can now tell a timed-out drain from a clean one.
- **`graceful_timeout=None`** stays unbounded where FastStream uses it that way (e.g. `ping()`), but the drain wait clamps `None` to a finite fallback (`_DEFAULT_DRAIN_TIMEOUT_SECONDS = 15.0`). `anyio.move_on_after(None)` has deadline `inf`, so without the clamp a single wedged handler would make `_inflight.join()` — and thus `stop()` — never return.

**Batched terminal flush + the drain barrier.** With `terminal_flush_batch_size > 1`, a batchable row's terminal `DELETE` is deferred to a batch flush, so its `task_done()` is deferred with it — called in `_flush_buffer`, not right after `dispatch_one`. This keeps `_inflight.join()` an honest drain barrier: `join()` returns only once every buffered row's `DELETE` has actually landed (or its lease was found lost), never merely because dispatch finished. A **graceful** `stop()` still empties the buffer — the worker loop's `finally` best-effort-flushes any completed-but-unflushed rows before exiting, so cleanly-drained buffered rows are deleted and do not redeliver. A **timed-out** drain abandons buffered rows exactly like inline ones: their leases expire and another replica reclaims them. `_flush_buffer` `task_done()`s every buffered row even when the batch `DELETE` raises (then re-raises), so a flush error can never leave `join()` hanging.

**Why we skip `super().stop()`.** Its `MultiLock.wait_release(graceful_timeout)` would either return instantly (healthy path; `_inflight.join()` already waited a stricter condition) or re-wait the same stuck handlers for another full budget (wedged path; **2× shutdown regression**). The subscriber inlines `TasksMixin.stop`'s cleanup body instead. Per-subscriber shutdown bound: `graceful_timeout`.

## Broker: parallel-gather subscriber stop

`OutboxBroker.stop` overrides `BrokerUsecase.stop`'s sequential `for sub in subscribers: await sub.stop()` with `asyncio.gather(*(sub.stop() for sub in subscribers), return_exceptions=True)`.

**Why.** Sequential N × `graceful_timeout` exceeds K8s default `terminationGracePeriodSeconds=30s` once a service has 2+ subscribers at the default 15s budget. Gather collapses total shutdown to ≈ `max(per-sub) ≈ graceful_timeout` regardless of N.

`return_exceptions=True` (not `TaskGroup`) so a stuck subscriber doesn't cancel the others mid-drain. Exception results are logged via `_log_subscriber_stop_error` and never re-raised — shutdown must complete even when individual subscribers misbehave.

`OutboxBroker.stop` sets `self.running = False` **before** the gather (shutdown is irreversible at that point), so an external cancellation of `stop()` mid-gather can't leave the broker advertising `running=True` over already-stopped subscribers (which `ping()` reads).

## Phase interaction with `dispatch_one` guard

During drain `self.running` stays True, so the `dispatch_one` guard (`not row.state_set and not self.running`) is dormant. After drain completes (or times out), `stop()` sets `running=False` before `task.cancel()`; any worker mid-`dispatch_one` at that instant then benefits from the guard against the silent-DELETE race.

The two changes are complementary — the `dispatch_one` guard covers correctness when drain times out or workers are cancelled; drain covers latency for healthy shutdown.

## Upstream divergence flag

Both overrides replace upstream FastStream methods. Stable for years upstream, but if FastStream adds new cleanup to `BrokerUsecase.stop`, `SubscriberUsecase.stop`, or `TasksMixin.stop`, we silently miss it. **Reviewers touching shutdown must re-check both overrides.** Regression tests pin both behaviors:

- `tests/test_fake.py::test_drain_finishes_inflight_rows_before_returning_in_fake_mode` — drain waits for in-flight rows (off-Postgres)
- `tests/test_fake.py::test_broker_stop_cancels_wedged_handler_within_graceful_timeout_in_fake_mode` — graceful-timeout bound (off-Postgres)
- `tests/test_fake.py::test_drain_timeout_emits_warning_and_metric_in_fake_mode` — timed-out drain surfaces a WARNING + `drain_timeout` metric (off-Postgres)
- `tests/test_integration.py` — the Postgres-backed drain + parallel-gather coverage

## Test-broker gotcha

`TestOutboxBroker._fake_close` (`testing.py`) directly sets `sub.running = False` and bypasses `subscriber.stop()` / `broker.stop()` entirely. Existing `run_loops=True` tests are unaffected; drain tests must explicitly `await broker.stop()` inside the `async with` block to exercise the drain code paths.
