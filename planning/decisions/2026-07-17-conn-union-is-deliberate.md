---
status: accepted
summary: Keep `conn: AsyncConnection | None` on AbstractOutboxClient's fetch/terminal methods — it is the honest shared type for two adapters with genuinely different connection models, not a lie to be narrowed.
supersedes: null
superseded_by: null
---

# `conn: AsyncConnection | None` on the client seam stays a union

**Decision:** The `fetch` / `delete_with_lease` / `delete_batch_with_lease` /
`mark_pending_with_lease` methods on `AbstractOutboxClient` keep their
`conn: AsyncConnection | None` parameter. We will **not** narrow it to a non-None
`AsyncConnection`, and we will **not** replace the union with a `cast`.

## Context

The 2026-07-16 architecture review (candidate #5) flagged the `| None` as a "type
lie": the real `OutboxClient` raises `TypeError` on `None` (`client.py` fetch:193,
delete_with_lease:286, delete_batch_with_lease:379, mark_pending_with_lease:415),
while `FakeOutboxClient` ignores `conn` entirely — so the shared type admits a state
neither production path exercises. The review proposed "let the fake accept a
non-optional `conn` it declines to use."

On inspection the widening is already **deliberate and documented** in the
`AbstractOutboxClient` docstring (`client.py:69-74`), and the review's proposed fix
does not hold.

## Decision & rationale

The `None` is not a fake artifact that can be signed away on the fake's side — the
**caller genuinely produces it**:

- `conn is None ⟺ engine is None ⟺ fake client`. `_open_worker_resources` yields
  `writer_conn=None` precisely when `engine is None` (`subscriber/usecase.py:510-511`);
  the fetch loop's `fetch_conn` is `None` on the same path. In production (real
  engine) `conn` is always a live `AsyncConnection`.
- Because the subscriber hands `None` into `self._client.<method>(conn, ...)` on the
  test-broker path, narrowing the fake's *signature* alone changes nothing — the
  call site would still pass `None` to a non-None parameter.
- A `cast(AsyncConnection, None)` at the fake-path yield sites would replace an
  honest-but-broad union (`AsyncConnection | None`, truthfully "can be None", guarded
  by the real client) with a cast that actively **lies** (asserts `AsyncConnection`
  while holding `None` at runtime). Strictly worse.

The only *honest* narrowing is to move connection-acquisition **behind the seam** —
an `acquire_writer()` / `acquire_reader()` context manager on `AbstractOutboxClient`
where the real client yields a pooled autocommit `AsyncConnection` and the fake
yields its own sentinel writer, so the terminal/fetch methods take a non-None handle
(or become methods on it). That is a sizable refactor of the load-bearing worker
loop, fetch loop, both adapters, and the lease-token write path. The payoff —
removing four defensive `TypeError` guards and four call-site comments — does not
justify that risk today. The union is the honest expression of a real duality: two
adapters, one of which has no database connection.

## Revisit trigger

Reopen if **either**:

- a **third** `AbstractOutboxClient` adapter is added — the two-adapter symmetry that
  makes the shared union the natural type changes, and a handle abstraction may earn
  its keep; or
- the connection lifecycle is moved behind the seam for an **independent** reason
  (e.g. pooling / perf work on the writer or fetch connection), at which point the
  `acquire_writer()` / `acquire_reader()` handle becomes cheap to add and the union
  should be narrowed along with it.
