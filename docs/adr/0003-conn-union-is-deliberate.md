# `conn: AsyncConnection | None` on the client seam stays a union

**Decision:** `fetch` / `delete_with_lease` / `delete_batch_with_lease` / `mark_pending_with_lease`
on `AbstractOutboxClient` keep their `conn: AsyncConnection | None` parameter. It is not narrowed to
a non-`None` `AsyncConnection`, and the union is not replaced with a `cast`.

The 2026-07-16 architecture review flagged the `| None` as a type lie: the real `OutboxClient` raises
`TypeError` on `None` while `FakeOutboxClient` ignores `conn` entirely, so the shared type admits a
state neither production path exercises. It does not hold up. The `None` is not a fake artifact that
can be signed away on the fake's side — the caller genuinely produces it. `conn is None ⟺ engine is
None ⟺ fake client`: `_open_worker_resources` yields `writer_conn=None` precisely when `engine is
None`, and the fetch loop's `fetch_conn` is `None` on the same path. Because the subscriber hands
`None` into `self._client.<method>(conn, ...)` on the test-broker path, narrowing the fake's
signature alone changes nothing — the call site would still pass `None` to a non-`None` parameter.
The union is the honest expression of a real duality: two adapters, one of which has no database
connection.

- **A `cast(AsyncConnection, None)` at the fake-path yield sites** would replace an honest-but-broad
  union, truthfully "can be `None`" and guarded by the real client, with a cast that actively lies —
  asserting `AsyncConnection` while holding `None` at runtime. Strictly worse.
- **Moving connection acquisition behind the seam** — an `acquire_writer()` / `acquire_reader()`
  context manager where the real client yields a pooled autocommit connection and the fake yields
  its own sentinel writer — is the only honest narrowing, and it is a sizable refactor of the
  load-bearing worker loop, fetch loop, both adapters, and the lease-token write path. The payoff,
  removing four defensive `TypeError` guards and four call-site comments, does not justify that risk
  today.

**Revisit trigger:** a third `AbstractOutboxClient` adapter is added — the two-adapter symmetry that
makes the shared union natural changes, and a handle abstraction may earn its keep; or the
connection lifecycle moves behind the seam for an independent reason (pooling or perf work on the
writer or fetch connection), at which point the handle becomes cheap and the union should be
narrowed along with it.
