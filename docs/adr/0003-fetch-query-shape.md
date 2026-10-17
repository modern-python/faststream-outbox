# The fetch claims rows with ordered per-queue scans, not one filtered sort

The fetch query takes each subscribed queue separately through `CROSS JOIN LATERAL` and runs two
branches per queue, unleased rows and expired leases. Each branch is an
`ORDER BY next_attempt_at, id LIMIT n FOR UPDATE SKIP LOCKED` scan, the branches are combined with
`UNION ALL`, and an outer `ORDER BY ... LIMIT n` picks the rows to claim. The unleased branch reads
`<table>_pending_idx`, whose key is `(queue, next_attempt_at, id)`, so the scan stops after `n`
unlocked rows and a fetch costs the same with ten ready rows or a million. The earlier single query,
`WHERE queue = ANY(:queues) AND (acquired_token IS NULL OR <expired>)`, claimed the same rows in the
same order, but Postgres could serve neither the OR nor `= ANY` with an ordered index scan, and it
doesn't push the row limit through `LockRows`, so every fetch read and sorted the whole backlog:
about 200 ms per fetch at a million ready rows, against 0.1 ms for this shape, on PostgreSQL 13 and
17 alike. `id` is in the index because rows published in one transaction share `next_attempt_at`,
and without it a fetch reads the entire tie group. The costs accepted are a statement that is harder
to read, up to `2 × n` rows per queue locked briefly while only `n` are claimed (other fetchers skip
them until the claim commits, which affects fairness, not correctness), and a concurrent index
rebuild when upgrading from 0.14. The expired-lease branch still sorts every expired lease of its
queue; an index for it was left out until it is measured against a realistic number of hung handlers.
