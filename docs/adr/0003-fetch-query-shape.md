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
rebuild when upgrading from 0.14. The expired-lease branch reads `<table>_leased_idx`, keyed
the same way over leased rows, and filters out leases that haven't expired. Its first index,
`(queue, acquired_at)`, made the branch read and sort every expired lease of its queue: with 50,000
of them in a million rows a fetch took about 10 ms on PostgreSQL 17 and 15 ms on 13, against 1.4 ms
with the ordered index. It replaced that index rather than joining it, because a second index on
leased rows would add a write to every claim. Leases that are still held sit in the ordered index
too and the scan steps over them; with 20,000 in flight at the head of the queue a fetch still took
about 1.5 ms.
