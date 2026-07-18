---
status: accepted
summary: Free-threaded support is a compatibility guarantee (runs on 3.14t, GIL off), not a parallelism redesign; target 3.14t only.
supersedes: null
superseded_by: null
---

# Free-threading is a compatibility guarantee, not a parallelism redesign

**Decision:** "Free-threaded support" means proving and advertising that
`faststream-outbox` runs correctly on a free-threaded CPython (3.14t) with the
GIL disabled — it does **not** mean rearchitecting the subscriber to use multiple
CPU cores. Target 3.14t only, not 3.13t.

## Context

Free-threaded CPython removes the GIL, so threaded CPU-bound Python can use
multiple cores. The question raised: should `faststream-outbox` "support nogil"?
Two readings were on the table:

1. **Compatibility** — guarantee the package imports and runs green under a
   free-threaded interpreter, and say so.
2. **Exploit parallelism** — redesign the subscriber so its worker loops run
   across OS threads / multiple event loops to use multiple cores.

## Decision & rationale

We take reading (1) and explicitly reject (2) for now.

- The package is pure-Python asyncio: one event loop, N worker *tasks* (not OS
  threads), zero `threading`/lock/C-extension code. Free-threading changes none
  of its runtime semantics; the win from (1) is a proven guarantee, achieved with
  a CI job + classifier + docs and no source change.
- Reading (2) is a substantial rearchitecture of a deliberately single-loop
  design (the two-loop subscriber, lease-token invariant, drain-on-stop all
  assume one loop). Its payoff is also questionable: outbox throughput is
  dominated by Postgres I/O and row-lease contention, not by in-process CPU, so
  multi-core in-process fan-out is unlikely to be the bottleneck. Scaling today is
  "run more subscriber processes," which already uses more cores. (2) carries
  real invariant-breaking risk for an unproven gain — declined.
- **3.14t only, not 3.13t:** the compiled deps (`asyncpg`, `sqlalchemy`,
  `pydantic-core`) ship `cp314t` wheels but no `cp313t` wheels, so 3.13t cannot
  install the full graph. No point targeting an interpreter the dependencies
  cannot support.

Details of the shipped compat work → change
[2026-07-18.01-free-threaded-support](../changes/2026-07-18.01-free-threaded-support.md).

## Revisit trigger

Reopen (2) if a profiled workload shows the subscriber is **in-process CPU-bound**
(not Postgres/lease-bound) and running more processes is not an acceptable scale
lever. Reopen the 3.13t question if the compiled deps start publishing `cp313t`
wheels *and* 3.13t is still within the support window.
