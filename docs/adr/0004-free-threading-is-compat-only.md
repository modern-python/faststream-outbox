# Free-threading is a compatibility guarantee, not a parallelism redesign

**Decision:** "free-threaded support" means proving and advertising that `faststream-outbox` runs
correctly on a free-threaded CPython (3.14t) with the GIL disabled. It does not mean rearchitecting
the subscriber to use multiple CPU cores. Target 3.14t only, not 3.13t.

The package is pure-Python asyncio: one event loop, N worker *tasks* rather than OS threads, and no
`threading`, lock, or C-extension code of its own. Free-threading changes none of its runtime
semantics, so the compatibility guarantee is achieved with a CI job, a classifier, and docs — no
source change. 3.14t only because the compiled dependencies (`asyncpg`, `sqlalchemy`,
`pydantic-core`) ship `cp314t` wheels but no `cp313t` ones, so 3.13t cannot install the full graph;
there is no point targeting an interpreter the dependencies cannot support.

- **Exploiting the parallelism** — running worker loops across OS threads or multiple event loops —
  is a substantial rearchitecture of a deliberately single-loop design; the two-loop subscriber, the
  lease-token invariant, and drain-on-stop all assume one loop. The payoff is also questionable:
  outbox throughput is dominated by Postgres I/O and row-lease contention, not in-process CPU, and
  scaling today is "run more subscriber processes", which already uses more cores. Real
  invariant-breaking risk for an unproven gain.

**Revisit trigger:** a profiled workload shows the subscriber is in-process CPU-bound rather than
Postgres/lease-bound *and* running more processes is not an acceptable scale lever. Reopen the 3.13t
question if the compiled dependencies start publishing `cp313t` wheels while 3.13t is still within
the support window.
