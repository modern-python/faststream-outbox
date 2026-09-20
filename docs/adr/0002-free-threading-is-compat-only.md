# Free-threading is a compatibility guarantee, not a parallelism redesign

Free-threaded support means proving that `faststream-outbox` runs correctly on free-threaded CPython
3.14t with the GIL disabled; it does not mean rearchitecting the subscriber to use multiple cores.
The package is pure-Python asyncio, one event loop with N worker tasks rather than OS threads, so
free-threading changes none of its runtime semantics and the guarantee costs a CI job, a
`Free Threading :: 2 - Beta` classifier, and a docs note rather than a source change. Exploiting the
parallelism was rejected because the two-loop subscriber, the lease-token invariant, and
drain-on-stop all assume one loop, throughput is dominated by Postgres I/O, and scaling today means
running more subscriber processes. The guarantee was bounded by SQLAlchemy until 2.0.54, whose
cyextensions declare `Py_MOD_GIL_NOT_USED`; below it, importing SQLAlchemy re-enabled the GIL
process-wide, which is why 2.0.54 is the floor on 3.14. 3.13t is not a target
because the compiled dependencies ship `cp314t` wheels only.
