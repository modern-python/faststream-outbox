# Free-threading is a compatibility guarantee, not a parallelism redesign

**Decision:** "free-threaded support" means proving that `faststream-outbox` runs correctly on a
free-threaded CPython (3.14t) with the GIL disabled, scoped to "when SQLAlchemy's Cython extensions
are off". It does not mean rearchitecting the subscriber to use multiple CPU cores. Target 3.14t
only, not 3.13t.

The package is pure-Python asyncio — one event loop, N worker *tasks* rather than OS threads, no
`threading`, lock, or C-extension code of its own — so free-threading changes none of its runtime
semantics and the guarantee costs a CI job, a classifier, and a docs note rather than a source
change. What users need to know is in `docs/introduction/installation.md`: 3.14t support, and that
`DISABLE_SQLALCHEMY_CEXT_RUNTIME=1` is required for a genuinely GIL-free process because
SQLAlchemy's cyextensions do not declare `Py_MOD_GIL_NOT_USED` and re-enable the GIL process-wide on
import.

- **Exploiting the parallelism** — worker loops across OS threads or multiple event loops — is a
  rearchitecture of a deliberately single-loop design; the two-loop subscriber, the lease-token
  invariant, and drain-on-stop all assume one loop. Outbox throughput is dominated by Postgres I/O
  and lease contention rather than in-process CPU, and scaling today is "run more subscriber
  processes", which already uses more cores. Real invariant-breaking risk for an unproven gain.
- **Dropping the GIL-off assertion** and making no GIL claim discards the regression guard that
  caught the SQLAlchemy behaviour in the first place.
- **Deferring until SQLAlchemy ships free-thread-safe cyextensions** banks nothing: the guarantee is
  true today, the workaround is SQLAlchemy's own documented switch, the upstream timeline is
  open-ended, and withholding the classifier leaves 3.14t adopters with no signal.
- **Targeting 3.13t** is not available: the compiled dependencies ship `cp314t` wheels but no
  `cp313t` ones, so the full graph will not install.
