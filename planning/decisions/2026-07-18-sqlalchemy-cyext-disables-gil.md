---
status: accepted
summary: The free-threaded CI job sets DISABLE_SQLALCHEMY_CEXT_RUNTIME=1 because SQLAlchemy's Cython extensions re-enable the GIL on 3.14t; docs tell users to do the same for a genuinely GIL-free process.
supersedes: null
superseded_by: null
---

# SQLAlchemy C extensions are disabled to keep the GIL off on 3.14t

**Decision:** Scope the free-threaded (3.14t) guarantee to "runs with the GIL
genuinely disabled *when SQLAlchemy's Cython extensions are off*." The
`freethreaded` CI job sets `DISABLE_SQLALCHEMY_CEXT_RUNTIME=1` (SQLAlchemy's own
sanctioned switch) and asserts `sys._is_gil_enabled() is False`; the docs tell
users to set the same env var for a truly GIL-free process.

## Context

Adding the free-threaded compat guarantee (change
[2026-07-18.01](../changes/2026-07-18.01-free-threaded-support.md)), the local
proof on 3.14t found that `import sqlalchemy` **silently re-enables the GIL**.
SQLAlchemy 2.0.50 ships `cp314t` wheels, but its Cython extensions
(`sqlalchemy.cyextension.*`) do not declare free-thread safety (no
`Py_MOD_GIL_NOT_USED` slot), so CPython force-re-enables the GIL process-wide on
import. `asyncpg` and `pydantic-core` are unaffected — SQLAlchemy is the sole
offender. A `cp314t` wheel is therefore necessary but not sufficient for a
GIL-free run.

Options weighed: (1) set `DISABLE_SQLALCHEMY_CEXT_RUNTIME=1` and keep the
GIL-off assertion; (2) drop the assertion and make no GIL claim (compat only);
(3) defer the whole change until SQLAlchemy ships a free-threading-safe
cyextension.

## Decision & rationale

Take (1). The library runs *correctly* on 3.14t regardless of the GIL, but a
genuinely-disabled GIL matters for a user who runs other threaded code in the
same process — SQLAlchemy's cyext would otherwise re-enable the GIL
process-wide and kill their parallelism. `DISABLE_SQLALCHEMY_CEXT_RUNTIME` is
SQLAlchemy's documented, supported switch (pure-Python fallback of the same
behavior, only slower), not a hack, and it is fully under our control — so
there is no "wait for upstream" gap that would justify (3). Dropping the
assertion (2) discards the regression guard that caught this in the first
place. The cost of (1) is one CI env var plus a one-line docs caveat, and it is
trivially reversible when upstream fixes the extensions.

Rejected (3) — defer: the guarantee is already true today (the suite passes on
3.14t), the workaround is sanctioned and stable, the upstream timeline is
open-ended, and withholding classifier/docs leaves 3.14t adopters with no
signal. Deferring banks nothing and is strictly worse for users than shipping
with the documented caveat.

## Revisit trigger

Drop `DISABLE_SQLALCHEMY_CEXT_RUNTIME=1` (from CI and the docs caveat) once
SQLAlchemy ships Cython extensions that declare `Py_MOD_GIL_NOT_USED` and the
GIL stays disabled on 3.14t with C-accel on. Re-run the change's Step-3 GIL
assertion without the env var to confirm before removing it.
