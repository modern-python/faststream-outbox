# SQLAlchemy C extensions are disabled to keep the GIL off on 3.14t

**Decision:** scope the free-threaded guarantee of [ADR-0004](0004-free-threading-is-compat-only.md)
to "runs with the GIL genuinely disabled *when SQLAlchemy's Cython extensions are off*". The
`freethreaded` CI job sets `DISABLE_SQLALCHEMY_CEXT_RUNTIME=1` and asserts
`sys._is_gil_enabled() is False`; the docs tell users to set the same variable for a truly GIL-free
process.

Proving the guarantee on 3.14t found that `import sqlalchemy` silently re-enables the GIL.
SQLAlchemy ships `cp314t` wheels, but its Cython extensions (`sqlalchemy.cyextension.*`) do not
declare free-thread safety — no `Py_MOD_GIL_NOT_USED` slot — so CPython force-re-enables the GIL
process-wide on import. `asyncpg` and `pydantic-core` are unaffected; SQLAlchemy is the sole
offender, and a `cp314t` wheel is therefore necessary but not sufficient for a GIL-free run. The
library runs correctly on 3.14t either way, but a genuinely disabled GIL matters to a user running
other threaded code in the same process, whose parallelism SQLAlchemy's cyext would otherwise kill.
`DISABLE_SQLALCHEMY_CEXT_RUNTIME` is SQLAlchemy's own documented switch — the pure-Python fallback
of the same behaviour, only slower — not a hack, and it is fully under our control. The cost is one
CI variable plus a one-line docs caveat, trivially reversible.

- **Dropping the assertion and making no GIL claim** discards the regression guard that caught this
  in the first place.
- **Deferring the whole change until SQLAlchemy ships a free-threading-safe cyextension** banks
  nothing: the guarantee is already true today, the workaround is sanctioned and stable, the
  upstream timeline is open-ended, and withholding the classifier and docs leaves 3.14t adopters
  with no signal at all.

**Revisit trigger:** SQLAlchemy ships Cython extensions that declare `Py_MOD_GIL_NOT_USED` and the
GIL stays disabled on 3.14t with C acceleration on. Re-run the GIL assertion without the variable to
confirm before removing it from CI and the docs.
