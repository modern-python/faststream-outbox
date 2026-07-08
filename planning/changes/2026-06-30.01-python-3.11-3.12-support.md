---
summary: Lower the supported-Python floor from 3.13 to 3.11 by backporting typing.override via typing_extensions; widen the CI matrix to 3.11/3.12.
---

# Design: Support Python 3.11 and 3.12

## Summary

`faststream-outbox` pins `requires-python = ">=3.13,<4"`, but its source is
already source-compatible with Python 3.11 save for one construct. This change
lowers the floor to 3.11 so the package installs and runs on 3.11, 3.12, 3.13,
and 3.14. It is a pure-Python library with no compiled extensions, so the work
is a source-compatibility + metadata change plus a wider CI matrix. It mirrors
the change already shipped in the sibling repo `faststream-redis-timers` (#49),
minus a PEP 695 type-alias edit that outbox does not need.

## Motivation

The 3.13 floor is stricter than the code requires and excludes the large share
of users still on 3.11/3.12. Empirical scan of the source against a real
CPython 3.11.9 interpreter shows the gap is two backportable `typing` symbols,
both reachable via `typing_extensions`:

| Symbol | Since | Locations | 3.11 |
|--------|-------|-----------|------|
| `override` | 3.12 | `broker.py` (`@typing.override` ×5), `subscriber/usecase.py` (`@typing.override` ×6), `registrator.py:3` (`from typing import ..., override`), `publisher/usecase.py:13` (`from typing import override`) | `ImportError` / `AttributeError` |
| `get_protocol_members` | 3.13 | `tests/test_fake.py`, `tests/test_unit.py` (protocol-completeness assertions) | `AttributeError` |

(The `get_protocol_members` use lives in the test suite, which also runs under
the floor on the CI matrix; `ty` surfaced it once the floor dropped. Both
symbols are rerouted through `typing_extensions` by the same unconditional
import; neither requires `sys.version_info` gating.)

Confirmed non-issues (verified, not assumed):

- Every source file under `faststream_outbox/` `py_compile`s cleanly on
  CPython 3.11.9 — there is **no PEP 695 syntax** (`type X = ...` aliases or
  `class Foo[T]` generics) anywhere.
- `asyncio.timeout` (`client.py:433`) and `datetime.UTC` (`_time.py:8`) were
  both added in Python 3.11, so they are valid at exactly the new floor. They
  are why the floor is **3.11 and not lower** — going below 3.11 would break
  them.
- `typing.Self`, `typing.Any`, `typing.TYPE_CHECKING` all exist in 3.11.
- Upstream deps support 3.11: faststream, redis, sqlalchemy, anyio all declare
  `requires-python >=3.10` or lower.
- `typing_extensions` is already present transitively (faststream pins
  `>=4.12.0`); `override` has lived there since 4.4.0, so a `>=4.12.0` floor
  amply covers it.

## Non-goals

- No new runtime features or behavior change — same public API, same semantics.
- No change to the dev/test `Dockerfile` Python version (it is a build image,
  not a supported-version gate).
- No change to `architecture/` capability pages — none reference the Python
  floor.

## Design

### 1. Backport `override` via `typing_extensions`

Declare `typing-extensions>=4.12.0` as a direct runtime dependency and import
`override` from it unconditionally (no `sys.version_info` gating).

- `broker.py` and `subscriber/usecase.py`: add `from typing_extensions import
  override` and replace each `@typing.override` with `@override`. The
  `import typing` line stays (still used for `typing.Self`, `typing.Any`,
  etc.).
- `registrator.py:3` and `publisher/usecase.py:13`: remove `override` from the
  `from typing import ...` line and add `from typing_extensions import
  override`.

The existing `# ty: ignore[invalid-method-override]` comments on the decorated
methods are unrelated to this change and stay as-is.

**Alternatives rejected:**
- *Version-gated stdlib imports* (`if sys.version_info >= (3, 12): from typing
  import override else: ...`) — more code, still needs typing_extensions on
  3.11, no benefit.
- *Drop `@override`* — loses the override-mismatch checking `ty` relies on.

### 2. `pyproject.toml` metadata

- `requires-python`: `>=3.13,<4` → `>=3.11,<4`
- `dependencies`: add `typing-extensions>=4.12.0`
- `classifiers`: add `Programming Language :: Python :: 3.11` and `:: 3.12`
- `[tool.ruff] target-version`: `py313` → `py311` (lint against the floor so
  3.12+-only syntax cannot silently reappear)

### 3. CI matrix

`.github/workflows/_checks.yml`: add `"3.11"` and `"3.12"` to the pytest
`python-version` matrix (currently `["3.13","3.14"]`). The lint job stays on
3.13.

## Testing

No new tests. This is a compatibility-surface change; verification is the
existing suite (100% coverage gate, `--cov-fail-under=100`) running green
across the widened CI matrix on 3.11, 3.12, 3.13, 3.14. Locally, an
import/runtime smoke check on the uv-managed 3.11 interpreter
(`uv run --python 3.11 --no-sync python -c "import faststream_outbox"` after a
3.11 sync, or a `--no-cov` subset of the no-Postgres suites) confirms the
`override` backport resolves before CI.

## Risk

- **Low.** The only code change is the import source of `override`; behavior is
  identical on 3.13/3.14 (typing_extensions re-exports the stdlib object).
- A 3.11/3.12-only runtime difference in a dependency could surface in CI — but
  all direct deps already advertise 3.10+ support, so this is unlikely. The
  widened matrix is exactly what would catch it.
- `uv.lock` must be regenerated locally so resolution succeeds on the lowered
  floor; it is git-ignored and not committed.
