# FastStream 0.7.1 TestBroker typing alignment — design

**Status:** Draft
**Date:** 2026-06-04
**Slug:** `faststream-0.7.1-testbroker-typing`

## Goal

Adopt the upstream `TestBroker` typing fix shipped in FastStream 0.7.1
(ag2ai/faststream#2903) by binding the new `EnterType` generic to
`OutboxBroker` and removing the two `# ty: ignore` directives that
worked around the same upstream bug in our codebase.

## Background

In FastStream 0.7.0, `TestBroker.__aenter__` was annotated
`Broker | list[Broker]`. That union made the natural usage shape fail
the type checker:

```python
async with TestOutboxBroker(OutboxBroker(...)) as br:
    await br.publish(...)
    # error: Item "list[OutboxBroker]" of "OutboxBroker | list[OutboxBroker]"
    #        has no attribute "publish"
```

The sibling project `faststream-redis-timers` worked around this by
overriding `__aenter__` to return a single broker directly (plus an
`isinstance(list)` assert). **This project took a different route**:
it suppressed the resulting `ty` diagnostics with two targeted ignores
and never wrote an `__aenter__` override.

FastStream 0.7.1 (PR ag2ai/faststream#2903) fixes the root cause:

1. `TestBroker` becomes `Generic[Broker, EnterType]`. `EnterType` uses
   `typing_extensions.TypeVar` with `default=Any` for backward compatibility.
2. `__aenter__` returns `EnterType` instead of `Broker | list[Broker]`.
3. Each concrete subclass adds two `@overload`s on `__init__` that bind
   `EnterType` to either `SomeBroker` (single) or `tuple[SomeBroker, ...]`
   (multi). Note the multi case now returns a `tuple`, not a `list`.
4. The ASGI registry annotation in `try_it_out.py` becomes
   `TestBroker[Any, Any]`.
5. The AST-inspection helper in `_internal/testing/ast.py` learns to walk
   past *any* number of `__init__` frames, so subclasses (like ours) that
   add their own `__init__` continue to work.

Because `EnterType` defaults to `Any`, our existing `TestBroker[OutboxBroker]`
annotation would still type-check under 0.7.1 — but the two `ty: ignore`
suppressions can come off once we bind `EnterType = OutboxBroker` and
align the registry-hook annotation to the new two-param shape.

## Scope

### In scope

- Drop `# ty: ignore[invalid-type-arguments]` on the `TestOutboxBroker`
  class declaration in `faststream_outbox/testing.py`, switching the
  generic to `TestBroker[OutboxBroker, OutboxBroker]`.
- Drop `# ty: ignore[invalid-return-type]` on the `get_broker_registry`
  return in `faststream_outbox/__init__.py`, updating the annotation to
  `TestBroker[typing.Any, typing.Any]` to match upstream's new registry
  signature.
- Bump the FastStream pin in `pyproject.toml`:
  `faststream>=0.7,<0.8` → `faststream>=0.7.1,<0.8`.
- Add one regression test in `tests/test_fake.py` ensuring
  `async with TestOutboxBroker(broker)` yields a single `OutboxBroker`
  (not a list or tuple).

### Out of scope

- Multi-broker `TestOutboxBroker` support (current `__init__` accepts a
  single broker; we have no use case for multi).
- The `# ty: ignore[invalid-argument-type]` on `patch_broker_calls(broker)`
  in `testing.py:_fake_start` — this is unrelated (config-generic
  invariance on `BrokerUsecase[Msg, Conn, BrokerConfig]`) and is
  documented in `CLAUDE.md` under the publisher/producer ignore table.
- The `# ty: ignore[missing-argument]` directives on `broker.publish(...)`
  calls inside `tests/test_fake.py` — also unrelated (test broker
  patches `publish` to make `session` optional in tests; `ty` sees the
  original signature). Documented in `CLAUDE.md`.
- Any behavioral or runtime change.
- Refactors elsewhere in the package.

## Detailed changes

### `faststream_outbox/testing.py`

Current (line 521):

```python
class TestOutboxBroker(TestBroker[OutboxBroker]):  # ty: ignore[invalid-type-arguments]
```

After:

```python
class TestOutboxBroker(TestBroker[OutboxBroker, OutboxBroker]):
```

Binding `EnterType = OutboxBroker` makes `__aenter__` return
`OutboxBroker` directly. The class no longer raises
`invalid-type-arguments` from `ty`, so the suppression comes off.

### `faststream_outbox/__init__.py`

Current (lines 45–47):

```python
@functools.lru_cache(maxsize=1)
def get_broker_registry() -> dict[type[BrokerUsecase[typing.Any, typing.Any]], type[TestBroker[typing.Any]]]:
    return {**original_get_broker_registry(), OutboxBroker: TestOutboxBroker}  # ty: ignore[invalid-return-type]
```

After:

```python
@functools.lru_cache(maxsize=1)
def get_broker_registry() -> dict[
    type[BrokerUsecase[typing.Any, typing.Any]],
    type[TestBroker[typing.Any, typing.Any]],
]:
    return {**original_get_broker_registry(), OutboxBroker: TestOutboxBroker}
```

Matches upstream's new `try_it_out._get_broker_registry` signature.
With both type params present and `TestOutboxBroker` now declared
`TestBroker[OutboxBroker, OutboxBroker]`, the return value is
structurally assignable and the ignore comes off.

### `pyproject.toml`

Current (line 12): `"faststream>=0.7,<0.8",`
After:           `"faststream>=0.7.1,<0.8",`

### `tests/test_fake.py` — new regression test

Appended to the end of `tests/test_fake.py`:

```python
async def test_test_broker_aenter_returns_single_outbox_broker() -> None:
    """0.7.1's EnterType binding means TestOutboxBroker yields a single OutboxBroker, not a list/tuple.

    Guards the contract through the upstream typing refactor: even if the base
    class signature changes again, our single-broker subclass must always hand
    back a single broker instance.
    """
    broker = _make_broker()
    async with TestOutboxBroker(broker) as br:
        assert isinstance(br, OutboxBroker)
```

The single `isinstance(br, OutboxBroker)` assertion is sufficient:
since `OutboxBroker` is not a `list` or `tuple` subclass, an extra
`assert not isinstance(br, (list, tuple))` adds no additional safety.
The docstring covers the intent.

No new imports needed — `TestOutboxBroker`, `OutboxBroker`, and the
`_make_broker()` helper are already in scope at the top of
`tests/test_fake.py`. The test follows the existing `_make_broker()`
pattern used throughout the file, not the integration-style
`OutboxBroker(engine=...)` construction.

## Validation

Run in order:

1. `just install` — pull in `faststream==0.7.1`.
2. `just lint` — confirm `ruff`, `ruff format`, and `ty check` are all
   clean after removing the two ignores. **If `ty` flags a *different*
   issue on either annotation, stop and investigate** rather than
   re-adding the original suppression — that would mean upstream's
   0.7.1 fix isn't behaving as documented in our environment.
3. `just test` — full suite under docker compose. Of particular interest:
   - `tests/test_fake.py` — every test uses `async with TestOutboxBroker(broker)`.
     If `EnterType` were wired wrong, `br.publish(...)` and similar calls
     would explode at type-check time and possibly at runtime under
     stricter Python.
   - The new regression test from `tests/test_fake.py`.

## Risks

- **Upstream AST helper walking past our `__init__` frame.** PR #2903
  explicitly handles arbitrary `__init__` depth via a `while … name ==
  "__init__"` walk in `_internal/testing/ast.py`. `TestOutboxBroker`
  adds an extra `__init__` frame on top of `TestBroker.__init__`; the
  full `test_fake.py` suite exercises this path. No action needed.
- **`uv lock --upgrade` pulling in unrelated upgrades.** `just install`
  refreshes all dependencies. If incidental breakage surfaces, narrow
  the upgrade to `uv lock --upgrade-package faststream` and re-run
  `uv sync --frozen` to avoid pulling in unrelated changes.
- **Pinning out 0.7.0 consumers.** The previous release already
  required `>=0.7`; bumping to `>=0.7.1` is a trivial floor increment.
  No migration note needed.
- **`ty` still flags a different diagnostic after the changes.** If
  this happens, document the new suppression with a tight justification
  (matching the format in `CLAUDE.md`'s ignore table) rather than
  re-adding the originals — the originals targeted bugs that 0.7.1
  fixes upstream, so they would be misleading.

## Rollout

- Single PR on branch `chore/faststream-0.7.1-testbroker-typing`,
  matching the sibling project's naming convention.
- Bundled commit (the pin bump and the suppression removals are tightly
  coupled — the removals are only safe once we require 0.7.1+).
- Follows the project workflow in `CLAUDE.md`:
  brainstorming → spec → writing-plans → plan →
  executing-plans / subagent-driven-development →
  requesting-code-review → finishing-a-development-branch.
