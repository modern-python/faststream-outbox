---
summary: validate_schema() appends a hand-written-migration pointer to its RuntimeError for Alembic-blind drift (the outbox_lease_ck CHECK and partial-index predicates autogenerate cannot remediate).
---

# Design: Actionable error for Alembic-blind schema drift

## Summary

`validate_schema()` detects schema drift that `alembic revision --autogenerate`
**cannot** generate a migration for — the `<table>_lease_ck` CHECK constraint and
the load-bearing partial-index predicates. Today the operator gets a bare
`RuntimeError` and a dead end: re-running autogenerate produces nothing. This
change appends a one-line pointer to that error — only when an Alembic-blind
drift is present — directing the operator to a new docs section that holds the
exact hand-written migration recipe. No comparator hook; no new exception type.

## Motivation

`make_outbox_table` declares three partial indexes (each with a load-bearing
`postgresql_where`) and a `<table>_lease_ck` CHECK
(`(acquired_token IS NULL) = (acquired_at IS NULL)`). On a **fresh**
`create_table`, autogenerate renders all of it. On an **incremental** migration
onto a pre-existing table, Alembic's `compare_metadata` has no check-constraint
comparator and its index comparator ignores `postgresql_where`, so a missing or
drifted CHECK — and a non-partial / wrong-predicate / non-unique index — ships
silently. `validate_schema()` backstops this with direct `pg_catalog` /
`pg_constraint` probes (`_validate_index_predicates_sync`,
`_validate_check_constraints_sync`), so the drift *is* caught at runtime.

But the resulting error — e.g.

> `RuntimeError: Outbox schema mismatch: missing CHECK constraint 'outbox_lease_ck' (expected 'acquired_token is null = acquired_at is null')`

— is a dead end. The operator runs `alembic revision --autogenerate` expecting a
remediation migration and gets an empty `upgrade()`, because the same blindness
that let the drift through also prevents autogenerate from fixing it. The
detection is correct; the remediation path is missing.

The third validation pass (the Alembic `compare_metadata` diff for
tables/columns/plain indexes) reports drift that autogenerate *can* fix, so it
needs no special handling — the operator there just re-runs autogenerate.

## Non-goals

- An Alembic autogenerate comparator hook ("make autogenerate emit it"):
  explicitly declined — we are not registering a `comparators.dispatch_for`
  extension or asking users to wire one into `env.py`.
- A structured exception type (`SchemaMismatchError` with a `.remediation`
  list): out of scope; the raised type stays `RuntimeError`.
- Embedding the full copy-pasteable DDL in the error text: the error carries a
  short pointer; the DDL lives in docs.
- Server-default drift: still not validated (`compare_server_default=False`,
  unchanged).

## Design

### 1. Error composition in `validate_schema()` (`client.py`)

`validate_schema()` runs the same three probe groups as today, but tracks
whether the **Alembic-blind** probes contributed any errors. The blind probes
are exactly `_validate_index_predicates_sync` and
`_validate_check_constraints_sync`, both run only on the outbox table
(`self._table`) — the DLQ table declares no partial-index predicates or CHECK
constraints, so it is validated solely by the Alembic diff. The Alembic
`compare_metadata` diff (`_validate_schema_sync` / `_validate_dlq_schema_sync`)
is autogenerate-fixable and does **not** set the blind flag.

The message is built through a new **pure** helper so the append logic is
unit-testable without a live engine (the 100 % coverage gate forbids an
untested branch, and `test_unit.py` runs with no Postgres):

```python
_SCHEMA_MISMATCH_PREFIX = "Outbox schema mismatch: "
_AUTOGEN_BLIND_HINT = (
    "These (CHECK constraints and partial-index predicates) are invisible to "
    "'alembic revision --autogenerate' — hand-write the migration: "
    "https://faststream-outbox.modern-python.org/operations/alembic/"
    "#fixing-drift-autogenerate-cant-see"
)


def _compose_schema_mismatch_message(errors: list[str], *, has_blind_drift: bool) -> str:
    msg = _SCHEMA_MISMATCH_PREFIX + "; ".join(errors)
    if has_blind_drift:
        msg += "\n\n" + _AUTOGEN_BLIND_HINT
    return msg
```

`validate_schema()` collects the blind-probe errors separately, ORs their
presence into `has_blind_drift`, and raises
`RuntimeError(_compose_schema_mismatch_message(errors, has_blind_drift=...))`.

The prefix `"Outbox schema mismatch: " + "; ".join(errors)` and the per-error
strings are unchanged, so every existing `pytest.raises(..., match=...)`
substring assertion keeps passing. The pointer is appended on its own line
(`\n\n`) after the joined errors.

### 2. Docs section (`docs/operations/alembic.md`)

New section `## Fixing drift autogenerate can't see { #fixing-drift-autogenerate-cant-see }`,
placed after "Drift detection in CI" and before "DLQ retention via partition
drop". It states the two Alembic-blind classes and why autogenerate misses them
(no check-constraint comparator; index comparator ignores `postgresql_where`),
then gives exact hand-written `op.*` recipes:

- **Missing or drifted `lease_ck` CHECK** — drop first only if it exists but
  drifted, then create:

  ```python
  # only if it exists with a wrong predicate:
  op.drop_constraint('outbox_lease_ck', 'outbox', type_='check')
  op.create_check_constraint(
      'outbox_lease_ck', 'outbox',
      '(acquired_token IS NULL) = (acquired_at IS NULL)',
  )
  ```

- **Non-partial / wrong-predicate / non-unique index** — drop and recreate with
  the load-bearing `postgresql_where` (and `unique=True` for the timer-id
  index):

  ```python
  op.drop_index('outbox_timer_id_uq', table_name='outbox')
  op.create_index(
      'outbox_timer_id_uq', 'outbox', ['queue', 'timer_id'],
      unique=True, postgresql_where=sa.text('timer_id IS NOT NULL'),
  )
  # outbox_pending_idx: postgresql_where=sa.text('acquired_token IS NULL')
  # outbox_lease_idx:   postgresql_where=sa.text('acquired_token IS NOT NULL')
  ```

The recipe names the three index suffixes (`_pending_idx`, `_lease_idx`,
`_timer_id_uq`) and their expected predicates, matching
`_EXPECTED_INDEX_PREDICATES` in `client.py`.

A one-line cross-link is added from `docs/usage/schema-validation.md` to this
anchor.

### 3. Anchor stability

The error URL is `https://faststream-outbox.modern-python.org/operations/alembic/#fixing-drift-autogenerate-cant-see`
(site_url from `mkdocs.yml`, directory-URL form matching the existing
`#dlq-retention-via-partition-drop` anchor). The explicit `{ #... }` attr-list
anchor pins the slug so a later heading reword can't silently break the link.

## Operations

None. No infra, DNS, or external-account changes.

## Testing

- **`test_unit.py`** (no Postgres):
  - `_compose_schema_mismatch_message(errors, has_blind_drift=True)` contains the
    pointer URL and the `;`-joined prefix.
  - `has_blind_drift=False` omits the pointer entirely.
  - Prefix and `; ` join format are intact in both cases.
- **`test_integration.py`**:
  - Extend an Alembic-blind case (e.g.
    `test_validate_schema_fails_when_lease_check_constraint_missing`) to assert
    the pointer URL is in the raised message.
  - Assert an autogenerate-fixable case (e.g.
    `test_validate_schema_fails_when_columns_missing`) does **not** contain the
    pointer.
- **Docs**: `just docs-build` (`mkdocs build --strict`) passes — the new anchor
  resolves and the cross-link from `schema-validation.md` does not 404.
- **Lint**: `just lint-ci` clean.

## Risk

- **Low — link rot.** If the docs site_url or page path changes, the error URL
  goes stale. Mitigation: the explicit attr-list anchor + the `--strict` docs
  build (which fails on a broken in-repo cross-link) catch the in-repo half; the
  hostname is the one piece a strict build can't verify, and it is the published
  canonical domain.
- **Low — flag plumbing.** `has_blind_drift` must be ORed from the two blind
  probes specifically, not from the full error list, or the pointer would also
  fire on pure column drift. Covered by the autogenerate-fixable negative test.
- **Negligible — message contract.** Appending a trailing line preserves the
  prefix and per-error substrings, so existing `match=` assertions and any
  operator log-greps on the prefix are unaffected.

## On merge

Promote into `architecture/`: the schema-validation / drift behavior is
described in `CLAUDE.md`'s "User-owned schema" section (and any
`architecture/` deep-dive that covers `validate_schema`). Add a sentence noting
that an Alembic-blind drift error now carries a remediation pointer to
`docs/operations/alembic.md#fixing-drift-autogenerate-cant-see`.
