"""Schema-drift validation for the outbox (and optional DLQ) table.

Opt-in checks that the live Postgres table(s) match the package's canonical shape.
Split out of :mod:`faststream_outbox.client` so the transport client stays focused on
the read/write paths; this module concentrates the schema-drift concern and isolates
the optional ``alembic`` dependency (backing :func:`validate_schema` only).

The autogenerate diff (:func:`_run_validate`) is Alembic-blind to two drifts — partial
index predicates and CHECK constraints — so two direct ``pg_*`` probes
(:func:`_validate_index_predicates_sync`, :func:`_validate_check_constraints_sync`)
cover the gap.
"""

from typing import TYPE_CHECKING

from sqlalchemy import MetaData, text

# Optional dependency: alembic backs validate_schema() only. The probe lives in
# ``_import_checker`` so every optional-extra site uses the same shape. Users who
# don't call validate_schema() never trigger the runtime import path.
from faststream_outbox._import_checker import is_alembic_installed
from faststream_outbox.autovacuum import _RELOPTIONS_QUERY, autovacuum_findings
from faststream_outbox.schema import (
    _LEASE_CK_SUFFIX,
    _LEASE_IDX_SUFFIX,
    _PENDING_IDX_SUFFIX,
    _TIMER_ID_UQ_SUFFIX,
    make_dlq_table,
    make_outbox_table,
)


if TYPE_CHECKING:
    import typing
    from collections.abc import Callable, Mapping, Sequence

    from alembic.autogenerate import compare_metadata as _alembic_compare_metadata
    from alembic.migration import MigrationContext as _AlembicMigrationContext
    from sqlalchemy import Connection, Table
    from sqlalchemy.ext.asyncio import AsyncEngine

if is_alembic_installed:
    from alembic.autogenerate import compare_metadata as _alembic_compare_metadata
    from alembic.migration import MigrationContext as _AlembicMigrationContext


async def validate_schema(
    engine: "AsyncEngine",
    table: "Table",
    *,
    dlq_table: "Table | None" = None,
    check_autovacuum: bool = False,
) -> None:
    """Validate that the database table(s) match the package's expected columns.

    Raises ``RuntimeError`` listing every mismatch across the outbox table and,
    when configured, the DLQ table. Opt-in: call from your startup hook or
    ``/health`` endpoint, not from ``broker.start()`` (so Alembic can run
    migrations against the same DB without blocking startup).

    Pass ``check_autovacuum=True`` to also enforce the recommended
    ``autovacuum_vacuum_scale_factor = 0`` + threshold reloptions (see
    :func:`faststream_outbox.autovacuum.outbox_autovacuum_ddl`); an untuned table
    raises a distinctly-labeled "Outbox autovacuum not tuned: " error, separate
    from a schema mismatch, so an operator can tell the two apart. Default
    ``False`` never checks it.
    """
    async with engine.connect() as conn:
        errors = await conn.run_sync(_validate_schema_sync, table)
        # S2 / lease_ck: these two probes catch drift that `alembic revision
        # --autogenerate` cannot remediate — its index comparator ignores
        # postgresql_where and it has no check-constraint comparator at all.
        # Collect them separately so the raised error can point operators at
        # the hand-written-migration recipe (_AUTOGEN_BLIND_HINT) only when
        # one of them actually fired.
        blind_errors = await conn.run_sync(_validate_index_predicates_sync, table)
        blind_errors.extend(await conn.run_sync(_validate_check_constraints_sync, table))
        errors.extend(blind_errors)
        if dlq_table is not None:
            errors.extend(await conn.run_sync(_validate_dlq_schema_sync, dlq_table))
        autovacuum_errors: list[str] = []
        if check_autovacuum:
            reloptions = (
                await conn.execute(_RELOPTIONS_QUERY, {"table": table.name, "schema": table.schema})
            ).scalar_one_or_none()
            autovacuum_errors = autovacuum_findings(table.name, reloptions)
    message_parts: list[str] = []
    if errors:
        message_parts.append(_compose_schema_mismatch_message(errors, has_blind_drift=bool(blind_errors)))
    if autovacuum_errors:
        message_parts.append("Outbox autovacuum not tuned: " + "; ".join(autovacuum_errors))
    if message_parts:
        raise RuntimeError("\n\n".join(message_parts))


def _normalize_predicate(predicate: str) -> str:
    """Canonicalize a Postgres partial-index predicate for comparison (lowercase, drop parens/space)."""
    return " ".join(predicate.lower().replace("(", " ").replace(")", " ").split())


# Expected partial-index predicates, keyed by the index-name suffix make_outbox_table uses.
# These are what the fetch CTE and the producer's ON CONFLICT arbiter rely on (S2).
_EXPECTED_INDEX_PREDICATES = {
    _PENDING_IDX_SUFFIX: "acquired_token is null",
    _TIMER_ID_UQ_SUFFIX: "timer_id is not null",
    _LEASE_IDX_SUFFIX: "acquired_token is not null",
}

# Suffixes whose index must be UNIQUE — the timer_id arbiter relies on it for ON CONFLICT (F2-10).
_EXPECTED_UNIQUE_INDEXES = {_TIMER_ID_UQ_SUFFIX}

# No ``indpred IS NOT NULL`` filter: we must also catch an expected index that exists but
# was created NON-partial (predicate NULL) — that breaks ON CONFLICT inference the same way
# a wrong predicate does, and alembic's diff won't flag it either. ``pg_get_expr`` returns
# NULL for a non-partial index, which we treat as a distinct error below.
_INDEX_PREDICATE_QUERY = text(
    "SELECT c.relname AS index_name, pg_get_expr(i.indpred, i.indrelid) AS predicate, i.indisunique AS is_unique "
    "FROM pg_index i "
    "JOIN pg_class c ON c.oid = i.indexrelid "
    "JOIN pg_class t ON t.oid = i.indrelid "
    "JOIN pg_namespace n ON n.oid = t.relnamespace "
    "WHERE t.relname = :table AND n.nspname = COALESCE(:schema, current_schema())",
)


def _validate_index_predicates_sync(connection: "Connection", table: "Table") -> list[str]:
    """Compare the live partial-index WHERE predicates against what the package expects (S2).

    Alembic's index diff ignores ``postgresql_where``, so the alembic autogenerate pass
    (:func:`_run_validate`) does not catch two drifts that break the producer's ``ON CONFLICT``
    arbiter inference at publish time — both flagged by this separate probe:

    * a **wrong** predicate (e.g. ``{table}_timer_id_uq`` built ``WHERE timer_id IS NULL``
      instead of ``IS NOT NULL``), and
    * a present-but-**non-partial** index (a plain ``UNIQUE (queue, timer_id)`` with no
      ``WHERE``) — ``indpred`` is NULL, which alembic also can't distinguish from the partial form.

    An index that is **absent** entirely is left to the alembic existence diff.
    """
    rows = (
        connection.execute(
            _INDEX_PREDICATE_QUERY,
            {"table": table.name, "schema": table.schema},
        )
        .mappings()
        .all()
    )
    live = {row["index_name"]: row for row in rows}  # predicate is None for a non-partial index
    errors: list[str] = []
    for suffix, want in _EXPECTED_INDEX_PREDICATES.items():
        name = f"{table.name}{suffix}"
        if name in live:
            predicate = live[name]["predicate"]
            if predicate is None:
                errors.append(f"index {name!r} is not a partial index (expected predicate '{want}')")
            elif _normalize_predicate(predicate) != want:
                got = _normalize_predicate(predicate)
                errors.append(f"index {name!r} has wrong partial predicate: expected '{want}', got '{got}'")
            # F2-10: a same-named but NON-unique timer_id index passes the predicate check
            # yet breaks the producer's ON CONFLICT arbiter inference at publish time.
            if suffix in _EXPECTED_UNIQUE_INDEXES and not live[name]["is_unique"]:
                errors.append(f"index {name!r} is not UNIQUE (required for the timer_id ON CONFLICT arbiter)")
    return errors


_CHECK_CONSTRAINT_QUERY = text(
    "SELECT con.conname AS name, pg_get_constraintdef(con.oid) AS definition "
    "FROM pg_constraint con "
    "JOIN pg_class t ON t.oid = con.conrelid "
    "JOIN pg_namespace n ON n.oid = t.relnamespace "
    "WHERE t.relname = :table AND n.nspname = COALESCE(:schema, current_schema()) "
    "AND con.contype = 'c'",
)

# Expected CHECK-constraint predicates, keyed by the constraint-name suffix make_outbox_table
# uses. Normalized form (lowercased, parens/whitespace collapsed, leading ``check`` stripped)
# of ``(acquired_token IS NULL) = (acquired_at IS NULL)``.
_EXPECTED_CHECK_CONSTRAINTS = {
    _LEASE_CK_SUFFIX: "acquired_token is null = acquired_at is null",
}


def _validate_check_constraints_sync(connection: "Connection", table: "Table") -> list[str]:
    """Verify the live DB carries a CHECK enforcing each invariant the package needs.

    Alembic's ``compare_metadata`` registers no check-constraint comparator, so a missing
    or altered lease CHECK — the ``(acquired_token IS NULL) = (acquired_at IS NULL)`` invariant
    that makes a half-set lease unrepresentable — slips through :func:`_run_validate` entirely.
    Probe ``pg_constraint`` directly, mirroring :func:`_validate_index_predicates_sync`.

    Match by **predicate, not name**. A CHECK's name is irrelevant to whether it enforces the
    invariant, and the name is not predictable from the package side: a ``MetaData`` with a
    SQLAlchemy ``ck`` ``naming_convention`` re-templates the package's ``<table>_lease_ck`` to
    ``ck_<table>_<table>_lease_ck`` on the in-memory ``Table``, yet a hand-written migration
    (``op.create_check_constraint('<table>_lease_ck', ...)``) creates the literal name verbatim
    — Alembic op functions don't apply ``target_metadata``'s convention. So the live name varies
    by how the migration was authored; only the predicate is stable. Found under **any** name →
    pass; absent (including a drifted predicate, which is just "the right one is missing") → error.
    """
    rows = (
        connection.execute(
            _CHECK_CONSTRAINT_QUERY,
            {"table": table.name, "schema": table.schema},
        )
        .mappings()
        .all()
    )
    # pg_get_constraintdef returns e.g. ``CHECK (((a IS NULL) = (b IS NULL)))``; strip the
    # leading ``check`` keyword after normalizing away parens/case/whitespace.
    live_predicates = {_normalize_predicate(row["definition"]).removeprefix("check ").strip() for row in rows}
    errors: list[str] = []
    for suffix, want in _EXPECTED_CHECK_CONSTRAINTS.items():
        if want not in live_predicates:
            errors.append(
                f"missing CHECK constraint enforcing '{want}' (the lease invariant; name it e.g. {table.name}{suffix})",
            )
    return errors


# The published docs anchor for hand-written migrations that fix drift
# `alembic revision --autogenerate` cannot emit (no check-constraint comparator;
# the index comparator ignores postgresql_where). Appended to the RuntimeError
# only when an Alembic-blind probe actually fired — see validate_schema().
_SCHEMA_MISMATCH_PREFIX = "Outbox schema mismatch: "
_AUTOGEN_BLIND_HINT = (
    "These (CHECK constraints and partial-index predicates) are invisible to "
    "'alembic revision --autogenerate' — hand-write the migration: "
    "https://faststream-outbox.modern-python.org/operations/alembic/"
    "#fixing-drift-autogenerate-cant-see"
)


def _compose_schema_mismatch_message(errors: list[str], *, has_blind_drift: bool) -> str:
    """Build the validate_schema RuntimeError text; append the remediation pointer for Alembic-blind drift."""
    msg = _SCHEMA_MISMATCH_PREFIX + "; ".join(errors)
    if has_blind_drift:
        msg += "\n\n" + _AUTOGEN_BLIND_HINT
    return msg


def _validate_schema_sync(connection: "Connection", table: "Table") -> list[str]:
    """Run the outbox-table validation pass; see :func:`_run_validate` for the diff machinery."""
    return _run_validate(connection, table, make_outbox_table)


def _validate_dlq_schema_sync(connection: "Connection", table: "Table") -> list[str]:
    """Run the DLQ-table validation pass; see :func:`_run_validate` for the diff machinery."""
    return _run_validate(connection, table, make_dlq_table)


def _run_validate(
    connection: "Connection",
    table: "Table",
    canonical_factory: "Callable[[MetaData, str], Table]",
) -> list[str]:
    """Run Alembic's autogenerate diff against the live DB and surface any "missing schema" drift.

    The canonical schema is whatever ``canonical_factory`` produces — the same Table the user
    attaches to their own ``MetaData`` via ``make_outbox_table`` / ``make_dlq_table``. Delegating
    to Alembic avoids re-implementing column / index comparison logic (which would diverge from
    the declaration over time) and keeps the package out of the schema-management business that
    Alembic already owns.

    ``add_*`` and ``modify_*`` ops fail validation (the DB is missing or has the wrong shape for
    something the broker needs). ``remove_*`` ops are ignored — the user may have extra columns
    or indexes for their own use, and we don't care.

    NOT validated: server defaults (``compare_server_default=False``). Alembic's server-default
    comparison is notoriously flaky against Postgres' normalized expressions (``func.now()`` vs
    ``CURRENT_TIMESTAMP`` vs ``now()``), so we disable it to avoid false positives. The cost: a
    table missing ``server_default=func.now()`` on ``next_attempt_at`` will leave fresh rows
    with NULL ``next_attempt_at``, which the fetch CTE's ``next_attempt_at <= now()`` predicate
    silently filters out — a silent broker outage. If you change the canonical table to rely on
    additional server defaults, add a targeted ``information_schema`` probe rather than flipping
    this flag.
    """
    if not is_alembic_installed:
        msg = "validate_schema() requires alembic. Install with `pip install faststream-outbox[validate]`."
        raise ImportError(msg)

    # Isolated MetaData containing ONLY the canonical table, so the user's
    # domain tables (in their own MetaData) don't show up in the diff. Carry the
    # user's schema onto the canonical copy so the autogenerate diff compares
    # ``app.outbox`` against ``app.outbox`` rather than the default search_path —
    # matching the schema-qualified DLQ CTE in ``_build_dlq_cte_stmt`` (B10).
    canonical_metadata = MetaData(schema=table.schema)
    canonical_factory(canonical_metadata, table.name)

    # Alembic reports the connection's DEFAULT schema to ``include_name`` as ``None`` (with
    # ``include_schemas=True``). So a table that explicitly names the default schema
    # (``MetaData(schema="public")``, or a named schema that happens to be on the
    # search_path) must be normalized to ``None`` before comparing, else ``name == "public"``
    # never matches Alembic's ``None`` and a CORRECT table falsely reads as "does not exist".
    default_schema_name = connection.dialect.default_schema_name
    target_schema = None if table.schema == default_schema_name else table.schema

    def _include_name(name: str | None, type_: str, parent_names: "Mapping[str, str | None]") -> bool:
        # ``include_schemas=True`` makes Alembic enumerate EVERY schema and call this with
        # ``type_ == "schema"`` per schema — restrict to the (normalized) target schema so
        # unrelated schemas' tables never reflect into the diff (which would be false drift).
        if type_ == "schema":
            return name == target_schema
        if type_ == "table":
            return name == table.name
        return parent_names.get("table_name") == table.name

    ctx = _AlembicMigrationContext.configure(
        connection,
        opts={
            "compare_type": True,
            "compare_server_default": False,
            # Reflect beyond the default schema so a table in a non-default
            # ``MetaData(schema=...)`` is visible to ``compare_metadata`` (else it reads as
            # "table does not exist"); ``_include_name`` narrows reflection to the target schema.
            "include_schemas": True,
            "include_name": _include_name,
            "target_metadata": canonical_metadata,
        },
    )
    diff = _alembic_compare_metadata(ctx, canonical_metadata)
    return _flatten_drift_errors(diff, table.name)


def _flatten_drift_errors(diff: "Sequence[typing.Any]", table_name: str) -> list[str]:
    """Walk Alembic's nested diff and surface only the ops that mean *missing schema*.

    Top-level entries are tuples for table-level ops (``add_table``, ``remove_table``) and
    lists of nested tuples for column / index ops on existing tables. ``remove_*`` ops are
    skipped — extras are user business.
    """
    errors: list[str] = []
    for entry in diff:
        if isinstance(entry, list):
            for nested in entry:
                err = _drift_entry_to_error(nested, table_name)
                if err is not None:
                    errors.append(err)
        else:
            err = _drift_entry_to_error(entry, table_name)
            if err is not None:
                errors.append(err)
    return errors


def _drift_entry_to_error(entry: "tuple[typing.Any, ...]", table_name: str) -> str | None:
    """Map one Alembic op tuple to a human-readable error string, or None to ignore.

    Tuple shapes per Alembic's autogenerate contract:
      add_column       -> (op, schema, table_name, Column)
      modify_type      -> (op, schema, table_name, column_name, opts, existing_type, metadata_type)
      modify_nullable  -> (op, schema, table_name, column_name, opts, existing_null, metadata_null)
      add_index        -> (op, Index)

    ``add_constraint`` is intentionally not mapped. The canonical table *does* declare
    a CHECK (``<table>_lease_ck``) and a partial unique index (``<table>_timer_id_uq``):
    the unique index surfaces as ``add_index`` above, but Alembic's ``compare_metadata``
    has no check-constraint comparator, so a missing or altered CHECK never appears in
    this diff at all. That gap is covered separately by
    :func:`_validate_check_constraints_sync` (a direct ``pg_constraint`` probe).
    """
    op = entry[0]
    if op == "add_table":
        return f"table '{table_name}' does not exist"
    if op == "add_column":
        col = entry[3]
        return f"table '{table_name}' missing column '{col.name}'"
    if op == "modify_type":
        col_name = entry[3]
        existing_type = entry[5]
        expected_type = entry[6]
        return (
            f"table '{table_name}' column '{col_name}' type mismatch: expected {expected_type!r}, got {existing_type!r}"
        )
    if op == "modify_nullable":
        col_name = entry[3]
        return f"table '{table_name}' column '{col_name}' nullability mismatch"
    if op == "add_index":
        idx = entry[1]
        cols = [c.name for c in idx.columns]
        return f"table '{table_name}' missing index over {cols} (name={idx.name!r}, unique={bool(idx.unique)})"
    return None
