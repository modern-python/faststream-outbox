"""Per-table autovacuum tuning for the outbox table.

The outbox is a high-churn queue table: every message is one INSERT + one lease
UPDATE + one terminal DELETE, so dead tuples accumulate at ~2x the message rate.
Postgres' default ``autovacuum_vacuum_scale_factor = 0.2`` fires vacuum only after a
*fraction of the table* is dead -- on a queue table that lets bloat accumulate, and
if the table ever bloats the fraction is of the bloated size, so vacuum fires ever
less often (a death spiral). Setting the scale factor to 0 disables that trigger so
vacuum frequency tracks *churn* via a constant threshold, not table size.

SQLAlchemy's ``Table`` cannot carry reloptions (the PG dialect accepts no such
kwarg), so Alembic autogenerate can never emit them. These settings must be applied
by an explicit statement the user runs -- :func:`outbox_autovacuum_ddl` renders it
for an Alembic migration; ``validate_schema(check_autovacuum=True)`` enforces it
(raises) when the table lacks it. The package applies nothing itself; enforcement
is opt-in via that flag, not a default.
"""

from sqlalchemy import text
from sqlalchemy.dialects import postgresql


# Single source of truth for the reloption keys, shared by the renderer and the probe.
# Scale-factor keys are STRUCTURAL: they must be 0 to break the fraction-of-table
# death-spiral. Threshold keys are TUNABLE: they must merely be present (any value).
_SCALE_FACTOR_KEYS: tuple[str, str] = (
    "autovacuum_vacuum_scale_factor",
    "autovacuum_vacuum_insert_scale_factor",
)
_VACUUM_THRESHOLD_KEY = "autovacuum_vacuum_threshold"
_INSERT_THRESHOLD_KEY = "autovacuum_vacuum_insert_threshold"

# Standalone PG identifier preparer (no engine needed) -- quotes reserved words / odd
# characters exactly as SQLAlchemy's own DDL does; leaves simple names unquoted.
_IDENTIFIER_PREPARER = postgresql.dialect().identifier_preparer


def outbox_autovacuum_ddl(
    table_name: str = "outbox",
    *,
    schema: str | None = None,
    vacuum_threshold: int = 1000,
    insert_threshold: int = 1000,
) -> str:
    """Render the recommended ``ALTER TABLE … SET (autovacuum_*)`` statement.

    Drop it into an Alembic migration (``op.execute(outbox_autovacuum_ddl("outbox"))``)
    or run it via psql. ``vacuum_threshold`` / ``insert_threshold`` tune how many dead
    (resp. inserted) tuples trigger autovacuum; the scale factors are fixed at 0 -- that
    is the structural fix, not a knob. The insert-triggered reloptions require Postgres 13+.

    ``schema`` defaults to ``None``, which renders an unqualified table name that resolves
    via the connection's ``search_path`` -- matching both ``Table.schema=None`` and
    the ``validate_schema(check_autovacuum=True)`` reloptions lookup's
    ``COALESCE(:schema, current_schema())``. Pass the same ``schema`` as the outbox
    ``Table`` (e.g. ``table.schema``) when it lives in a named schema, so this DDL
    targets the same table the check reads.
    """
    quoted_table = _IDENTIFIER_PREPARER.quote(table_name)
    quoted_name = quoted_table if schema is None else f"{_IDENTIFIER_PREPARER.quote(schema)}.{quoted_table}"
    options = (
        (_SCALE_FACTOR_KEYS[0], "0"),
        (_VACUUM_THRESHOLD_KEY, str(vacuum_threshold)),
        (_SCALE_FACTOR_KEYS[1], "0"),
        (_INSERT_THRESHOLD_KEY, str(insert_threshold)),
    )
    settings = ", ".join(f"{key} = {value}" for key, value in options)
    return f"ALTER TABLE {quoted_name} SET ({settings})"


# reloptions come back from asyncpg as a ``list[str]`` of ``"key=value"`` items, or
# None when the table has no options set (or does not exist). NULL nspname match uses
# current_schema() so a search_path-relative table resolves the same way the app does.
_RELOPTIONS_QUERY = text(
    "SELECT c.reloptions FROM pg_class c "
    "JOIN pg_namespace n ON n.oid = c.relnamespace "
    "WHERE c.relname = :table AND n.nspname = COALESCE(:schema, current_schema())",
)

_SEE_DOCS = "apply outbox_autovacuum_ddl() in a migration -- see docs/operations/alembic.md"


def _parse_reloptions(reloptions: "list[str] | None") -> dict[str, str]:
    """Turn asyncpg's ``["k=v", …]`` reloptions array (or None) into a dict."""
    if not reloptions:
        return {}
    parsed: dict[str, str] = {}
    for item in reloptions:
        key, _, value = item.partition("=")
        parsed[key] = value
    return parsed


def autovacuum_findings(table_name: str, reloptions: "list[str] | None") -> list[str]:
    """Return one finding string per missing/wrong autovacuum setting; ``[]`` means OK.

    Pure -- takes already-fetched ``pg_class.reloptions`` (see :data:`_RELOPTIONS_QUERY`)
    so the caller (``OutboxClient.validate_schema``) owns the query and the connection.
    Structural check: each scale-factor key must be present AND 0; each threshold key
    must be present (any value -- a user's custom threshold must not be flagged).
    """
    options = _parse_reloptions(reloptions)
    findings: list[str] = []
    for key in _SCALE_FACTOR_KEYS:
        value = options.get(key)
        if value is None:
            findings.append(f"{table_name}: {key} is unset (want 0) -- bloat accumulates under churn; {_SEE_DOCS}.")
        elif float(value) != 0.0:
            findings.append(f"{table_name}: {key} is {value}, not 0 -- bloat accumulates under churn; {_SEE_DOCS}.")
    findings.extend(
        f"{table_name}: {key} is unset -- {_SEE_DOCS}."
        for key in (_VACUUM_THRESHOLD_KEY, _INSERT_THRESHOLD_KEY)
        if key not in options
    )
    return findings
