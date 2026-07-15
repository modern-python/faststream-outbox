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
for an Alembic migration; :func:`check_outbox_autovacuum` reports when a table lacks
it. The package applies nothing itself and never raises for autovacuum: it is a
performance recommendation, not a correctness requirement.
"""

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
    vacuum_threshold: int = 1000,
    insert_threshold: int = 1000,
) -> str:
    """Render the recommended ``ALTER TABLE … SET (autovacuum_*)`` statement.

    Drop it into an Alembic migration (``op.execute(outbox_autovacuum_ddl("outbox"))``)
    or run it via psql. ``vacuum_threshold`` / ``insert_threshold`` tune how many dead
    (resp. inserted) tuples trigger autovacuum; the scale factors are fixed at 0 -- that
    is the structural fix, not a knob. The insert-triggered reloptions require Postgres 13+.
    """
    quoted = _IDENTIFIER_PREPARER.quote(table_name)
    options = (
        (_SCALE_FACTOR_KEYS[0], "0"),
        (_VACUUM_THRESHOLD_KEY, str(vacuum_threshold)),
        (_SCALE_FACTOR_KEYS[1], "0"),
        (_INSERT_THRESHOLD_KEY, str(insert_threshold)),
    )
    settings = ", ".join(f"{key} = {value}" for key, value in options)
    return f"ALTER TABLE {quoted} SET ({settings})"
