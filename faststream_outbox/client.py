"""
Postgres outbox client.

All read/write paths against the outbox table live here. The fetch query is the
load-bearing piece: a single CTE that selects available rows ``FOR UPDATE SKIP LOCKED``
and immediately ``UPDATE``s them with a fresh lease (``acquired_token`` + ``acquired_at``),
``RETURNING`` the row in one round-trip.

A row is "available" iff its lease is unset *or* its lease has expired
(``acquired_at < now() - lease_ttl_seconds``). This collapses what used to be a
state column plus a separate ``release_stuck`` reaper into a single predicate.

Every terminal write (``delete_with_lease``, ``mark_pending_with_lease``) filters
on ``acquired_token`` so a slow handler whose lease was reclaimed by a newer fetch
can no longer mutate that row.
"""

import datetime as _dt
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlalchemy import (
    Float,
    MetaData,
    bindparam,
    delete,
    func,
    or_,
    select,
    text,
    update,
)

from faststream_outbox.message import OutboxInnerMessage
from faststream_outbox.schema import make_outbox_table


# Optional dependency: alembic backs validate_schema() only. Importing at module
# level (per the project's "no inline imports" convention) means we resolve once
# at import time; users who don't call validate_schema() never trigger the path
# and never need the dependency installed.
try:
    from alembic.autogenerate import compare_metadata as _alembic_compare_metadata
    from alembic.migration import MigrationContext as _AlembicMigrationContext
except ImportError:  # pragma: no cover  # alembic is in the dev group so the except branch is unreachable in CI
    _alembic_compare_metadata = None  # ty: ignore[invalid-assignment]
    _AlembicMigrationContext = None  # ty: ignore[invalid-assignment]


if TYPE_CHECKING:
    import typing
    from collections.abc import Mapping, Sequence

    from sqlalchemy import Connection, Table
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine


@dataclass(frozen=True)
class FetchResult:
    """Outcome of a single fetch cycle, used to drive the subscriber's idle backoff."""

    rows: list[OutboxInnerMessage]


class OutboxClient:
    def __init__(self, engine: "AsyncEngine", outbox_table: "Table") -> None:
        self._engine = engine
        self._table = outbox_table

    @property
    def table(self) -> "Table":
        return self._table

    @property
    def engine(self) -> "AsyncEngine":
        """
        The underlying ``AsyncEngine``.

        Used by the subscriber loop to open its own long-lived fetch connection and to
        drive ``LISTEN/NOTIFY``.
        """
        return self._engine

    async def fetch_with_conn(
        self,
        conn: "AsyncConnection",
        queues: "Sequence[str]",
        *,
        limit: int,
        lease_ttl_seconds: float,
    ) -> list[OutboxInnerMessage]:
        """
        Run the fetch CTE on a caller-supplied connection.

        Same contract as :meth:`fetch`. Used by ``OutboxSubscriber._fetch_loop`` to reuse
        a long-lived connection instead of acquiring one per fetch from the pool. Each
        call opens its own transaction on *conn* via ``async with conn.begin():``.

        Callers must pass a non-empty *queues*; the empty-queue short-circuit lives in
        :meth:`fetch`.
        """
        token = uuid.uuid4()
        t = self._table

        # ``make_interval(secs => :lease_ttl)`` keeps the cutoff computation server-side
        # so lease expiry is immune to clock skew between worker and DB hosts.
        lease_cutoff = func.now() - func.make_interval(0, 0, 0, 0, 0, 0, bindparam("lease_ttl", type_=Float))
        ready = (
            select(t.c.id)
            .where(
                t.c.next_attempt_at <= func.now(),
                or_(*(t.c.queue == q for q in queues)),
                or_(t.c.acquired_token.is_(None), t.c.acquired_at < lease_cutoff),
            )
            .order_by(t.c.next_attempt_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
            .cte("ready")
        )
        stmt = (
            update(t)
            .where(t.c.id.in_(select(ready.c.id)))
            .values(
                acquired_at=func.now(),
                acquired_token=token,
                deliveries_count=t.c.deliveries_count + 1,
            )
            .returning(*t.c)
        )
        async with conn.begin():
            result = await conn.execute(stmt, {"lease_ttl": max(0.0, lease_ttl_seconds)})
            rows = result.mappings().all()
        return [_row_to_message(dict(row)) for row in rows]

    async def fetch(
        self,
        queues: "Sequence[str]",
        *,
        limit: int,
        lease_ttl_seconds: float,
    ) -> list[OutboxInnerMessage]:
        """
        Atomically claim up to *limit* available rows for the given queue names.

        A row is available iff its lease is unset (``acquired_token IS NULL``) or its
        lease is older than *lease_ttl_seconds*. Returns the freshly-leased rows; each
        carries ``acquired_token`` which the worker loop must echo back on the terminal
        ``DELETE``/``UPDATE``.

        Convenience wrapper that opens a one-shot connection and delegates to
        :meth:`fetch_with_conn`. The subscriber loop uses ``fetch_with_conn`` directly
        with a long-lived connection.
        """
        if not queues:
            return []
        async with self._engine.connect() as conn:
            return await self.fetch_with_conn(
                conn,
                queues,
                limit=limit,
                lease_ttl_seconds=lease_ttl_seconds,
            )

    async def delete_with_lease(self, message_id: int, acquired_token: uuid.UUID) -> bool:
        """Delete *message_id* iff it still holds *acquired_token*. Returns True if deleted."""
        t = self._table
        stmt = delete(t).where(t.c.id == message_id, t.c.acquired_token == acquired_token)
        async with self._engine.begin() as conn:
            result = await conn.execute(stmt)
        return (result.rowcount or 0) > 0

    async def mark_pending_with_lease(  # noqa: PLR0913
        self,
        message_id: int,
        acquired_token: uuid.UUID,
        *,
        delay_seconds: float,
        attempts_count: int,
        first_attempt_at: _dt.datetime,
        last_attempt_at: _dt.datetime,
    ) -> bool:
        """
        Release the lease on *message_id* and reschedule it for retry, iff it still holds the lease.

        ``next_attempt_at`` is computed server-side as ``now() + delay_seconds`` so
        retry timing uses the DB clock, not the worker's. Returns True if the row was updated.
        """
        t = self._table
        next_attempt_at_expr = func.now() + func.make_interval(0, 0, 0, 0, 0, 0, bindparam("delay", type_=Float))
        stmt = (
            update(t)
            .where(t.c.id == message_id, t.c.acquired_token == acquired_token)
            .values(
                next_attempt_at=next_attempt_at_expr,
                attempts_count=attempts_count,
                first_attempt_at=first_attempt_at,
                last_attempt_at=last_attempt_at,
                acquired_at=None,
                acquired_token=None,
            )
        )
        async with self._engine.begin() as conn:
            result = await conn.execute(stmt, {"delay": max(0.0, delay_seconds)})
        return (result.rowcount or 0) > 0

    async def validate_schema(self) -> None:
        """
        Validate that the database table matches the package's expected columns.

        Raises ``RuntimeError`` listing every mismatch. Opt-in: call from your startup
        hook or ``/health`` endpoint, not from ``broker.start()`` (so Alembic can run
        migrations against the same DB without blocking startup).
        """
        async with self._engine.connect() as conn:
            errors = await conn.run_sync(_validate_schema_sync, self._table)
        if errors:
            msg = "Outbox schema mismatch: " + "; ".join(errors)
            raise RuntimeError(msg)

    async def ping(self) -> bool:
        try:
            async with self._engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
        except Exception:  # noqa: BLE001
            return False
        return True


def _row_to_message(row: dict) -> OutboxInnerMessage:
    return OutboxInnerMessage(
        id=row["id"],
        queue=row["queue"],
        payload=row["payload"],
        headers=row["headers"],
        attempts_count=row["attempts_count"],
        deliveries_count=row["deliveries_count"],
        created_at=row["created_at"],
        next_attempt_at=row["next_attempt_at"],
        first_attempt_at=row["first_attempt_at"],
        last_attempt_at=row["last_attempt_at"],
        acquired_at=row["acquired_at"],
        acquired_token=row["acquired_token"],
    )


def _validate_schema_sync(connection: "Connection", table: "Table") -> list[str]:
    """
    Run Alembic's autogenerate diff against the live DB and surface any "missing schema" drift.

    The canonical schema is whatever ``make_outbox_table`` produces — the same Table the user
    attaches to their own ``MetaData``. Delegating to Alembic avoids re-implementing column /
    index comparison logic (which would diverge from the declaration over time) and keeps the
    package out of the schema-management business that Alembic already owns.

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
    if _alembic_compare_metadata is None or _AlembicMigrationContext is None:
        msg = "validate_schema() requires alembic. Install with `pip install faststream-outbox[validate]`."
        raise ImportError(msg)

    # Isolated MetaData containing ONLY the canonical outbox table, so the user's
    # domain tables (in their own MetaData) don't show up in the diff.
    canonical_metadata = MetaData()
    make_outbox_table(canonical_metadata, table_name=table.name)

    def _include_name(name: str | None, type_: str, parent_names: "Mapping[str, str | None]") -> bool:
        if type_ == "schema":
            return True
        if type_ == "table":
            return name == table.name
        return parent_names.get("table_name") == table.name

    ctx = _AlembicMigrationContext.configure(
        connection,
        opts={
            "compare_type": True,
            "compare_server_default": False,
            "include_name": _include_name,
            "target_metadata": canonical_metadata,
        },
    )
    diff = _alembic_compare_metadata(ctx, canonical_metadata)
    return _flatten_drift_errors(diff, table.name)


def _flatten_drift_errors(diff: "Sequence[typing.Any]", table_name: str) -> list[str]:
    """
    Walk Alembic's nested diff and surface only the ops that mean *missing schema*.

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
    """
    Map one Alembic op tuple to a human-readable error string, or None to ignore.

    Tuple shapes per Alembic's autogenerate contract:
      add_column       -> (op, schema, table_name, Column)
      modify_type      -> (op, schema, table_name, column_name, opts, existing_type, metadata_type)
      modify_nullable  -> (op, schema, table_name, column_name, opts, existing_null, metadata_null)
      add_index        -> (op, Index)

    The canonical outbox table declares no CHECK / FK / UNIQUE constraints
    (only the autoincrement PK, which Alembic emits as part of ``add_table``),
    so ``add_constraint`` is not a reachable op here.
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
