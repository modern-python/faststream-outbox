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

import abc
import datetime as _dt
import uuid
from typing import TYPE_CHECKING

from sqlalchemy import (
    ARRAY,
    Float,
    MetaData,
    String,
    and_,
    any_,
    bindparam,
    delete,
    func,
    or_,
    select,
    text,
    update,
)

# Optional dependency: alembic backs validate_schema() only. The probe lives in
# ``_import_checker`` so every optional-extra site uses the same shape. Users who
# don't call validate_schema() never trigger the runtime import path.
from faststream_outbox._import_checker import is_alembic_installed
from faststream_outbox.message import OutboxInnerMessage
from faststream_outbox.schema import make_dlq_table, make_outbox_table


if TYPE_CHECKING:
    import typing
    from collections.abc import Callable, Mapping, Sequence

    from alembic.autogenerate import compare_metadata as _alembic_compare_metadata
    from alembic.migration import MigrationContext as _AlembicMigrationContext
    from sqlalchemy import Connection, Table
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

if is_alembic_installed:
    from alembic.autogenerate import compare_metadata as _alembic_compare_metadata
    from alembic.migration import MigrationContext as _AlembicMigrationContext


class AbstractOutboxClient(abc.ABC):
    """
    Outbox client interface.

    Satisfied by both :class:`OutboxClient` (real Postgres) and ``FakeOutboxClient``
    (in-memory test substitute, defined in ``testing.py``). The subscriber's ``_client``
    holds either at runtime; declaring this base lets the type checker see one consistent
    surface instead of duck-typed drift between the two.

    ``conn`` is typed ``AsyncConnection | None`` because the fake legitimately accepts
    None (no engine; conn is ignored). The real :class:`OutboxClient` narrows that to a
    non-None requirement by raising ``TypeError`` at the boundary — the None form is
    only ever reached via the test-broker path against the fake, but the shared static
    type has to admit it for both implementations to satisfy the same interface.
    """

    @property
    @abc.abstractmethod
    def engine(self) -> "AsyncEngine | None": ...

    @property
    @abc.abstractmethod
    def table(self) -> "Table": ...

    @abc.abstractmethod
    async def fetch(
        self,
        conn: "AsyncConnection | None",
        queues: "Sequence[str]",
        *,
        limit: int,
        lease_ttl_seconds: float,
    ) -> list[OutboxInnerMessage]: ...

    @abc.abstractmethod
    async def delete_with_lease(
        self,
        conn: "AsyncConnection | None",
        message_id: int,
        acquired_token: uuid.UUID,
        *,
        dlq_payload: "Mapping[str, typing.Any] | None" = None,
    ) -> bool: ...

    @abc.abstractmethod
    async def mark_pending_with_lease(
        self,
        conn: "AsyncConnection | None",
        message_id: int,
        acquired_token: uuid.UUID,
        *,
        delay_seconds: float,
        attempts_count: int,
        first_attempt_at: _dt.datetime,
        last_attempt_at: _dt.datetime,
    ) -> bool: ...

    @abc.abstractmethod
    async def validate_schema(self) -> None: ...

    @abc.abstractmethod
    async def ping(self) -> bool: ...


class OutboxClient(AbstractOutboxClient):
    def __init__(
        self,
        engine: "AsyncEngine",
        outbox_table: "Table",
        *,
        dlq_table: "Table | None" = None,
    ) -> None:
        self._engine = engine
        self._table = outbox_table
        self._dlq_table = dlq_table

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

    async def fetch(
        self,
        conn: "AsyncConnection | None",
        queues: "Sequence[str]",
        *,
        limit: int,
        lease_ttl_seconds: float,
    ) -> list[OutboxInnerMessage]:
        """
        Atomically claim up to *limit* available rows for the given queue names on *conn*.

        A row is available iff its lease is unset (``acquired_token IS NULL``) or its
        lease is older than *lease_ttl_seconds*. Returns the freshly-leased rows; each
        carries ``acquired_token`` which the worker loop must echo back on the terminal
        ``DELETE``/``UPDATE``. Opens its own transaction on *conn* via ``async with
        conn.begin():``.

        *conn* is widened to ``AsyncConnection | None`` for :class:`AbstractOutboxClient`
        compatibility (the fake accepts None). The real client narrows here: passing None
        raises ``TypeError`` immediately instead of silently AttributeError'ing inside
        ``conn.begin()``.
        """
        if conn is None:
            msg = "OutboxClient.fetch requires a live AsyncConnection (got None)"
            raise TypeError(msg)
        if not queues:
            return []
        token = uuid.uuid4()
        t = self._table

        # ``make_interval(secs => :lease_ttl)`` keeps the cutoff computation server-side
        # so lease expiry is immune to clock skew between worker and DB hosts.
        lease_cutoff = func.now() - func.make_interval(0, 0, 0, 0, 0, 0, bindparam("lease_ttl", type_=Float))
        ready = (
            select(t.c.id)
            .where(
                t.c.next_attempt_at <= func.now(),
                # P11: ``queue = ANY(:queues)`` binds a single array param, so the SQL
                # text is stable regardless of the number of queues — asyncpg can reuse
                # the prepared statement across ticks instead of recompiling an
                # OR-of-N-equalities whose text changes with the queue count.
                t.c.queue == any_(bindparam("queues", list(queues), type_=ARRAY(String))),
                # The OR is split into two index-implying disjuncts so Postgres'
                # partial-index inference picks up both `_pending_idx` (Branch A,
                # `acquired_token IS NULL`) and `_lease_idx` (Branch B,
                # `acquired_token IS NOT NULL AND acquired_at < lease_cutoff`) and
                # combines them via BitmapOr. The naive form `acquired_at < lease_cutoff`
                # alone does not imply `acquired_token IS NOT NULL`, so the planner
                # cannot prove `_lease_idx` applies and falls back to a seq-scan.
                or_(
                    t.c.acquired_token.is_(None),
                    and_(t.c.acquired_token.is_not(None), t.c.acquired_at < lease_cutoff),
                ),
            )
            .order_by(t.c.next_attempt_at, t.c.id)
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

    async def delete_with_lease(
        self,
        conn: "AsyncConnection | None",
        message_id: int,
        acquired_token: uuid.UUID,
        *,
        dlq_payload: "Mapping[str, typing.Any] | None" = None,
    ) -> bool:
        """
        Delete *message_id* iff it still holds *acquired_token*. Returns True if deleted.

        Issues a single ``DELETE`` on *conn* with no explicit transaction wrapper — the
        production writer connection is configured ``isolation_level="AUTOCOMMIT"`` by
        :meth:`OutboxSubscriber._open_worker_resources` so each call is one round-trip.
        The lease guard rides on the ``WHERE acquired_token = …`` clause, not the
        transaction: a slow handler whose lease was reclaimed by a newer fetch finds
        ``rowcount == 0`` and is silently dropped, whether *conn* is in autocommit
        (production path) or in an outer transaction (tests). See :meth:`fetch` for why
        *conn* is ``AsyncConnection | None`` instead of ``AsyncConnection``.

        When *dlq_payload* is provided **and** the client was constructed with a
        ``dlq_table``, the statement becomes a single CTE that DELETEs the outbox row
        and INSERTs the audit copy into the DLQ atomically:

            WITH deleted AS (
                DELETE FROM <outbox> WHERE id=:id AND acquired_token=:token
                RETURNING id, queue, payload, headers, deliveries_count, created_at
            )
            INSERT INTO <dlq> (original_id, queue, payload, headers, deliveries_count,
                               created_at, failure_reason, last_exception)
            SELECT id, queue, payload, headers, deliveries_count, created_at,
                   :failure_reason, :last_exception
            FROM deleted;

        Lease-lost ⇒ ``deleted`` is empty ⇒ INSERT inserts nothing ⇒ ``rowcount == 0``,
        same observable as the no-DLQ path. A DLQ-write failure (schema mismatch,
        disk full) rolls back the whole statement, so the outbox row stays leased
        and is reclaimed when the lease expires — DLQ misconfiguration surfaces as
        outbox growth + ``lease_lost`` spikes rather than silently dropping audit
        data. ``dlq_payload`` carries ``{"failure_reason": str, "last_exception":
        str | None}``; the keys are required.
        """
        if conn is None:
            msg = "OutboxClient.delete_with_lease requires a live AsyncConnection (got None)"
            raise TypeError(msg)
        if dlq_payload is not None:
            if self._dlq_table is None:
                # P10: a dlq_payload with no dlq_table would silently degrade to a plain
                # DELETE — losing the audit row. If the broker/client wiring ever desyncs,
                # fail loudly rather than drop forensics.
                msg = (
                    "delete_with_lease received a dlq_payload but the client has no dlq_table; "
                    "refusing to drop the audit row silently"
                )
                raise RuntimeError(msg)
            stmt, params = self._build_dlq_cte_stmt(message_id, acquired_token, dlq_payload)
            result = await conn.execute(stmt, params)
            return (result.rowcount or 0) > 0
        t = self._table
        del_stmt = delete(t).where(t.c.id == message_id, t.c.acquired_token == acquired_token)
        result = await conn.execute(del_stmt)
        return (result.rowcount or 0) > 0

    def _build_dlq_cte_stmt(
        self,
        message_id: int,
        acquired_token: uuid.UUID,
        dlq_payload: "Mapping[str, typing.Any]",
    ) -> "tuple[typing.Any, dict[str, typing.Any]]":
        """
        Compose the single-statement DLQ CTE plus the parameter dict.

        Identifiers are quoted via the dialect's identifier preparer so reserved words
        and odd characters survive interpolation. The outbox/DLQ table names are
        application-controlled (from the user's ``MetaData``), not request-derived
        input, so the quoting is a robustness/correctness safeguard not a security
        boundary.
        """
        # ``self._dlq_table`` is guaranteed non-None at the only call site (the guard
        # in ``delete_with_lease``); local alias narrows the type for the formatting.
        dlq_table = self._dlq_table
        assert dlq_table is not None  # noqa: S101
        preparer = self._engine.dialect.identifier_preparer
        # ``format_table`` renders the schema-qualified, quoted name (``"app"."outbox"``)
        # when the Table carries a non-default ``schema=``. ``quote(table.name)`` dropped
        # the schema, so a ``MetaData(schema="app")`` deployment hit ``UndefinedTable`` on
        # every terminal failure (poison rows retry forever, the outbox grows) or silently
        # wrote to a same-named search_path table (B10).
        outbox_name = preparer.format_table(self._table)
        dlq_name = preparer.format_table(dlq_table)
        # S608: outbox_name / dlq_name come from application-defined SQLAlchemy
        # Table objects (not request input) and are quoted via the dialect's
        # identifier preparer — values flow through :bindparam placeholders.
        cte_sql = (
            f"WITH deleted AS ("  # noqa: S608
            f"DELETE FROM {outbox_name} "
            f"WHERE id = :message_id AND acquired_token = :acquired_token "
            f"RETURNING id, queue, payload, headers, deliveries_count, created_at, timer_id"
            f") "
            f"INSERT INTO {dlq_name} ("
            f"original_id, queue, payload, headers, deliveries_count, created_at, "
            f"failure_reason, last_exception, timer_id"
            f") "
            f"SELECT id, queue, payload, headers, deliveries_count, created_at, "
            f":failure_reason, :last_exception, timer_id "
            f"FROM deleted"
        )
        sql = text(cte_sql)
        params = {
            "message_id": message_id,
            "acquired_token": acquired_token,
            "failure_reason": dlq_payload["failure_reason"],
            "last_exception": dlq_payload["last_exception"],
        }
        return sql, params

    async def mark_pending_with_lease(
        self,
        conn: "AsyncConnection | None",
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

        Issues a single ``UPDATE`` on *conn* with no explicit transaction wrapper — the
        production writer connection is configured ``isolation_level="AUTOCOMMIT"`` by
        :meth:`OutboxSubscriber._open_worker_resources` so each call is one round-trip.
        ``next_attempt_at`` is computed server-side as ``now() + delay_seconds`` (DB-clock
        anchored) regardless of the transaction context. The lease guard rides on the
        ``WHERE acquired_token = …`` clause, not the transaction wrapping it. See
        :meth:`fetch` for why *conn* is ``AsyncConnection | None`` instead of
        ``AsyncConnection``.
        """
        if conn is None:
            msg = "OutboxClient.mark_pending_with_lease requires a live AsyncConnection (got None)"
            raise TypeError(msg)
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
        result = await conn.execute(stmt, {"delay": max(0.0, delay_seconds)})
        return (result.rowcount or 0) > 0

    async def validate_schema(self) -> None:
        """
        Validate that the database table(s) match the package's expected columns.

        Raises ``RuntimeError`` listing every mismatch across the outbox table and,
        when configured, the DLQ table. Opt-in: call from your startup hook or
        ``/health`` endpoint, not from ``broker.start()`` (so Alembic can run
        migrations against the same DB without blocking startup).
        """
        async with self._engine.connect() as conn:
            errors = await conn.run_sync(_validate_schema_sync, self._table)
            # S2: alembic's autogenerate diff compares index columns + uniqueness but NOT
            # the partial-index WHERE predicate, so a wrong postgresql_where slips through
            # and later breaks the producer's ON CONFLICT arbiter. Probe the predicates
            # directly against the live catalog.
            errors.extend(await conn.run_sync(_validate_index_predicates_sync, self._table))
            if self._dlq_table is not None:
                errors.extend(await conn.run_sync(_validate_dlq_schema_sync, self._dlq_table))
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
        timer_id=row["timer_id"],
    )


def _normalize_predicate(predicate: str) -> str:
    """Canonicalize a Postgres partial-index predicate for comparison (lowercase, drop parens/space)."""
    return " ".join(predicate.lower().replace("(", " ").replace(")", " ").split())


# Expected partial-index predicates, keyed by the index-name suffix make_outbox_table uses.
# These are what the fetch CTE and the producer's ON CONFLICT arbiter rely on (S2).
_EXPECTED_INDEX_PREDICATES = {
    "_pending_idx": "acquired_token is null",
    "_timer_id_uq": "timer_id is not null",
    "_lease_idx": "acquired_token is not null",
}

_INDEX_PREDICATE_QUERY = text(
    "SELECT c.relname AS index_name, pg_get_expr(i.indpred, i.indrelid) AS predicate "
    "FROM pg_index i "
    "JOIN pg_class c ON c.oid = i.indexrelid "
    "JOIN pg_class t ON t.oid = i.indrelid "
    "JOIN pg_namespace n ON n.oid = t.relnamespace "
    "WHERE t.relname = :table AND n.nspname = COALESCE(:schema, current_schema()) "
    "AND i.indpred IS NOT NULL",
)


def _validate_index_predicates_sync(connection: "Connection", table: "Table") -> list[str]:
    """
    Compare the live partial-index WHERE predicates against what the package expects (S2).

    Alembic's index diff ignores ``postgresql_where``, so a drifted predicate (e.g. a
    ``{table}_timer_id_uq`` built ``WHERE timer_id IS NULL`` instead of ``IS NOT NULL``)
    passes :func:`_run_validate` yet breaks ``ON CONFLICT`` arbiter inference at publish
    time. Missing / non-partial indexes are left to the alembic diff; this only flags a
    predicate mismatch on an index that does exist.
    """
    rows = (
        connection.execute(
            _INDEX_PREDICATE_QUERY,
            {"table": table.name, "schema": table.schema},
        )
        .mappings()
        .all()
    )
    live = {row["index_name"]: _normalize_predicate(row["predicate"]) for row in rows}
    errors: list[str] = []
    for suffix, want in _EXPECTED_INDEX_PREDICATES.items():
        name = f"{table.name}{suffix}"
        got = live.get(name)
        if got is not None and got != want:
            errors.append(f"index {name!r} has wrong partial predicate: expected '{want}', got '{got}'")
    return errors


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
    """
    Run Alembic's autogenerate diff against the live DB and surface any "missing schema" drift.

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
    # matching the schema-qualified DLQ CTE in ``_build_dlq_cte`` (B10).
    canonical_metadata = MetaData(schema=table.schema)
    canonical_factory(canonical_metadata, table.name)

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
