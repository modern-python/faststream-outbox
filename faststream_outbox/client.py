"""Postgres outbox client.

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
import asyncio
import datetime as _dt
import uuid
from typing import TYPE_CHECKING

from sqlalchemy import (
    ARRAY,
    Float,
    String,
    and_,
    any_,
    bindparam,
    delete,
    func,
    or_,
    select,
    text,
    tuple_,
    update,
)
from sqlalchemy.ext.asyncio import AsyncSession

from faststream_outbox import schema_validation
from faststream_outbox.message import OutboxInnerMessage
from faststream_outbox.schema import (
    _DLQ_INJECTED_COLUMNS,
    _DLQ_PROJECTION,
    validate_table_identifiers,
)


if TYPE_CHECKING:
    import typing
    from collections.abc import Mapping, Sequence

    from sqlalchemy import Table
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine


# Upper bound on the ``ping()`` liveness probe so a half-dead socket can't hang it.
_PING_TIMEOUT_SECONDS = 5.0


class AbstractOutboxClient(abc.ABC):
    """Outbox client interface.

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
    async def delete_batch_with_lease(
        self,
        conn: "AsyncConnection | None",
        pairs: "Sequence[tuple[int, uuid.UUID]]",
    ) -> set[int]: ...

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
    async def cancel_timer(self, *, queue: str, timer_id: str, session: "AsyncSession") -> bool: ...

    @abc.abstractmethod
    async def fetch_unprocessed(
        self,
        *,
        session: "AsyncSession",
        queue: str | None = None,
        limit: int = 1000,
    ) -> list[OutboxInnerMessage]: ...

    @abc.abstractmethod
    async def validate_schema(self, *, check_autovacuum: bool = False) -> None: ...

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
        # F3-02: the 63-byte identifier guard otherwise lives only in make_outbox_table;
        # validate here too so a directly-constructed or reflected Table can't slip an
        # over-long NOTIFY channel / index name past it.
        validate_table_identifiers(outbox_table.name)
        self._engine = engine
        self._table = outbox_table
        self._dlq_table = dlq_table

    @property
    def table(self) -> "Table":
        return self._table

    @property
    def engine(self) -> "AsyncEngine":
        """The underlying ``AsyncEngine``.

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
        """Atomically claim up to *limit* available rows for the given queue names on *conn*.

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
        """Delete *message_id* iff it still holds *acquired_token*. Returns True if deleted.

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
        """Compose the single-statement DLQ CTE plus the parameter dict.

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
        # Column lists derive from _DLQ_PROJECTION (projected pairs first, then the
        # injected failure-context columns) so the real CTE and the fake stay in lockstep
        # off one source. INSERT and SELECT share the same order, so the named columns map
        # positionally.
        returning_cols = ", ".join(out for out, _ in _DLQ_PROJECTION)
        insert_cols = ", ".join([dlq for _, dlq in _DLQ_PROJECTION] + list(_DLQ_INJECTED_COLUMNS))
        select_exprs = ", ".join(
            [out for out, _ in _DLQ_PROJECTION] + [f":{col}" for col in _DLQ_INJECTED_COLUMNS],
        )
        # S608: outbox_name / dlq_name come from application-defined SQLAlchemy Table
        # objects (not request input) and are quoted via the dialect's identifier preparer;
        # the column names come from _DLQ_PROJECTION constants. Values flow through
        # :bindparam placeholders.
        cte_sql = (
            f"WITH deleted AS ("  # noqa: S608
            f"DELETE FROM {outbox_name} "
            f"WHERE id = :message_id AND acquired_token = :acquired_token "
            f"RETURNING {returning_cols}"
            f") "
            f"INSERT INTO {dlq_name} ({insert_cols}) "
            f"SELECT {select_exprs} "
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

    async def delete_batch_with_lease(
        self,
        conn: "AsyncConnection | None",
        pairs: "Sequence[tuple[int, uuid.UUID]]",
    ) -> set[int]:
        """Delete every ``(id, acquired_token)`` pair that still holds its lease in one statement.

        Returns the set of ids actually deleted. A row whose lease was reclaimed by a newer
        fetch is absent from ``pairs``' matches and thus absent from the returned set -- the
        caller treats it as ``lease_lost``. One round-trip on *conn* (production writer is
        AUTOCOMMIT), atomic: all matching rows delete or the statement fails wholesale. Used
        only by the batched terminal-flush path; the DLQ and per-row paths keep
        :meth:`delete_with_lease`. See :meth:`fetch` for why *conn* is ``AsyncConnection | None``.
        """
        if conn is None:
            msg = "OutboxClient.delete_batch_with_lease requires a live AsyncConnection (got None)"
            raise TypeError(msg)
        if not pairs:
            return set()
        t = self._table
        stmt = (
            delete(t)
            .where(tuple_(t.c.id, t.c.acquired_token).in_([(pid, ptok) for pid, ptok in pairs]))
            .returning(t.c.id)
        )
        result = await conn.execute(stmt)
        return {row_id for (row_id,) in result.all()}

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
        """Release the lease on *message_id* and reschedule it for retry, iff it still holds the lease.

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

    async def cancel_timer(self, *, queue: str, timer_id: str, session: "AsyncSession") -> bool:
        """Delete a not-yet-leased ``(queue, timer_id)`` row on the caller's session."""
        if not isinstance(session, AsyncSession):
            msg = "OutboxClient.cancel_timer requires an sqlalchemy.ext.asyncio.AsyncSession"
            raise TypeError(msg)
        t = self._table
        stmt = delete(t).where(
            t.c.queue == queue,
            t.c.timer_id == timer_id,
            t.c.acquired_token.is_(None),
        )
        result = await session.execute(stmt)
        return (result.rowcount or 0) > 0  # ty: ignore[unresolved-attribute]

    async def fetch_unprocessed(
        self,
        *,
        session: "AsyncSession",
        queue: str | None = None,
        limit: int = 1000,
    ) -> list[OutboxInnerMessage]:
        """Read up to *limit* rows (optionally filtered by *queue*) on the caller's session."""
        if not isinstance(session, AsyncSession):
            msg = "OutboxClient.fetch_unprocessed requires an sqlalchemy.ext.asyncio.AsyncSession"
            raise TypeError(msg)
        if limit < 1:
            # F4-04: a non-positive limit otherwise hits SQL (LIMIT -1 → DB error) or
            # silently returns nothing (LIMIT 0); reject it up front, consistently with
            # the fake.
            msg = f"limit must be >= 1, got {limit}"
            raise ValueError(msg)
        t = self._table
        stmt = select(*t.c).order_by(t.c.id).limit(limit)
        if queue is not None:
            stmt = stmt.where(t.c.queue == queue)
        result = await session.execute(stmt)
        return [_row_to_message(dict(row)) for row in result.mappings().all()]

    async def validate_schema(self, *, check_autovacuum: bool = False) -> None:
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
        await schema_validation.validate_schema(
            self._engine,
            self._table,
            dlq_table=self._dlq_table,
            check_autovacuum=check_autovacuum,
        )

    async def ping(self) -> bool:
        # Bound the probe: an unwrapped connect+SELECT 1 against a half-dead TCP socket
        # can hang on the kernel keepalive default (~hours), defeating ping()'s purpose
        # as a liveness check (F2-12; mirrors the LISTEN health probe's wait_for).
        try:
            async with asyncio.timeout(_PING_TIMEOUT_SECONDS), self._engine.connect() as conn:
                await conn.execute(text("SELECT 1"))
        except Exception:  # noqa: BLE001  # includes TimeoutError on a hung probe
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
