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
import datetime as dt
import uuid
from typing import TYPE_CHECKING

from sqlalchemy import (
    ARRAY,
    Float,
    Integer,
    String,
    bindparam,
    cast,
    delete,
    func,
    insert,
    select,
    text,
    true,
    union_all,
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

    from sqlalchemy import CTE, ColumnElement, Insert, Table, Update
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
    ) -> set[tuple[int, uuid.UUID]]: ...

    @abc.abstractmethod
    async def mark_pending_with_lease(
        self,
        conn: "AsyncConnection | None",
        message_id: int,
        acquired_token: uuid.UUID,
        *,
        delay_seconds: float,
        attempts_count: int,
        first_attempt_at: dt.datetime,
        last_attempt_at: dt.datetime,
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


def _build_fetch_stmt(outbox: "Table") -> "Update":
    """Build the claim statement once; queues, limit, lease TTL and the new token are bind parameters.

    Each queue gets two ``LATERAL`` branches, unleased rows and expired leases, and each branch is an
    ordered ``LIMIT n FOR UPDATE SKIP LOCKED`` scan, so Postgres stops after ``n`` unlocked rows instead of
    sorting the whole backlog. See ADR 0003 for why the query has this shape.
    """
    t = outbox
    limit = bindparam("limit", type_=Integer)
    # ``make_interval(secs => :lease_ttl)`` keeps the cutoff server-side, immune to worker/DB clock skew.
    lease_cutoff = func.now() - func.make_interval(0, 0, 0, 0, 0, 0, bindparam("lease_ttl", type_=Float))
    queue_array = ARRAY(String)
    queue_name = func.unnest(cast(bindparam("queues", type_=queue_array), queue_array)).label("name")
    queues = select(queue_name).distinct().cte("queues")

    def branch(name: str, *lease_state: "ColumnElement[bool]") -> "CTE":
        per_queue = (
            select(t.c.id, t.c.next_attempt_at)
            .where(t.c.queue == queues.c.name, t.c.next_attempt_at <= func.now(), *lease_state)
            .order_by(t.c.next_attempt_at, t.c.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
            .lateral(f"{name}_rows")
        )
        return select(per_queue.c.id, per_queue.c.next_attempt_at).select_from(queues.join(per_queue, true())).cte(name)

    pending = branch("pending", t.c.acquired_token.is_(None))
    expired = branch("expired", t.c.acquired_token.is_not(None), t.c.acquired_at < lease_cutoff)
    candidates = union_all(
        select(pending.c.id, pending.c.next_attempt_at), select(expired.c.id, expired.c.next_attempt_at)
    ).subquery("candidates")
    ready = select(candidates.c.id).order_by(candidates.c.next_attempt_at, candidates.c.id).limit(limit).cte("ready")
    return (
        update(t)
        .where(t.c.id.in_(select(ready.c.id)))
        .values(
            acquired_at=func.now(),
            acquired_token=bindparam("token", type_=t.c.acquired_token.type),
            deliveries_count=t.c.deliveries_count + 1,
        )
        .returning(*t.c)
    )


def _build_dlq_cte(outbox: "Table", dlq: "Table") -> "Insert":
    """Build the DELETE-then-archive CTE once; the row id, lease token and failure context are bind parameters."""
    deleted = (
        delete(outbox)
        .where(outbox.c.id == bindparam("message_id"), outbox.c.acquired_token == bindparam("acquired_token"))
        .returning(*(outbox.c[col] for col, _ in _DLQ_PROJECTION))
        .cte("deleted")
    )
    archived = select(
        *(deleted.c[col] for col, _ in _DLQ_PROJECTION),
        *(bindparam(col, type_=dlq.c[col].type) for col in _DLQ_INJECTED_COLUMNS),
    )
    return insert(dlq).from_select([*(col for _, col in _DLQ_PROJECTION), *_DLQ_INJECTED_COLUMNS], archived)


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
        self._fetch_stmt = _build_fetch_stmt(outbox_table)
        self._dlq_cte = None if dlq_table is None else _build_dlq_cte(outbox_table, dlq_table)

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
        params = {"queues": list(queues), "limit": limit, "lease_ttl": lease_ttl_seconds, "token": uuid.uuid4()}
        async with conn.begin():
            result = await conn.execute(self._fetch_stmt, params)
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
            if self._dlq_cte is None:
                # P10: a dlq_payload with no dlq_table would silently degrade to a plain
                # DELETE — losing the audit row. If the broker/client wiring ever desyncs,
                # fail loudly rather than drop forensics.
                msg = (
                    "delete_with_lease received a dlq_payload but the client has no dlq_table; "
                    "refusing to drop the audit row silently"
                )
                raise RuntimeError(msg)
            params = {
                "message_id": message_id,
                "acquired_token": acquired_token,
                "failure_reason": dlq_payload["failure_reason"],
                "last_exception": dlq_payload["last_exception"],
            }
            result = await conn.execute(self._dlq_cte, params)
            return (result.rowcount or 0) > 0
        t = self._table
        del_stmt = delete(t).where(t.c.id == message_id, t.c.acquired_token == acquired_token)
        result = await conn.execute(del_stmt)
        return (result.rowcount or 0) > 0

    async def delete_batch_with_lease(
        self,
        conn: "AsyncConnection | None",
        pairs: "Sequence[tuple[int, uuid.UUID]]",
    ) -> set[tuple[int, uuid.UUID]]:
        """Delete every ``(id, acquired_token)`` pair that still holds its lease in one statement.

        Returns the ``(id, acquired_token)`` pairs actually deleted. A pair whose lease was
        reclaimed by a newer fetch matches nothing and is absent from the returned set -- the
        caller treats it as ``lease_lost``. The SQL text doesn't depend on ``len(pairs)``. One
        round-trip on *conn* (production writer is AUTOCOMMIT), atomic: all matching rows delete
        or the statement fails wholesale. Used only by the batched terminal-flush path; the DLQ and per-row paths keep
        :meth:`delete_with_lease`. See :meth:`fetch` for why *conn* is ``AsyncConnection | None``.
        """
        if conn is None:
            msg = "OutboxClient.delete_batch_with_lease requires a live AsyncConnection (got None)"
            raise TypeError(msg)
        if not pairs:
            return set()
        t = self._table
        ids_type, tokens_type = ARRAY(t.c.id.type), ARRAY(t.c.acquired_token.type)
        wanted = (
            func.unnest(
                cast(bindparam("ids", [pid for pid, _ in pairs], type_=ids_type), ids_type),
                cast(bindparam("tokens", [ptok for _, ptok in pairs], type_=tokens_type), tokens_type),
            )
            .table_valued("id", "token")
            .render_derived(name="wanted")
        )
        stmt = (
            delete(t)
            .where(t.c.id == wanted.c.id, t.c.acquired_token == wanted.c.token)
            .returning(t.c.id, t.c.acquired_token)
        )
        result = await conn.execute(stmt)
        return {(row_id, token) for row_id, token in result.all()}

    async def mark_pending_with_lease(
        self,
        conn: "AsyncConnection | None",
        message_id: int,
        acquired_token: uuid.UUID,
        *,
        delay_seconds: float,
        attempts_count: int,
        first_attempt_at: dt.datetime,
        last_attempt_at: dt.datetime,
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
