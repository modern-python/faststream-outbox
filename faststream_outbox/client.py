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
    bindparam,
    delete,
    func,
    inspect,
    or_,
    select,
    text,
    update,
)

from faststream_outbox.message import OutboxInnerMessage


if TYPE_CHECKING:
    from collections.abc import Sequence

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
    insp = inspect(connection)
    errors: list[str] = []
    if not insp.has_table(table.name):
        errors.append(f"table '{table.name}' does not exist")
        return errors
    actual = {c["name"] for c in insp.get_columns(table.name)}
    expected = {c.name for c in table.columns}
    missing = expected - actual
    if missing:
        errors.append(f"table '{table.name}' missing columns: {sorted(missing)}")
    return errors
