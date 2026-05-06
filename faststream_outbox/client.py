"""
Postgres outbox client.

All read/write paths against the outbox table live here. The fetch query is the
load-bearing piece: a single CTE that selects due rows ``FOR UPDATE SKIP LOCKED``
and immediately ``UPDATE``s them to ``processing`` with a fresh lease token,
``RETURNING`` the row in one round-trip.

Every terminal write (``delete_with_lease``, ``mark_pending_with_lease``) filters
on ``acquired_token`` so a slow handler whose lease was reclaimed by
``release_stuck`` can no longer mutate that row.
"""

import datetime as _dt
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING

from sqlalchemy import (
    Float,
    and_,
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
from faststream_outbox.schema import OutboxState


if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy import Connection, Table
    from sqlalchemy.ext.asyncio import AsyncEngine


@dataclass(frozen=True)
class FetchResult:
    """Outcome of a single fetch cycle, used to drive the subscriber's idle backoff."""

    rows: list[OutboxInnerMessage]


class OutboxClient:
    def __init__(self, engine: "AsyncEngine", outbox_table: "Table") -> None:
        self._engine = engine
        self._table = outbox_table
        # Stable advisory-lock key derived from the table name; ``hashtext`` returns int4.
        self._advisory_lock_sql = text(
            f"SELECT pg_try_advisory_xact_lock(hashtext('faststream_outbox:{outbox_table.name}'))"
        )

    @property
    def table(self) -> "Table":
        return self._table

    async def fetch(self, queues: "Sequence[str]", *, limit: int) -> list[OutboxInnerMessage]:
        """
        Atomically claim up to *limit* due rows for the given queue names.

        Returns the freshly-leased rows. Each row carries ``acquired_token`` which the
        worker loop must echo back on the terminal ``DELETE``/``UPDATE``.
        """
        if not queues:
            return []
        token = uuid.uuid4()
        t = self._table

        ready = (
            select(t.c.id)
            .where(
                t.c.state == OutboxState.PENDING.value,
                t.c.next_attempt_at <= func.now(),
                or_(*(t.c.queue == q for q in queues)),
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
                state=OutboxState.PROCESSING.value,
                acquired_at=func.now(),
                acquired_token=token,
                deliveries_count=t.c.deliveries_count + 1,
            )
            .returning(*t.c)
        )
        async with self._engine.begin() as conn:
            result = await conn.execute(stmt)
            rows = result.mappings().all()
        return [_row_to_message(dict(row)) for row in rows]

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
        next_attempt_at: _dt.datetime,
        attempts_count: int,
        first_attempt_at: _dt.datetime,
        last_attempt_at: _dt.datetime,
    ) -> bool:
        """
        Move *message_id* back to ``pending`` for retry, iff it still holds the lease.

        Returns True if the row was updated.
        """
        t = self._table
        stmt = (
            update(t)
            .where(t.c.id == message_id, t.c.acquired_token == acquired_token)
            .values(
                state=OutboxState.PENDING.value,
                next_attempt_at=next_attempt_at,
                attempts_count=attempts_count,
                first_attempt_at=first_attempt_at,
                last_attempt_at=last_attempt_at,
                acquired_at=None,
                acquired_token=None,
            )
        )
        async with self._engine.begin() as conn:
            result = await conn.execute(stmt)
        return (result.rowcount or 0) > 0

    async def release_stuck(self, *, timeout_seconds: float) -> int:
        """
        Flip ``processing`` rows back to ``pending`` once their lease is older than *timeout_seconds*.

        Wrapped in ``pg_try_advisory_xact_lock`` so multiple processes don't fight over
        the same rows. Returns the number of rows released (``0`` if the lock was not
        acquired — another process is doing the work).
        """
        t = self._table
        # ``make_interval(secs => :timeout)`` keeps the cutoff computation server-side so
        # release_stuck windows are immune to clock skew between worker and DB hosts.
        stale_cutoff = func.now() - func.make_interval(0, 0, 0, 0, 0, 0, bindparam("timeout", type_=Float))
        stmt = (
            update(t)
            .where(
                and_(
                    t.c.state == OutboxState.PROCESSING.value,
                    t.c.acquired_at.isnot(None),
                    t.c.acquired_at < stale_cutoff,
                )
            )
            .values(
                state=OutboxState.PENDING.value,
                acquired_at=None,
                acquired_token=None,
            )
        )
        async with self._engine.begin() as conn:
            lock_result = await conn.execute(self._advisory_lock_sql)
            if not lock_result.scalar():
                return 0
            result = await conn.execute(stmt, {"timeout": timeout_seconds})
        return result.rowcount or 0

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
        state=OutboxState(row["state"]),
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
