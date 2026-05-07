"""
Outbox table factory.

The package does not own the schema — users attach the returned ``Table`` to their own
``MetaData`` and write Alembic migrations themselves — but the partial index that the
fetch query relies on **is** declared on the table, so Alembic autogenerate picks it up
and users can't forget it.

A row is "available" iff its lease is unset (``acquired_token IS NULL``) or its lease
is expired (``acquired_at < now() - lease_ttl_seconds``). The fetch query reclaims
both cases inline; there is no separate state column or background reaper.
"""

from typing import TYPE_CHECKING

from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    Index,
    LargeBinary,
    String,
    Table,
    Uuid,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB


if TYPE_CHECKING:
    from sqlalchemy import MetaData


def make_outbox_table(metadata: "MetaData", table_name: str = "outbox") -> Table:
    """
    Build the outbox ``Table`` (with the partial fetch index) and attach it to *metadata*.

    The user wires the returned table into their own SQLAlchemy ``MetaData`` so it is
    discovered by Alembic's autogenerate. They are responsible for the actual migration.
    """
    table = Table(
        table_name,
        metadata,
        Column("id", BigInteger, primary_key=True, autoincrement=True),
        Column("queue", String(255), nullable=False),
        Column("payload", LargeBinary, nullable=False),
        Column("headers", JSONB, nullable=True),
        Column("attempts_count", BigInteger, nullable=False, server_default="0"),
        Column("deliveries_count", BigInteger, nullable=False, server_default="0"),
        Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
        Column("next_attempt_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
        Column("first_attempt_at", DateTime(timezone=True), nullable=True),
        Column("last_attempt_at", DateTime(timezone=True), nullable=True),
        Column("acquired_at", DateTime(timezone=True), nullable=True),
        Column("acquired_token", Uuid, nullable=True),
    )
    # Partial index that backs the fetch query's hot branch
    # (`WHERE acquired_token IS NULL AND queue = ? AND next_attempt_at <= now()`).
    # Lease-expired rows fall back to a sequential scan, which is fine — they're rare.
    Index(
        f"{table_name}_pending_idx",
        table.c.queue,
        table.c.next_attempt_at,
        postgresql_where=table.c.acquired_token.is_(None),
    )
    return table
