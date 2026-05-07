"""
Outbox table factory.

The package does not own the schema. Users attach the returned ``Table`` to their own
``MetaData`` and write Alembic migrations themselves. Recommended companion partial
index (create it in your migration alongside the table)::

    CREATE INDEX outbox_pending_idx ON outbox (queue, next_attempt_at)
      WHERE acquired_token IS NULL;

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
    Build the outbox ``Table`` and attach it to *metadata*.

    The user wires the returned table into their own SQLAlchemy ``MetaData`` so it is
    discovered by Alembic's autogenerate. They are responsible for the actual migration.

    The recommended composite partial index for fetch performance is documented in the
    module docstring above; create it explicitly in your migration.
    """
    return Table(
        table_name,
        metadata,
        Column("id", BigInteger, primary_key=True, autoincrement=True),
        Column("queue", String(255), nullable=False, index=True),
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
        Index(f"{table_name}_next_attempt_at_idx", "next_attempt_at"),
    )
