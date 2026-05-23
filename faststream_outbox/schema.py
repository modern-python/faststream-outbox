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

# Postgres' NAMEDATALEN-1 — the maximum identifier byte length, which bounds
# the LISTEN/NOTIFY channel name `outbox_<table>`.
_POSTGRES_IDENT_MAX_BYTES = 63


def make_outbox_table(metadata: "MetaData", table_name: str = "outbox") -> Table:
    """
    Build the outbox ``Table`` (with the partial fetch index) and attach it to *metadata*.

    The user wires the returned table into their own SQLAlchemy ``MetaData`` so it is
    discovered by Alembic's autogenerate. They are responsible for the actual migration.

    Raises ``ValueError`` if *table_name* would push the NOTIFY channel
    ``outbox_<table_name>`` past Postgres' 63-byte identifier limit (NAMEDATALEN-1).
    """
    # Byte length, not char count — UTF-8 multibyte chars expand and would silently
    # truncate the channel identifier on one side of LISTEN/NOTIFY.
    encoded_channel = b"outbox_" + table_name.encode("utf-8")
    if len(encoded_channel) > _POSTGRES_IDENT_MAX_BYTES:
        msg = (
            f"table_name {table_name!r} too long for NOTIFY channel "
            f"'outbox_<table_name>': must fit in {_POSTGRES_IDENT_MAX_BYTES} bytes "
            f"(got {len(encoded_channel)})"
        )
        raise ValueError(msg)
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
        Column("timer_id", String(255), nullable=True),
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
    # Partial unique index that backs `publish(..., timer_id=...)`'s ON CONFLICT DO NOTHING
    # and the (queue, timer_id) lookup in `cancel_timer`. Only enforced when timer_id is set,
    # so non-timer rows remain unconstrained.
    Index(
        f"{table_name}_timer_id_uq",
        table.c.queue,
        table.c.timer_id,
        unique=True,
        postgresql_where=table.c.timer_id.is_not(None),
    )
    # Partial index that backs the fetch query's expired-lease branch
    # (`WHERE acquired_token IS NOT NULL AND acquired_at < lease_cutoff`). Without it,
    # the OR's second arm forces a seq-scan of the whole table on every fetch — invisible
    # under healthy steady-state (Branch A dominates) but the tail grows linearly with
    # table size when handlers wedge or `lease_ttl_seconds < P99`. Trade-off: every fetch
    # UPDATE rewrites (acquired_token, acquired_at), so this index pays write amplification
    # proportional to the claim rate.
    Index(
        f"{table_name}_lease_idx",
        table.c.queue,
        table.c.acquired_at,
        postgresql_where=table.c.acquired_token.is_not(None),
    )
    return table
