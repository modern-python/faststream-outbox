"""
Outbox table factory.

The package does not own the schema — users attach the returned ``Table`` to their own
``MetaData`` and write Alembic migrations themselves — but the partial indexes that the
fetch query relies on **are** declared on the table, so Alembic autogenerate picks them
up and users can't forget them.

A row is "available" iff its lease is unset (``acquired_token IS NULL``) or its lease
is expired (``acquired_at < now() - lease_ttl_seconds``). The fetch query reclaims
both cases inline; there is no separate state column or background reaper.
"""

from typing import TYPE_CHECKING

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
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
    # truncate identifiers. Guard on the LONGEST identifier derived from table_name:
    # the NOTIFY channel "outbox_<t>" (7-byte prefix) AND the index / constraint names
    # ("<t>_pending_idx", "<t>_timer_id_uq", "<t>_lease_idx", "<t>_lease_ck"; suffixes
    # up to ~12 bytes). The index suffixes are longer than the channel prefix, so a
    # table_name that fits the channel can still overflow an index name and fail at
    # CREATE INDEX time (P7).
    name_bytes = table_name.encode("utf-8")
    derived = (
        b"outbox_" + name_bytes,
        name_bytes + b"_pending_idx",
        name_bytes + b"_timer_id_uq",
        name_bytes + b"_lease_idx",
        name_bytes + b"_lease_ck",
    )
    longest = max(derived, key=len)
    if len(longest) > _POSTGRES_IDENT_MAX_BYTES:
        msg = (
            f"table_name {table_name!r} too long: the derived identifier "
            f"{longest.decode('utf-8', 'replace')!r} must fit in {_POSTGRES_IDENT_MAX_BYTES} bytes "
            f"(got {len(longest)})"
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
        # P8: a lease is either fully set or fully unset. A half-set lease (e.g. a
        # manual UPDATE that sets acquired_at but not acquired_token) would be
        # permanently invisible to fetch, cancel_timer, and the metrics — this
        # CHECK makes that state unrepresentable.
        CheckConstraint(
            "(acquired_token IS NULL) = (acquired_at IS NULL)",
            name=f"{table_name}_lease_ck",
        ),
    )
    # Partial index that backs the fetch query's hot branch
    # (`WHERE acquired_token IS NULL AND queue = ? AND next_attempt_at <= now()`).
    # The expired-lease branch is covered by `_lease_idx` below.
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


def make_dlq_table(metadata: "MetaData", table_name: str = "outbox_dlq") -> Table:
    """
    Build the dead-letter-queue ``Table`` and attach it to *metadata*.

    Opt-in companion to :func:`make_outbox_table`. Pass the returned table to
    ``OutboxBroker(..., dlq_table=...)`` to enable archive-on-terminal-failure: the
    broker copies ``payload`` / ``headers`` / failure context into this table in the
    same Postgres statement as the outbox ``DELETE`` (atomic via CTE), so audit data
    survives even if the worker crashes between the DELETE and a follow-up insert.

    No FK to the outbox table — the row is gone in the same transaction, so the
    constraint would be unsatisfiable. ``original_id`` is a plain BigInteger for
    operator forensics; not unique (re-delivered ``timer_id`` rows could legitimately
    fail twice). No LISTEN/NOTIFY channel — nobody polls the DLQ, so the 63-byte
    identifier check in :func:`make_outbox_table` does not apply here.
    """
    table = Table(
        table_name,
        metadata,
        Column("id", BigInteger, primary_key=True, autoincrement=True),
        Column("original_id", BigInteger, nullable=False),
        Column("queue", String(255), nullable=False),
        Column("payload", LargeBinary, nullable=False),
        Column("headers", JSONB, nullable=True),
        Column("deliveries_count", BigInteger, nullable=False),
        Column("created_at", DateTime(timezone=True), nullable=False),
        Column("failed_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
        # 64 gives breathing room past the current 14-byte max ("retry_terminal") so the
        # canonical set can grow without a migration that rewrites every audit row.
        Column("failure_reason", String(64), nullable=False),
        Column("last_exception", String, nullable=True),
        # P9: carry the originating timer_id so a terminally-failed timer keeps its
        # business dedup key in the audit trail. Nullable — most rows have none.
        Column("timer_id", String(255), nullable=True),
    )
    Index(f"{table_name}_queue_failed_idx", table.c.queue, table.c.failed_at)
    return table
