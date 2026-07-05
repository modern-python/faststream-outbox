"""Outbox table factory.

The package does not own the schema — users attach the returned ``Table`` to their own
``MetaData`` and write Alembic migrations themselves — and the partial indexes that the
fetch query relies on, plus the ``<table>_lease_ck`` CHECK, are declared on the table.

Autogenerate brings these up fully **only on a fresh ``create_table``** (which renders
the CHECK and each partial index with its ``postgresql_where``). On an **incremental**
migration onto a pre-existing table, Alembic's Postgres comparator ignores
``postgresql_where`` and has no check-constraint comparator at all, so a drifted/non-partial
predicate or a missing CHECK ships silently. The opt-in :meth:`OutboxClient.validate_schema`
is the backstop for that drift — it probes the live ``pg_catalog`` predicates and the
``<table>_lease_ck`` definition directly.

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

# Single source for the identifiers derived from a table name. Used by the length
# guard and the Index/CheckConstraint names below, and imported by ``client.py``'s
# schema-validation probes — so adding/renaming an index touches exactly one place
# (F6-11). The NOTIFY channel is ``f"{_CHANNEL_PREFIX}{table_name}"``.
_CHANNEL_PREFIX = "outbox_"
_PENDING_IDX_SUFFIX = "_pending_idx"
_TIMER_ID_UQ_SUFFIX = "_timer_id_uq"
_LEASE_IDX_SUFFIX = "_lease_idx"
_LEASE_CK_SUFFIX = "_lease_ck"


def validate_table_identifiers(table_name: str) -> None:
    """Raise ``ValueError`` if any identifier derived from *table_name* exceeds Postgres' 63-byte limit.

    Byte length, not char count — UTF-8 multibyte chars expand and would silently truncate
    identifiers. Guards the LONGEST derived identifier: the NOTIFY channel ``outbox_<t>`` AND
    the index/constraint names (``<t>_pending_idx`` etc., longer than the channel prefix), so a
    name that fits the channel can still overflow an index name and fail at CREATE INDEX time (P7).
    Called by :func:`make_outbox_table` and ``OutboxClient.__init__`` so a directly-constructed or
    reflected ``Table`` can't bypass the guard (F3-02).
    """
    derived = tuple(
        ident.encode("utf-8")
        for ident in (
            f"{_CHANNEL_PREFIX}{table_name}",
            f"{table_name}{_PENDING_IDX_SUFFIX}",
            f"{table_name}{_TIMER_ID_UQ_SUFFIX}",
            f"{table_name}{_LEASE_IDX_SUFFIX}",
            f"{table_name}{_LEASE_CK_SUFFIX}",
        )
    )
    longest = max(derived, key=len)
    if len(longest) > _POSTGRES_IDENT_MAX_BYTES:
        msg = (
            f"table_name {table_name!r} too long: the derived identifier "
            f"{longest.decode('utf-8', 'replace')!r} must fit in {_POSTGRES_IDENT_MAX_BYTES} bytes "
            f"(got {len(longest)})"
        )
        raise ValueError(msg)


def make_outbox_table(metadata: "MetaData", table_name: str = "outbox") -> Table:
    """Build the outbox ``Table`` (with the partial fetch index) and attach it to *metadata*.

    The user wires the returned table into their own SQLAlchemy ``MetaData`` so it is
    discovered by Alembic's autogenerate. They are responsible for the actual migration.

    Raises ``ValueError`` if *table_name* would push any derived identifier past
    Postgres' 63-byte limit (NAMEDATALEN-1). The binding identifier is the longest
    one derived — usually an index/constraint name (``<table_name>_pending_idx``,
    ``<table_name>_timer_id_uq``), which is longer than the NOTIFY channel
    ``outbox_<table_name>``, so a name that fits the channel can still overflow an
    index name. See ``validate_table_identifiers``.
    """
    validate_table_identifiers(table_name)
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
            name=f"{table_name}{_LEASE_CK_SUFFIX}",
        ),
    )
    # Partial index that backs the fetch query's hot branch
    # (`WHERE acquired_token IS NULL AND queue = ? AND next_attempt_at <= now()`).
    # The expired-lease branch is covered by `_lease_idx` below.
    Index(
        f"{table_name}{_PENDING_IDX_SUFFIX}",
        table.c.queue,
        table.c.next_attempt_at,
        postgresql_where=table.c.acquired_token.is_(None),
    )
    # Partial unique index that backs `publish(..., timer_id=...)`'s ON CONFLICT DO NOTHING
    # and the (queue, timer_id) lookup in `cancel_timer`. Only enforced when timer_id is set,
    # so non-timer rows remain unconstrained.
    Index(
        f"{table_name}{_TIMER_ID_UQ_SUFFIX}",
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
        f"{table_name}{_LEASE_IDX_SUFFIX}",
        table.c.queue,
        table.c.acquired_at,
        postgresql_where=table.c.acquired_token.is_not(None),
    )
    return table


# Outbox-row columns copied verbatim into the DLQ archive row on terminal failure, as
# ``(outbox_column, dlq_column)`` pairs. The single source both the real client's DLQ
# CTE (``OutboxClient._build_dlq_cte_stmt``) and the fake (``FakeOutboxClient.delete_with_lease``)
# build from, so a DLQ column change is one edit here instead of hand-kept parity in two
# substrates. ``failed_at`` is not listed — it rides the DLQ column's ``server_default``.
_DLQ_PROJECTION: tuple[tuple[str, str], ...] = (
    ("id", "original_id"),
    ("queue", "queue"),
    ("payload", "payload"),
    ("headers", "headers"),
    ("deliveries_count", "deliveries_count"),
    ("created_at", "created_at"),
    ("timer_id", "timer_id"),
)

# DLQ columns supplied by the caller (failure context), not copied from the outbox row.
# Their names double as the bind-parameter names on the real path.
_DLQ_INJECTED_COLUMNS: tuple[str, ...] = ("failure_reason", "last_exception")


def make_dlq_table(metadata: "MetaData", table_name: str = "outbox_dlq") -> Table:
    """Build the dead-letter-queue ``Table`` and attach it to *metadata*.

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
