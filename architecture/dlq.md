# Opt-in DLQ on terminal failure — implementation detail

User-facing: `docs/usage/dlq.md`. Invariant summary: `CLAUDE.md` § Opt-in DLQ.

## Default and opt-in

`make_dlq_table(metadata, table_name="outbox_dlq")` returns a sibling audit table; pass it to `OutboxBroker(..., dlq_table=...)` to archive terminal failures. **Default broker behavior is unchanged when `dlq_table` is None — every existing code path is bit-for-bit identical.**

## Atomicity via single CTE

`OutboxClient.delete_with_lease` switches to `WITH deleted AS (DELETE … RETURNING …) INSERT INTO <dlq> SELECT … FROM deleted` when configured. One statement preserves the writer-connection autocommit fast path and the lease-token guard; INSERT failure rolls back the DELETE, so the outbox row stays leased and is reclaimed when the lease expires — **DLQ misconfiguration surfaces as outbox-table growth + `lease_lost` spikes rather than silent audit loss.** Identifiers are quoted via the dialect's `identifier_preparer`; values flow through bind params.

## `terminal_failure_reason` routing

`OutboxInnerMessage.terminal_failure_reason` is set on the three failure paths:

- `allow_delivery` False → `"max_deliveries"`
- `_nack` strategy-exhausted → `"retry_terminal"`
- `_reject` → `"rejected"`

`_flush_terminal` reads it to decide whether to build a DLQ payload; `dispatch_one` also reads it to pick the `nacked_terminal(reason=…)` tag value. **Branch on `terminal_failure_reason` BEFORE `last_exception`**, so manual `await msg.reject()` (no exception raised) routes correctly to `nacked_terminal(reason="rejected")` instead of the previously-incorrect `acked`. Success (`_ack`) leaves the field None; success rows never touch the DLQ.

**The `DLQFailureReason` `Literal` type (`message.py`) is the public contract** for this string — operator queries and dashboard labels key off these values, so changing them is API-breaking.

## `last_exception` bounds

`last_exception` is rendered by `_render_last_exception` (`subscriber/usecase.py`) and bounded by `_LAST_EXCEPTION_MAX_CHARS=8192`. The default render is `repr(exc)`; some exceptions carry MB-scale payloads (validation errors with the full request body, `asyncpg.DataError` with the rejected row), so an unbounded `repr` would extend the writer round-trip on a poison row and bloat the DLQ — truncation appends `…[truncated]`. Because that `repr` can embed payloads / PII / credentials, `OutboxBroker(..., last_exception_renderer=...)` (a `Callable[[BaseException], str | None]`, read from `OutboxBrokerConfig.last_exception_renderer`) lets a deployment redact (`type(exc).__name__`) or drop it (`None`); a custom renderer's output is still length-capped. The DLQ `failure_reason` column is `String(64)` (current literals fit in 14 bytes; the breathing room lets the canonical set grow without a column-widening migration).

## Retention

There is no built-in retention/pruning. Operators are responsible for archival — suggested pattern: partition the DLQ by `failed_at` and drop old partitions via a cron job.

## `validate_schema()` mechanics

`validate_schema()` delegates to `alembic.autogenerate.compare_metadata` against a throwaway `MetaData` populated by `make_outbox_table(...)` — so the canonical `Table` is the single source of truth and the validator never duplicates the schema declaration. It only flags **missing** schema (`add_*` / `modify_*` ops); `remove_*` ops are intentionally ignored so users may attach extras (audit columns, their own indexes). Alembic is an **optional dependency** (`faststream-outbox[validate]`); without it, `validate_schema()` raises `ImportError`, but every other code path works (the import lives at the top of `client.py` inside a try/except, with module-level sentinels `_alembic_compare_metadata` / `_AlembicMigrationContext` set to `None` on failure).

Alembic's diff is **blind to three things the producer's `ON CONFLICT` arbiter and the lease invariant depend on**, so `validate_schema()` runs extra `pg_catalog` probes alongside it: the partial-index **WHERE predicates** (alembic ignores `postgresql_where`), the **uniqueness** of `timer_id_uq` (`pg_index.indisunique` — a same-named non-unique index passes the predicate check yet breaks `ON CONFLICT` at publish time), and the **`<table>_lease_ck` CHECK** definition (alembic has no check-constraint comparator). Each surfaces a drifted/non-partial/non-unique index or a missing/altered CHECK that the diff alone would miss.
