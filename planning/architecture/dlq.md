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

`last_exception` is serialized via `repr()` and bounded by `_LAST_EXCEPTION_MAX_CHARS=8192` in `subscriber/usecase.py`. Some exceptions carry MB-scale payloads (validation errors with the full request body, `asyncpg.DataError` with the rejected row); an unbounded `repr` would extend the writer round-trip on a poison row and bloat the DLQ. Truncation appends `…[truncated]`. The DLQ `failure_reason` column is `String(64)` (current literals fit in 14 bytes; the breathing room lets the canonical set grow without a column-widening migration).

## Retention

There is no built-in retention/pruning. Operators are responsible for archival — suggested pattern: partition the DLQ by `failed_at` and drop old partitions via a cron job.

## `validate_schema()` mechanics

`validate_schema()` delegates to `alembic.autogenerate.compare_metadata` against a throwaway `MetaData` populated by `make_outbox_table(...)` — so the canonical `Table` is the single source of truth and the validator never duplicates the schema declaration. It only flags **missing** schema (`add_*` / `modify_*` ops); `remove_*` ops are intentionally ignored so users may attach extras (audit columns, their own indexes). Alembic is an **optional dependency** (`faststream-outbox[validate]`); without it, `validate_schema()` raises `ImportError`, but every other code path works (the import lives at the top of `client.py` inside a try/except, with module-level sentinels `_alembic_compare_metadata` / `_AlembicMigrationContext` set to `None` on failure).
