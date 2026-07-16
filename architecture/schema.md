# User-owned schema — implementation detail

User-facing: `docs/operations/alembic.md`. Invariant summary: `CLAUDE.md` § Schema.

## make_outbox_table + partial indexes

`make_outbox_table(metadata, table_name="outbox")` returns a `sqlalchemy.Table` on
the user's `MetaData`. The package never creates or migrates the table — that's
Alembic's job — but it declares three partial indexes on the table so that
autogenerate brings them up:

- `(queue, next_attempt_at) WHERE acquired_token IS NULL` — fetch CTE Branch A
  (unleased rows).
- `(queue, acquired_at) WHERE acquired_token IS NOT NULL` — fetch CTE Branch B
  (expired-lease reclaim).
- unique `(queue, timer_id) WHERE timer_id IS NOT NULL` — `timer_id` dedup.

## The lease CHECK constraint

In addition to the indexes, `make_outbox_table` declares a
`CHECK ((acquired_token IS NULL) = (acquired_at IS NULL))` — the `<table>_lease_ck`
constraint. It makes a half-set lease unrepresentable: the two lease columns must
either both be set or both be unset.

## Why the fetch CTE carries partial-index predicates

The fetch CTE's `OR` is written so that each disjunct explicitly carries its
partial-index predicate as a conjunct. Postgres only uses a partial index when the
query implies the index's `WHERE` clause; the naive form of the query (without the
predicate spelled out per disjunct) falls back to a seq-scan. Both fetch indexes
pay write amplification on every claim.

## ORDER BY and sort nodes

The fetch index also satisfies the `ORDER BY next_attempt_at, id`, but only for a
single-queue subscriber. A subscriber serving multiple queues
(`queue = ANY(:queues)`), or the expired-lease branch (which is ordered by
`next_attempt_at` while `_lease_idx` is keyed on `acquired_at`), adds a
`LIMIT`-bounded sort node. Prefer one subscriber per queue when fetch ordering cost
matters — the same segregation pattern as lease TTLs.

The `ORDER BY` lives on the inner CTE that selects and `LIMIT`s the rows; the outer
`UPDATE … RETURNING *` is unordered, so the order in which rows dispatch within a
single fetch batch is unspecified (F2-09). The ordering governs which rows are
claimed under contention (FIFO selection), not the per-row dispatch sequence —
which is irrelevant with `max_workers > 1` anyway. Don't rely on within-batch FIFO
delivery.

## No state column

There is no `state` column. A row is "available" iff `acquired_token IS NULL` or
`acquired_at < now() - lease_ttl_seconds`. Terminal failures `DELETE` by default;
opt in to audit via `dlq_table=make_dlq_table(metadata)`.

## validate_schema() — opt-in drift detection

`validate_schema()` is opt-in — call it from `/health` or a startup hook, not from
`broker.start()` — so that migrations can run against the same DB without a loop.

Beyond the alembic column/index diff it also probes the live partial-index
predicates (alembic ignores `postgresql_where`), catching a drifted or non-partial
`timer_id_uq` that would otherwise break `ON CONFLICT` at publish time (S2). It also
probes `pg_constraint` for the `<table>_lease_ck` CHECK (alembic has no
check-constraint comparator), catching a missing or drifted lease pairing.

Because these two probes (predicates + CHECK) catch drift that
`alembic revision --autogenerate` cannot remediate, the raised `RuntimeError`
appends a pointer to
`docs/operations/alembic.md#fixing-drift-autogenerate-cant-see` (the
hand-written-migration recipe) — but only when one of those two probes fired.
Autogenerate-fixable drift (columns, plain indexes, DLQ) gets no pointer. Message
composition lives in `_compose_schema_mismatch_message` (`client.py`), gated on
`has_blind_drift`.

The alembic diff runs with `include_schemas=True` so a table in a non-default
`MetaData(schema=...)` is reflected and compared (without it, `compare_metadata`
only sees the default schema and a named-schema table falsely reads as "table does
not exist"). `_include_name` narrows schema reflection to the target schema so
unrelated schemas never surface as false drift. Because Alembic reports the
connection's default schema to the hook as `None`, `table.schema` is first
normalized against `connection.dialect.default_schema_name` — a table that
explicitly names the default schema (`MetaData(schema="public")`, or a named schema
that is on the connection's `search_path`) becomes `None` so it still matches,
avoiding a false "table does not exist" on a correct table.

Alembic is optional (`faststream-outbox[validate]`); without it `validate_schema()`
raises `ImportError`, but every other path works.

## Autovacuum (recommended by default, enforced via a flag)

The outbox is high-churn (`dead_tup ≈ 2 × messages`: the lease `UPDATE` + terminal
`DELETE` each leave a dead tuple). Aggressive autovacuum
(`autovacuum_vacuum_scale_factor = 0` + a constant threshold, for both the vacuum and
insert-triggered pairs) is **recommended by default**: SQLAlchemy's `Table` cannot
carry reloptions, so autogenerate can't emit them, and the package applies nothing
itself. `outbox_autovacuum_ddl()` (in `autovacuum.py`) renders the migration
statement the user runs. Enforcement is opt-in: `validate_schema(check_autovacuum=True)`
(threaded through `OutboxClient`/`OutboxBroker`) reads `pg_class.reloptions` and
**raises** a distinctly-labeled "Outbox autovacuum not tuned: " error when the
table lacks the settings — separate from the "Outbox schema mismatch: " prefix, so
an operator can tell the two apart. Because it rides `validate_schema()`, the check
is coupled to the `[validate]` (Alembic) extra. `fillfactor` is excluded on evidence
(HOT is impossible — the claim `UPDATE` mutates both partial indexes' key columns).

`scale_factor`/threshold control vacuum *eligibility* — this is the structural fix
above, shipped and enforced by the probe. `outbox_autovacuum_ddl()` also accepts
optional `vacuum_cost_delay`/`vacuum_cost_limit`, which control vacuum *throughput*
instead; they default to unset, are not checked by the probe (situational tuning,
not a structural requirement), and are the binding constraint under heavy sustained
churn — eligibility alone cannot keep vacuum ahead of the dead-tuple rate if it runs
throttled.
