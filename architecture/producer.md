# Producer / publish path — implementation detail

User-facing: `docs/usage/` (publishing). Invariant summary: `CLAUDE.md` § Producer.

## The transactional contract

`broker.publish(body, *, queue, session, headers=None, correlation_id=None, activate_in=None, activate_at=None, timer_id=None)` and `broker.publish_batch(*bodies, queue, session, headers=None, activate_in=None, activate_at=None)` insert outbox rows through the caller's `AsyncSession`. They do not flush, commit, or open their own transaction — the row must commit with the caller's domain writes.

Both reject a non-`AsyncSession` with `TypeError`. `publish` returns the row id (or `None` on a `timer_id` conflict); `publish_batch` returns nothing and rejects `timer_id` (per-row dedup is meaningless in a batch). `broker.request` raises `NotImplementedError` (the outbox is fire-and-forget).

## OutboxProducer + the single insert path

`OutboxProducer` (`publisher/producer.py`) implements `ProducerProto[OutboxPublishCommand]` and is the canonical insert path. `broker.publish`, `publish_batch`, and `OutboxPublisher.publish` all build an `OutboxPublishCommand` (`response.py`) and route through `_basic_publish(cmd, producer=self.config.producer)` — encode + insert + NOTIFY semantics live in one place.

Session-type / queue / activate-args-mutex / tz validation lives in one shared `_validate_publish_args` (`response.py`), called by the `OutboxPublishCommand` constructor, `OutboxResponse.__init__`, and `broker.publish_batch`'s empty-batch branch — so every real publish entry point (including an empty batch) rejects the same misconfigurations identically and eagerly. The checks run in a fixed order: activate-args → session → queue.

`from_cmd` raises (relay chaining is unsupported here).

## NOTIFY dedup per transaction

`_notify` emits at most one `pg_notify` per `(transaction, queue)`, deduped via a `WeakKeyDictionary` memo keyed on the innermost active *sync* transaction (`session.sync_session.get_nested_transaction() or session.sync_session.get_transaction()`). This is behavior-preserving and default-on — no config knob: Postgres already coalesces identical NOTIFYs per transaction at delivery, so the dedup only removes redundant round-trips, and the subscriber already tolerates coalesced/duplicate wakes. The memo entry for a transaction GCs when that transaction object is collected, so a rolled-back savepoint's entry can never suppress a later real NOTIFY for the same queue once control returns to the outer transaction. The guarantee holds for the caller's transaction regardless of whether they use an explicit `session.begin()` or rely on autobegin: keying on the sync `SessionTransaction` (which the `Session` holds strongly for the transaction's lifetime) rather than the async `AsyncSessionTransaction` proxy (regenerated per call, only weak-referenced) means the memo entry survives between autobegin publishes instead of GC'ing and silently losing the dedup.

## Publisher wrapper

`broker.publisher(queue, *, headers=None, title=None, description=None, schema=None, include_in_schema=True)` returns an `OutboxPublisher` — a typed wrapper around `broker.publish` with the same transactional contract. Static decorator headers merge with per-call headers (per-call wins).

The publisher exists for AsyncAPI / per-queue config — not decorator-relay chaining: `OutboxPublisher.__call__` raises `NotImplementedError` at decoration time. A relay decorator can't reach an `AsyncSession` without breaking the transactional contract.

## Chained publishing via OutboxResponse

For chained publishing, handlers can `return OutboxResponse(body=..., queue=..., session=session)`.

`OutboxResponse.__init__` validates eagerly via the shared `_validate_publish_args` (so a misconfigured response raises at the `return` site, not at dispatch where it would masquerade as a handler failure); `as_publish_command()` re-runs the same validator, keeping `OutboxPublishCommand` the authoritative source.

FastStream gates `_make_response_publisher` on a truthy `message.reply_to`; `OutboxParser.parse_message` sets `reply_to=msg.queue` to trip it. The actual publisher is `OutboxFakePublisher` (`publisher/fake.py`), which gates on `isinstance(cmd, OutboxPublishCommand)` so plain returns (`None`, `dict`, …) become silent no-ops. `correlation_id` propagates via FastStream's `process_message` inheritance.

## Payload encoding

`_encode_payload` (`envelope.py`) is the internal helper that turns `body` into `(payload_bytes, headers_dict)`. It is used by both producers and is not exported.
