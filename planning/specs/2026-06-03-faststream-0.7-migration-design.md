# Design: FastStream 0.7 migration

**Date:** 2026-06-03
**Status:** Approved
**Slug:** `faststream-0.7-migration`

## Summary

Migrate `faststream-outbox` from `faststream>=0.6,<0.7` to `faststream>=0.7,<0.8`.
Single code path on 0.7 — no dual-version compat layer. Pure compatibility
migration: fix the mechanically-forced break points and drop the public
per-call `middlewares=` kwarg that upstream removed. No new 0.7 features
(broker-level `AckPolicy` default, multi-broker, MQTT, Redis cluster) are
adopted in this change; each gets its own follow-up spec if/when we want it.

## Motivation

FastStream 0.7.0 ships breaking changes to the internal surface this package
subclasses against:

1. `ProducerProto` gained a `codec: CodecProto` attribute.
2. `SubscriberUsecase.add_call` dropped the `middlewares_` kwarg.
3. `TestBroker.create_publisher_fake_subscriber` became an instance method
   (was `@staticmethod` upstream and in our subclass).
4. Publisher- and subscriber-level middlewares were removed from the public
   surface entirely (use broker-level `BaseMiddleware` instead).

Until this migration lands, `faststream-outbox` is pinned to `<0.7` (commit
`05e8480`). Downstream users on FastStream 0.7 cannot install the outbox.
Staying pinned indefinitely diverges from upstream and accumulates further
incompatibility debt as the 0.7.x series evolves.

The package is at version `"0"` (sentinel / pre-release). The user-facing
API has no stability promise, so a hard break on the `middlewares=` kwarg
is in policy provided it is documented in the commit/PR.

## Scope decisions

**Drop 0.6 support entirely.** Single code path, no `_compat.py` shim.
Users still on 0.6 stay on the currently-released wheel. (Decided during
brainstorming — keeps maintenance surface flat.)

**Pure compat migration.** No adoption of new 0.7 features in this change.
The five forced fix sites + the public-`middlewares=`-kwarg removal are the
entire scope. Broker-level `AckPolicy` default and multi-broker each get
their own spec if/when we want them.

**Drop the public `middlewares=` kwarg** on `OutboxRegistrator.subscriber`,
`OutboxRegistrator.publisher`, `OutboxRouter`/`OutboxRoute`, and the
FastAPI router. The two alternatives both lose:

- *Keep the kwarg, route to broker scope internally* — semantically wrong;
  broker-scope middleware runs on every queue, not the one the kwarg was
  attached to. Silent change of meaning is worse than a hard break.
- *Re-implement per-subscriber middleware in outbox* — reproduces behavior
  upstream just removed; ongoing maintenance cost for a v0 package.

**Single bundled PR.** All changes ship in one commit on
`chore/faststream-0.7-migration`. Splitting would create an incoherent
intermediate state — `add_call` doesn't accept `middlewares_=` anymore, so
the public `middlewares=` kwarg becomes a silent no-op if removed in a
separate PR.

## Design

### Per-file change list

#### `pyproject.toml`
- Bump `faststream>=0.6,<0.7` → `faststream>=0.7,<0.8`.
- No dev-group change (no version-pinned faststream there).

#### `faststream_outbox/publisher/producer.py` — `OutboxProducer`
- Add `codec: CodecProto` attribute, initialized in `__init__`. Imported
  from the upstream module that exposes it in 0.7 (path to verify during
  implementation — see Unknowns below).
- Value: use upstream's default codec instance (whatever ships in 0.7
  alongside the protocol — likely a JSON codec, to confirm during
  implementation). The producer owns its encoding pipeline via
  `_encode_payload` and ignores `self.codec` at runtime; the attribute
  exists solely to satisfy the protocol so ty's structural match against
  `ProducerProto` succeeds.
- If `Optional[CodecProto]` turns out to satisfy the protocol after all,
  default to `None` — simpler and avoids an import of a runtime symbol
  we never call. Implementation plan picks whichever passes ty.

#### `faststream_outbox/testing.py` — `FakeOutboxProducer`
- Add the same `codec` attribute (mirrors `OutboxProducer`).
- `create_publisher_fake_subscriber`: drop `@staticmethod`, add `self`.
  Keep `# pragma: no cover` and the `NotImplementedError` body. The
  `CLAUDE.md` architecture text already documents why it raises — that
  text stays valid (the abstract requirement still exists, we still
  bypass it).

#### `faststream_outbox/registrator.py` — `OutboxRegistrator`
- `subscriber()`: drop the
  `middlewares: Sequence[SubscriberMiddleware[OutboxInnerMessage]] = ()`
  parameter from the signature, and drop `middlewares_=middlewares` from
  the `add_call(...)` call at the current `registrator.py:99`.
- `publisher()`: drop the
  `middlewares: Sequence[PublisherMiddleware[OutboxPublishCommand]] = ()`
  parameter and the corresponding `middlewares=middlewares` pass-through
  to `create_publisher(...)`.
- Trim the `publisher()` docstring: the "*middlewares* run around every
  `publisher.publish(...)` call" sentence is gone.

#### `faststream_outbox/publisher/factory.py`, `publisher/config.py`, `publisher/usecase.py`
- Drop `middlewares=` from `create_publisher(...)` and from any
  `OutboxPublisherConfig` / `OutboxPublisher.__init__` field that stores
  it.
- Remove any internal application of those middlewares around
  `publisher.publish(...)`.

#### `faststream_outbox/router.py` — `OutboxRoute`
- Drop `middlewares=` from the `SubscriberRoute`-shaped constructor.
- Drop the corresponding pass-through to `broker.subscriber(...)`.

#### `faststream_outbox/fastapi/router.py` — `OutboxRouter`
- Drop `middlewares=` from the `subscriber()` and `publisher()` overrides.
- Reassess the `# noqa: PLR0913` on the wide constructors — keep the noqa
  iff the arg count stays at/above the `max-args = 15` ceiling.

#### `faststream_outbox/subscriber/factory.py`
- Verify `AckPolicy.ACK_FIRST` still exists in 0.7. The 0.7 changelog
  removed the *option name* `ack_first` (which we never exposed), but the
  `AckPolicy` enum member is the more likely survivor.
  - If the enum member exists: keep the footgun rejection
    (`ack_policy=AckPolicy.ACK_FIRST` is still a footgun for the outbox
    contract regardless of upstream defaults).
  - If the enum member is gone: delete the rejection branch and its test.
- Re-audit the other footgun checks (`lease_ttl_seconds <=
  max_fetch_interval`, etc.) for any incidental 0.7 reliance — none
  expected, but cheap to confirm.

#### Tests (`tests/test_unit.py`, `tests/test_fake.py`, `tests/test_integration.py`)
- Any test that passes `middlewares=[...]` to `broker.subscriber(...)`,
  `broker.publisher(...)`, `OutboxRouter`, or the FastAPI router:
  - If the test was *only* covering the per-call middleware shape, delete.
  - If the test was covering some other behavior that incidentally passed
    middlewares, rewrite using broker-scope `BaseMiddleware`
    (`OutboxBroker(middlewares=[...])`).
- `--cov-fail-under=100` is the hard gate: every removed code branch must
  have its coverage line gone, and every new branch (the `codec` attribute
  init, if it has any conditional) must be exercised.

#### Docs
- `README.md` and any `docs/usage/*.md` page that demonstrates per-call
  `middlewares=`: rewrite the example to use broker-scope `BaseMiddleware`,
  or drop the example.
- `CLAUDE.md` architecture section: remove the two passages that describe
  per-call middleware behavior — the `OutboxPublisher` paragraph
  ("*middlewares=* wrap every `publisher.publish(...)` call") and the
  registrator's publisher docstring quote.

### Concrete 0.7 surface (verified via upstream `main` branch)

For reference during implementation:

```python
# faststream/_internal/producer.py — ProducerProto attributes
_parser: AsyncCallable
_decoder: AsyncCallable
codec: CodecProto  # NEW in 0.7

# faststream/_internal/endpoint/subscriber/usecase.py — add_call signature
def add_call(
    self,
    *,
    parser_: Optional[CustomCallable],
    decoder_: Optional[CustomCallable],
    dependencies_: Iterable[Dependant],
    codec_: Optional[CodecProto] = None,  # NEW; outbox passes nothing
) -> Self: ...

# faststream/_internal/testing/broker.py — create_publisher_fake_subscriber
@abstractmethod
def create_publisher_fake_subscriber(
    self,
    broker: Broker,
    publisher: Any,
) -> tuple[SubscriberUsecase[Any], bool]: ...
```

## Verification

After all edits, the migration is done iff:

- `uv lock --upgrade` resolves `faststream` to a `0.7.x` release.
- `uv sync --all-extras --all-groups --frozen` succeeds.
- `just lint-ci` clean (ruff + ty).
- `just test` green at `--cov-fail-under=100` (Postgres-backed integration
  suite included).
- `uv run pytest tests/test_unit.py tests/test_fake.py` green standalone
  (no-Postgres tier).
- `git grep -n "middlewares_=\|middlewares=" faststream_outbox/ tests/ docs/`
  returns only `broker_middlewares=` / `BrokerMiddleware` entries — no
  per-call references.
- `git grep -n "faststream<0.7\|faststream>=0.6" .` returns nothing.
- `python -c "from faststream_outbox import OutboxBroker; from
  faststream_outbox.fastapi import OutboxRouter"` exits 0.

## Risk register

**R1 — Coverage gate after kwarg removal.** Dropping `middlewares=` removes
branches the existing tests exercise. `--cov-fail-under=100` will catch any
orphaned `if middlewares:` left behind. Mitigation: normal TDD-on-test
cleanup as part of the plan.

**R2 — `CodecProto` location/shape may differ.** Upstream confirms
`ProducerProto` requires `codec: CodecProto`, but the exact import path
(`faststream._internal.codec`?) and whether `None` is acceptable need
verification against the installed 0.7 wheel. Mitigation: implementation
plan step inspects the installed package; if `codec` is non-Optional, use
the upstream-provided default codec instance.

**R3 — `AckPolicy.ACK_FIRST` enum may be gone.** Mitigation: verify during
implementation; if removed, delete `subscriber/factory.py`'s rejection
branch and its test together.

**R4 — `_internal.*` imports may have moved.** 30+ symbols imported from
`faststream._internal.*` are not documented as stable; the 0.7 changelog
only enumerates public-surface changes. Mitigation: rely on lint + test
pipeline to surface broken imports; each is a single-line fix.

**R5 — `OutboxRoute` API break is public.** Dropping `middlewares=` from
the router is a hard break for any downstream user passing it. Mitigation:
document in commit message + PR body; v0 package, no stability promise.

**R6 — FastAPI `subscriber()`/`publisher()` override signature.**
`StreamRouter` in 0.7 may have shifted its kwargs ladder; our overrides
may need re-alignment with the new base signatures (especially around
`Default(...)` defaults). Mitigation: when ty complains, re-pin against
the new signatures — mechanical.

**R7 — Test-broker patch surfaces.** `TestOutboxBroker._patch_broker`
mocks `broker.publish` / `publish_batch` / `cancel_timer` /
`fetch_unprocessed`. If `BrokerUsecase` in 0.7 changed how these are
dispatched, patches may stop intercepting cleanly. Mitigation: surface
via `tests/test_fake.py`; if a patch is ineffective, replace with the new
dispatch hook.

## Unknowns the implementation plan will resolve

1. Exact `CodecProto` import path and whether `None` is allowed for the
   `codec` attribute.
2. Whether `AckPolicy.ACK_FIRST` enum member survived in 0.7.
3. Whether any `_internal.*` symbol the package currently imports was
   moved or renamed.
4. Whether `StreamRouter.subscriber()` / `publisher()` kwargs ladders
   shifted in 0.7.

Each is a small inspection step; none materially shifts the design.

## Out of scope (deferred to follow-up specs)

- Adopting broker-level `AckPolicy` default (per-broker default that
  subscribers inherit unless overridden).
- Adopting multi-broker capability (run `OutboxBroker` alongside another).
- Removing the publisher-relay `NotImplementedError` — still load-bearing
  post-0.7 (the dispatch loop still has no reachable `AsyncSession`).
- Reworking `OutboxResponse` session threading.
- `mkdocs` site rewrite beyond mechanical fixes to snippets that show
  `middlewares=`.
- Any change to `CHANGELOG.md` — none exists in the repo; the package is
  at version `"0"`.

## Order of operations (single commit)

1. Branch: `git switch -c chore/faststream-0.7-migration`.
2. `pyproject.toml`: bump pin.
3. `uv lock --upgrade && uv sync --all-extras --all-groups --frozen`.
4. Fix import/attribute errors as `ty` and `ruff` surface them
   (`OutboxProducer.codec`, `FakeOutboxProducer.codec`, `add_call`
   kwargs, `create_publisher_fake_subscriber` signature).
5. Drop public `middlewares=` kwarg from the four entry points
   (registrator subscriber/publisher, `OutboxRouter`/`OutboxRoute`,
   FastAPI router subscriber/publisher).
6. Drop internal storage/application of those middlewares in
   `publisher/factory.py` / `config.py` / `usecase.py`.
7. Update tests: delete or rewrite anything passing per-call
   `middlewares=`.
8. Update `README.md`, `docs/usage/*.md`, `CLAUDE.md` per the doc plan
   above.
9. `just lint && just test` until green at 100% coverage.
10. Single commit:
    `chore: migrate to faststream 0.7 (breaking: drop per-call middlewares=)`
    with a body that enumerates the break points and the dropped kwarg.
11. Open PR; body re-states the breaking change for downstream consumers.

## Acceptance criteria

- All Verification commands above pass.
- Commit message documents the breaking change.
- PR description lists each break point with file pointers so reviewers
  can spot-check.
- No grep hit for `faststream<0.7`, `faststream>=0.6`, or per-call
  `middlewares=` outside `broker_middlewares=` / `BrokerMiddleware`.
