---
status: shipped
date: 2026-06-23
slug: self-validating-subscriber-config
summary: Move subscriber-knob validation from the factory into OutboxSubscriberConfig.__post_init__ so every construction path is validated, not just the factory's.
supersedes: null
superseded_by: null
pr: 111
outcome: |
  Landed. Validation moved to OutboxSubscriberConfig.__post_init__ (guarded super-call +
  self._validate()); factory.py dropped _validate_subscriber_config and now just wires.
  Behavior preserved exactly (EMPTY→None ack_policy mapping). One wrinkle surfaced under
  CI: moving validation under the dataclass-generated __init__ added a "<string>" frame
  that the 3.13 C warnings.warn(skip_file_prefixes=...) refuses to skip (works on 3.14),
  so the FastAPI-router attribution test failed in docker. Replaced skip_file_prefixes
  with a manual stacklevel walk (_subscriber_warn) that's version- and call-path-robust.
  Existing validation tests passed as the regression guard; full suite 543 passed at 100%.
---

# Change: Make the subscriber config validate itself

**Lane:** lightweight — 2 files, net-neutral LOC (a relocation), no new file, no
public-API change, existing tests cover it.

## Goal

Subscriber-knob validation lived in `subscriber/factory.py::_validate_subscriber_config`,
called from `create_subscriber`. `OutboxSubscriberConfig` was a bare `@dataclass` with no
`__post_init__`, so constructing it directly bypassed every guard. Move validation into
`OutboxSubscriberConfig.__post_init__` so *every* construction path — `@broker.subscriber`,
`@router.subscriber`, direct construction — is validated; there is no way in that skips it.

This is candidate #3 from the 2026-06-23 architecture review.

## Approach

- `OutboxSubscriberConfig.__post_init__` does a guarded `super().__post_init__()` (upstream
  `SubscriberUsecaseConfig` has none today, but dataclasses call only the most-derived one —
  the guard keeps a future upstream init running) then `self._validate()`.
- `_validate()` holds the relocated checks. Behavior is **preserved exactly**: it maps
  `_ack_policy is EMPTY → None` so the checks match on the *explicitly-passed* policy as the
  factory-side validation did (e.g. the NACK+NoRetry advisory still fires only on an explicit
  `NACK_ON_ERROR`, not on the default that resolves to it via the `ack_policy` property).
- `create_subscriber` drops the validation call and the function; it just wires now.

**Warning attribution (version-robustness).** Moving validation under the dataclass-generated
`__init__` inserts a frame whose `co_filename` is the literal `"<string>"` between the user's
call and `__post_init__`. The 3.13 C `warnings.warn(skip_file_prefixes=...)` does **not** skip
that frame (3.14 does), so warnings mis-attributed to `<string>` and the FastAPI-router
attribution test failed under docker/3.13. Replaced `skip_file_prefixes` with `_subscriber_warn`,
which computes `stacklevel` by walking out to the first non-internal frame (this package /
faststream / `<string>`) — robust across CPython versions and across the direct vs router paths
(which differ in frame count).

**Why minimal — broader "config validates everything" not pursued.** Only subscriber-knob
validation moved. Other config objects (`OutboxBrokerConfig`, publisher config) were not
swept in; this change is scoped to the one shallow factory the review flagged.

## Files

- `faststream_outbox/subscriber/config.py` — add `__post_init__` + `_validate` +
  `_subscriber_warn` (manual-stacklevel attribution).
- `faststream_outbox/subscriber/factory.py` — remove `_validate_subscriber_config` and its
  call; keep construction/wiring.

## Verification

- [x] Existing tests are the regression guard (unchanged): `test_unit.py` subscriber
  reject/warn/no-warning cases (via `@broker.subscriber`), `test_fastapi.py`
  `test_subscriber_misconfig_warning_attributed_to_user_via_fastapi_router` (router path
  attribution), `test_fake.py::test_subscriber_config_ack_policy_returns_explicit_value`
  (direct construction).
- [x] `just test` — 543 passed, 100% coverage (docker / Python 3.13).
- [x] `just lint-ci` — clean.
