# Retry strategies — implementation detail

User-facing: `docs/usage/` (retries). Invariant summary: `CLAUDE.md` § Retry.

## get_next_attempt_delay

Retry strategies live in `retry.py`. The core method,
`get_next_attempt_delay(*, first_attempt_at, last_attempt_at, attempts_count, exception=None)`,
returns the delay in seconds before the next attempt, or `None` to signal
terminal failure.

The returned value is a delay, not an absolute timestamp: the DB computes
`next_attempt_at` from it server-side, so the timing is immune to clock skew
between the worker and the DB host.

The method receives the raised `exception` so subclasses can retry only on
transient errors.

## Template enforcement

`_RetryStrategyTemplate` enforces the two cross-cutting limits shared by the
concrete strategies: `max_attempts` and `max_total_delay_seconds`. A concrete
strategy that derives from the template gets both of these caps applied on top
of its own per-attempt delay computation.

## ExponentialRetry

`ExponentialRetry` adds two optional knobs on top of the template: jitter and
`max_delay_seconds`.

## max_total_delay_seconds is a lower bound

`max_total_delay_seconds` is a lower bound on the horizon, not an exact ceiling.
`elapsed` is measured as `last_attempt_at − first_attempt_at`, and both
timestamps are set equal on the first attempt. Because of that, the budget
always permits roughly one more interval beyond the nominal cap (F2-01).

Size `max_total_delay_seconds` as "at least this long", not as an exact ceiling.

## Default strategy

A subscriber with no explicit `retry_strategy` resolves to:

```python
ExponentialRetry(
    initial_delay_seconds=1.0,
    multiplier=2.0,
    max_delay_seconds=300.0,
    max_attempts=10,
    jitter_factor=0.2,
)
```

This comes from `_default_retry_strategy()` in `registrator.py`.

"Delete on first error" is the wrong default for an outbox, so it is not the
default; opt in to that behavior explicitly with `NoRetry()`.
