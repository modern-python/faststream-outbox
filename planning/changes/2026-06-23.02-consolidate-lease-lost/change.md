---
summary: Consolidate the duplicated lease-lost detect→log→emit block shared by _flush_terminal and _flush_retry into one _emit_lease_lost helper.
---

# Change: Give lease-lost telemetry one home

**Lane:** lightweight — ~15 LOC net, 1 file, no new file, no public-API change,
existing unit tests cover it.

## Goal

`_flush_terminal` and `_flush_retry` (`subscriber/usecase.py`) each carried a
byte-identical ~17-line tail — WARNING log (`extra={event, phase, row_id, queue,
deliveries_count}`) + `_emit_metric("lease_lost", …)` — differing only in `phase`
(`"terminal"`/`"retry"`). A change to lease-lost telemetry meant editing both with
divergence risk. Collapse the duplication into one `_emit_lease_lost(row, *, phase)`.

## Approach

Extract the log+metric into `OutboxSubscriber._emit_lease_lost(row, *, phase)`. Each
flush method keeps its own explicit `if not landed: self._emit_lease_lost(row,
phase=…); return False` branch, so the control flow a reader wants to see (the
rowcount-0 → redeliver decision) stays visible. The terminal/retry blocks were
identical apart from `phase`, so the helper captures both; the log prose now derives
from `phase` (no test couples to the wording).

**Scope deliberately minimal — (B) and (C) rejected.** This is candidate #2 from the
2026-06-23 architecture review ("give the lease a home"). The grand forms were
evaluated and dropped:

- **(B) a `Lease` value object** `(message_id, token)` threaded through the client
  interface — rejected: the pair is passed at only two call sites, and it would ripple
  through `AbstractOutboxClient` + both adapters + the just-landed
  `tests/test_client_contract.py` for modest gain.
- **(C) a full "Lease module"** owning issue/guard/cutoff — rejected for the same
  reason [[client-rules-kernel]] left eligibility/lease/retry-timing as two
  implementations: the real client runs the lease guard *in SQL*
  (`WHERE acquired_token = :token`), so a pure Lease module would have one Python
  consumer (the subscriber) — a hypothetical seam, i.e. indirection.

The only genuinely duplicated, in-process piece was the lease-lost handling; that is
all this change touches. The rest of the lease lifecycle (issue/guard/expiry in SQL,
the `acquired_token` field, the `<table>_lease_ck` CHECK) is already single-sourced or
intrinsically SQL and stays put. **A future architecture review should not re-suggest
(B)/(C) without new evidence that the SQL-guard constraint has changed.**

## Files

- `faststream_outbox/subscriber/usecase.py` — add `_emit_lease_lost`; both flush
  methods call it instead of repeating the log+metric.

## Verification

- [x] Existing tests pin the contract (invoke `_flush_terminal`/`_flush_retry`
  directly): `test_flush_terminal_logs_lease_lost_at_warning_with_structured_fields`,
  `..._retry...`, `test_metrics_lease_lost_terminal/retry_emits_recorder_event`,
  `test_dispatch_one_lease_lost_emits_only_lease_lost_not_acked` — `uv run pytest
  tests/test_unit.py -k lease_lost` green (6 passed).
- [x] `just lint-ci` — clean.
- [x] `just test` — full suite green at 100% coverage (543 passed).
