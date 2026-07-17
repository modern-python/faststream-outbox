"""Normalize runs to per-message metrics, render them, and gate against a baseline.

Metric tiers are set by Task 3's *measured* determinism, not by assumption. The total
statement count is NOT structural: the subscriber's fetch loop polls on a wall-clock
timer, so ``calls`` was 519 idle but 894 under CPU load. What IS load-independent and
was verified exact both idle and under load:

* ``tup_upd`` / ``tup_del`` / ``tup_ins`` -- tuple counters (rows actually changed).
  These are the PRIMARY structural invariant.
* ``delete_calls`` -- the terminal DELETE execution count (corroborating). The fetch
  CTE compiles to ``WITH ready AS (...) UPDATE`` and classifies under the ``with`` key,
  so ``calls_by_op['update']`` is ~0 on the happy path -- gate ``delete``, never that.
* ``insert_calls`` / ``select_calls`` -- the producer's INSERT and pg_notify SELECT.

``wal_records`` is near-structural but carries ~5% server-wide spread, so it is gated
with a 10% upper-bound band, never exact. The total ``calls``, ``wal_bytes`` (full-page
images), ``wal_fpi`` and wall-clock / ``msgs_per_second`` are reported for humans but
NEVER gated -- they are timing- or FPI-noisy and would flake the build.
"""

import typing

from benchmarks.workload import RunResult


# Raw structural totals, gated on EXACT equality. Load-independent across idle/loaded runs.
# For a consumer run insert/select are 0; for a producer run delete is 0. Exact-0 ==
# exact-0 passes, so the same key set gates both scenarios.
#
# ``delete_calls`` stays EXACT even for the batched (tfbs>1) point. On the per-row path it
# is ``messages`` (one DELETE per row); on the batched path it is the flush count, and with
# fetch_batch_size==terminal_flush_batch_size the fetch delivers full buffers, so it is
# ``messages / tfbs`` exactly (5000/100 == 50). Empty fetch polls under load inflate total
# ``calls`` but never ``delete_calls`` (a flush fires only on buffered rows), so this count
# is structural, not timing-dependent -- Task 4's Step 5 measured it at 50 across 5 runs
# with zero variance. THREE conditions make it exact here: fetch_batch_size == tfbs (full
# buffers, no partial-buffer flush); messages % tfbs == 0 (no partial tail flush); AND the
# bench handler does not suspend mid-drain -- the no-op run_consumer handler never yields, so
# the worker drains a full generation to the size trigger instead of interleaving with the
# fetch loop. If a future config broke any -- fetch not divisible by tfbs (split buffers),
# messages not a multiple of tfbs (a partial final flush, e.g. messages=5050/tfbs=100 -> 51
# delete_calls), or a bench handler that does real I/O (queue empties mid-buffer -> partial
# idle flush) -- revisit this to a loose upper bound; today all three hold and it is deterministic.
EXACT_KEYS: tuple[str, ...] = ("delete_calls", "tup_upd", "tup_del", "tup_ins", "insert_calls", "select_calls")

# Near-deterministic: ~5% observed spread, so a 10% upper-bound band leaves headroom.
TOLERANT_KEYS: dict[str, float] = {"wal_records": 0.10}


def normalize(result: RunResult) -> dict[str, float]:
    """Per-message metrics for humans, plus the raw totals the gate compares.

    The gate reads the raw structural totals (``delete_calls``, the tuple counters,
    ``wal_records``); the ``*_per_msg`` ratios and ``msgs_per_second`` exist only for the
    human table. Task 5 pins the message count to the baseline's, so the raw totals are
    directly comparable and free of float-rounding ambiguity in the gate.
    """
    n = result.config.messages
    p = result.probe
    delete_calls = p.calls_by_op.get("delete", 0)
    insert_calls = p.calls_by_op.get("insert", 0)
    select_calls = p.calls_by_op.get("select", 0)
    return {
        "messages": n,
        "wall_seconds": result.wall_seconds,
        "msgs_per_second": n / result.wall_seconds if result.wall_seconds else 0.0,
        # Raw structural totals -- the gate compares exactly these.
        "delete_calls": delete_calls,
        "insert_calls": insert_calls,
        "select_calls": select_calls,
        "tup_ins": p.tup_ins,
        "tup_upd": p.tup_upd,
        "tup_del": p.tup_del,
        "wal_records": p.wal_records,
        # Per-message ratios -- informational only.
        "round_trips_per_msg": p.calls / n,
        "delete_calls_per_msg": delete_calls / n,
        "wal_records_per_msg": p.wal_records / n,
        "wal_bytes_per_msg": p.wal_bytes / n,
        # Reported context, never gated: timing/FPI noise and post-mortem bloat signals.
        "calls": p.calls,
        "wal_bytes": p.wal_bytes,
        "wal_fpi": p.wal_fpi,
        "tup_hot_upd": p.tup_hot_upd,
        "dead_tup": p.dead_tup,
        "heap_bytes": p.heap_bytes,
        "index_bytes": p.index_bytes,
    }


def _key(result: RunResult) -> str:
    # The tfbs segment is appended ONLY when batching is enabled (>1) so pre-batching
    # baseline keys (all tfbs=1) stay byte-identical and are not orphaned; a batched
    # point (e.g. consumer/w1/b100/tfbs100) never collides with its tfbs=1 sibling.
    cfg = result.config
    base = f"{result.scenario}/w{cfg.max_workers}/b{cfg.fetch_batch_size}"
    if cfg.terminal_flush_batch_size != 1:
        return f"{base}/tfbs{cfg.terminal_flush_batch_size}"
    return base


def to_baseline(results: list[RunResult]) -> dict[str, typing.Any]:
    """Build the JSON payload: each run keyed by scenario + workers + batch size."""
    return {"runs": {_key(r): normalize(r) for r in results}}


def format_table(results: list[RunResult]) -> str:
    """Render the runs as a human table with a gated-vs-informational footer."""
    # Columns/precision/footer facts are mirrored in format_markdown -- keep in sync.
    header = (
        f"{'scenario':<22} {'msg/s':>9} {'delete/msg':>11} {'WALrec/msg':>11} "
        f"{'WALB/msg':>9} {'fpi':>5} {'upd':>7} {'del':>7} {'dead_tup':>9}"
    )
    lines = [header, "-" * len(header)]
    for r in results:
        m = normalize(r)
        lines.append(
            f"{_key(r):<22} {m['msgs_per_second']:>9.0f} {m['delete_calls_per_msg']:>11.3f} "
            f"{m['wal_records_per_msg']:>11.2f} {m['wal_bytes_per_msg']:>9.0f} "
            f"{m['wal_fpi']:>5.0f} {m['tup_upd']:>7.0f} {m['tup_del']:>7.0f} {m['dead_tup']:>9.0f}",
        )
    lines.append("")
    lines.append("GATED (fail the build): delete_calls + tuple counters (upd/del/ins) + the")
    lines.append("producer's insert_calls/select_calls, all exact; wal_records within a 10%")
    lines.append("band. Columns upd/del are those gated raw counters.")
    lines.append("INFORMATIONAL (never gated): total calls, WAL bytes, msg/s -- FPI/timing noise.")
    lines.append("delete/msg, WALrec/msg and WALB/msg are per-message views, not the gated totals.")
    return "\n".join(lines)


def format_markdown(results: list[RunResult], failures: list[str]) -> str:
    """Render the sweep as a GitHub-flavored Markdown comment body.

    Heading + verdict + table + a condensed gated-vs-informational footnote.
    ``failures`` is ``compare()``'s output: empty means the gate passed. This
    function only formats -- the exit code is the caller's, so the comment can be
    posted on both pass and fail.
    """
    # Columns/precision/footer facts are mirrored in format_table -- keep in sync.
    verdict = "✅ gate passed" if not failures else "❌ gate FAILED"
    lines = ["## Benchmark gate", "", verdict, ""]
    if failures:
        lines.extend(f"- {failure}" for failure in failures)
        lines.append("")
    lines.append("| scenario | msg/s | delete/msg | WALrec/msg | WALB/msg | fpi | upd | del | dead_tup |")
    lines.append("| --- | --: | --: | --: | --: | --: | --: | --: | --: |")
    for r in results:
        m = normalize(r)
        lines.append(
            f"| {_key(r)} | {m['msgs_per_second']:.0f} | {m['delete_calls_per_msg']:.3f} "
            f"| {m['wal_records_per_msg']:.2f} | {m['wal_bytes_per_msg']:.0f} | {m['wal_fpi']:.0f} "
            f"| {m['tup_upd']:.0f} | {m['tup_del']:.0f} | {m['dead_tup']:.0f} |",
        )
    lines.append("")
    lines.append(
        "_Gated (fails the build): `delete_calls` + tuple counters (upd/del/ins) + the "
        "producer's `insert_calls`/`select_calls`, exact; `wal_records` within a 10% band. "
        "msg/s, WAL bytes and total calls are informational (timing/FPI noise)._",
    )
    return "\n".join(lines)


def compare(current: dict[str, typing.Any], baseline: dict[str, typing.Any]) -> list[str]:
    """Diff current raw totals against the baseline. Empty list means pass.

    EXACT_KEYS must match exactly; TOLERANT_KEYS may only exceed an upper bound (a
    regression is MORE work, so a lower value never fails). A baseline run key absent
    from ``current`` is itself a failure.
    """
    failures: list[str] = []
    for key, want in baseline["runs"].items():
        got = current["runs"].get(key)
        if got is None:
            failures.append(f"{key}: missing from the current run")
            continue
        failures.extend(
            f"{key}: {metric} changed (exact-gated): baseline {want[metric]:.0f} -> current {got[metric]:.0f}"
            for metric in EXACT_KEYS
            if got[metric] != want[metric]
        )
        for metric, tolerance in TOLERANT_KEYS.items():
            limit = want[metric] * (1 + tolerance)
            if got[metric] > limit:
                failures.append(
                    f"{key}: {metric} exceeded the {tolerance:.0%} band: "
                    f"baseline {want[metric]:.0f} -> current {got[metric]:.0f} (limit {limit:.0f})",
                )
    return failures
