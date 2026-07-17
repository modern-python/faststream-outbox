"""CLI: `python -m benchmarks run` / `python -m benchmarks check`.

Invoked through `just bench` / `just bench-check`, which run it inside docker compose
so the postgres service has pg_stat_statements preloaded.
"""

import argparse
import asyncio
import json
import os
import pathlib
import sys

from benchmarks.config import DEFAULT_DSN, RunConfig
from benchmarks.report import compare, format_markdown, format_table, to_baseline
from benchmarks.workload import RunResult, make_engine, run_consumer, run_producer


BASELINE_PATH = pathlib.Path(__file__).parent / "baseline.json"

# The sweep. Consumer points cross workers x batch size; the producer point measures
# the write path (two statements per publish).
_WORKER_COUNTS = (1, 2, 4)
_BATCH_SIZES = (10, 100)


def _median_by_wall(runs: list[RunResult]) -> RunResult:
    """Return the run with the median wall-clock.

    Wall-clock is the only noisy thing we report (Docker IO), so repeat it and take the
    middle. Returning the whole RunResult -- rather than a median of each metric
    independently -- keeps every number in a row internally consistent: they all come
    from one real run. The gated counters are deterministic anyway, so which run wins
    does not affect the gate.
    """
    return sorted(runs, key=lambda r: r.wall_seconds)[len(runs) // 2]


async def _run_sweep(dsn: str, messages: int, repeats: int) -> list[RunResult]:
    """Build the engine once, then run every sweep point through it.

    NEVER construct an engine inside the loop: SQLAlchemy's one-time dialect init emits
    6 extra statements on an engine's first connection and would inflate the baseline.
    """
    engine = make_engine(dsn)
    results: list[RunResult] = []
    try:
        # Warm-up, discarded: the first run pays connection setup and page faults.
        await run_consumer(engine, RunConfig(messages=min(messages, 200)))
        for workers in _WORKER_COUNTS:
            for batch in _BATCH_SIZES:
                cfg = RunConfig(messages=messages, max_workers=workers, fetch_batch_size=batch)
                runs = [await run_consumer(engine, cfg) for _ in range(repeats)]
                results.append(_median_by_wall(runs))
        # Batched-flush before/after: the tfbs=1 sibling above is consumer/w1/b100; this
        # tfbs=100 counterpart (consumer/w1/b100/tfbs100) coalesces the terminal DELETEs
        # so delete_calls drops ~100x. YAGNI: one batched point, not a full cross-product.
        batched_cfg = RunConfig(messages=messages, max_workers=1, fetch_batch_size=100, terminal_flush_batch_size=100)
        batched_runs = [await run_consumer(engine, batched_cfg) for _ in range(repeats)]
        results.append(_median_by_wall(batched_runs))
        producer_cfg = RunConfig(messages=messages)
        producer_runs = [await run_producer(engine, producer_cfg) for _ in range(repeats)]
        results.append(_median_by_wall(producer_runs))
    finally:
        await engine.dispose()
    return results


def _check(dsn: str, *, markdown: bool = False) -> int:
    """Run the sweep at the baseline's message count and gate the counters."""
    if not BASELINE_PATH.exists():
        sys.stdout.write(f"no baseline at {BASELINE_PATH}; run `just bench --write-baseline` first\n")
        return 1
    baseline = json.loads(BASELINE_PATH.read_text())
    # Pin the message count to the baseline's: raw totals are only comparable at the same
    # N (there is a small fixed statement overhead per run). Counters are deterministic,
    # so repeats=1 -- repeating them only burns CI time.
    messages = int(next(iter(baseline["runs"].values()))["messages"])
    results = asyncio.run(_run_sweep(dsn, messages, repeats=1))
    failures = compare(to_baseline(results), baseline)
    if markdown:
        # Python owns the whole PR-comment body; the workflow just posts stdout.
        sys.stdout.write(format_markdown(results, failures) + "\n")
        return 1 if failures else 0
    sys.stdout.write(format_table(results) + "\n")
    if failures:
        sys.stdout.write("\nBENCHMARK GATE FAILED:\n")
        for failure in failures:
            sys.stdout.write(f"  - {failure}\n")
        return 1
    sys.stdout.write("\nbenchmark gate: OK\n")
    return 0


def _run(dsn: str, messages: int, repeats: int, *, write_baseline: bool) -> int:
    """Run the sweep, print the table, and optionally overwrite the committed baseline."""
    results = asyncio.run(_run_sweep(dsn, messages, repeats=repeats))
    sys.stdout.write(format_table(results) + "\n")
    if write_baseline:
        BASELINE_PATH.write_text(json.dumps(to_baseline(results), indent=2, sort_keys=True) + "\n")
        sys.stdout.write(f"\nwrote {BASELINE_PATH}\n")
    return 0


def _build_parser() -> argparse.ArgumentParser:
    """Two subcommands with their own flags, so a misplaced flag errors at parse.

    A flat parser accepted every flag with either command (`run --markdown` parsed
    and silently no-oped); subparsers scope each flag to the command that reads it.
    """
    parser = argparse.ArgumentParser(prog="benchmarks")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run = subparsers.add_parser("run", help="run the sweep and print the table")
    run.add_argument("--messages", type=int, default=5_000)
    # Wall-clock is IO-noisy, so `run` repeats each point and reports the median.
    run.add_argument("--repeats", type=int, default=3)
    run.add_argument("--write-baseline", action="store_true")

    # `check` uses repeats=1: the gated counters are deterministic, so repeating them
    # only burns CI time -- it takes no --repeats/--messages (pinned to the baseline).
    check = subparsers.add_parser("check", help="gate the counters against baseline.json")
    check.add_argument("--markdown", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)

    dsn = os.environ.get("POSTGRES_DSN", DEFAULT_DSN)

    if args.command == "check":
        return _check(dsn, markdown=args.markdown)
    return _run(dsn, args.messages, args.repeats, write_baseline=args.write_baseline)


if __name__ == "__main__":
    sys.exit(main())
