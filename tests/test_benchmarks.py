"""Unit tests for the benchmark harness' pure logic.

Deliberately no Postgres: the pytest CI job's postgres service cannot preload
pg_stat_statements, so anything catalog-dependent is verified by `just bench-check`.
The gate compares RAW STRUCTURAL TOTALS (delete_calls, the tuple counters, wal_records),
which Task 3 proved load-independent -- NOT the total `calls`, which scales with the
fetch loop's wall-clock polling.
"""

import pytest

from benchmarks.__main__ import _build_parser, _extend_points
from benchmarks.config import ExtendConfig, RunConfig
from benchmarks.extend import ExtendResult, Sample, autovacuum_ddl, fetch_mean_ms, format_extend, percentiles
from benchmarks.probes import ProbeResult
from benchmarks.report import EXACT_KEYS, compare, format_markdown, format_table, normalize, to_baseline
from benchmarks.workload import RunResult


def _probe(
    *,
    delete_calls: int = 500,
    insert_calls: int = 0,
    select_calls: int = 0,
    fetch_calls: int = 12,
    other_calls: int = 10,
    **overrides: float,
) -> ProbeResult:
    calls_by_op = {
        "select": select_calls,
        "insert": insert_calls,
        "update": 0,
        "delete": delete_calls,
        "with": fetch_calls,
        "other": other_calls,
    }
    defaults: dict[str, object] = {
        "calls": 522,
        "calls_by_op": calls_by_op,
        "exec_ms": 100.0,
        "blks_hit": 1000,
        "blks_read": 10,
        "wal_records": 5501,
        "wal_fpi": 5,
        "wal_bytes": 385408,
        "tup_ins": 0,
        "tup_upd": 500,
        "tup_del": 500,
        "tup_hot_upd": 0,
        "tup_newpage_upd": 500,
        "dead_tup": 0,
        "heap_bytes": 65536,
        "index_bytes": 32768,
    }
    defaults.update(overrides)
    return ProbeResult(**defaults)


def _result(
    scenario: str = "consumer", *, messages: int = 500, wall_seconds: float = 2.0, **probe_kwargs: float
) -> RunResult:
    return RunResult(
        scenario=scenario,
        config=RunConfig(messages=messages),
        wall_seconds=wall_seconds,
        probe=_probe(**probe_kwargs),  # ty: ignore[invalid-argument-type]
    )


def _producer(*, select_calls: int = 500, **probe_kwargs: float) -> RunResult:
    """Build a producer run: inserts + pg_notify selects, no deletes or updates."""
    return _result(
        "producer",
        delete_calls=0,
        insert_calls=500,
        select_calls=select_calls,
        tup_ins=500,
        tup_upd=0,
        tup_del=0,
        **probe_kwargs,  # ty: ignore[invalid-argument-type]
    )


def test_normalize_divides_by_message_count() -> None:
    metrics = normalize(_result())
    assert metrics["round_trips_per_msg"] == pytest.approx(1.044)
    assert metrics["wal_records_per_msg"] == pytest.approx(11.002)
    assert metrics["delete_calls_per_msg"] == pytest.approx(1.0)
    assert metrics["msgs_per_second"] == pytest.approx(250.0)
    # delete_calls is surfaced from calls_by_op, not from the `calls` total.
    assert metrics["delete_calls"] == 500
    # Raw totals survive normalization -- the gate compares these.
    assert metrics["tup_del"] == 500
    assert metrics["tup_upd"] == 500
    assert metrics["messages"] == 500
    assert metrics["wall_seconds"] == pytest.approx(2.0)


def test_compare_passes_on_identical_results() -> None:
    baseline = to_baseline([_result()])
    assert compare(to_baseline([_result()]), baseline) == []


def test_compare_fails_exactly_on_extra_delete_call() -> None:
    # One extra terminal DELETE per run is the core regression the gate catches.
    baseline = to_baseline([_result()])
    current = to_baseline([_result(delete_calls=501)])
    failures = compare(current, baseline)
    assert len(failures) == 1
    assert "delete_calls" in failures[0]
    assert "501" in failures[0]


def test_compare_fails_on_tup_del_drift() -> None:
    baseline = to_baseline([_result()])
    failures = compare(to_baseline([_result(tup_del=499)]), baseline)
    assert len(failures) == 1
    assert "tup_del" in failures[0]


def test_compare_fails_on_tup_upd_drift() -> None:
    baseline = to_baseline([_result()])
    failures = compare(to_baseline([_result(tup_upd=501)]), baseline)
    assert len(failures) == 1
    assert "tup_upd" in failures[0]


def test_compare_tolerates_small_wal_record_drift() -> None:
    baseline = to_baseline([_result(wal_records=5501)])
    # +3.3%, within the 10% band.
    assert compare(to_baseline([_result(wal_records=5683)]), baseline) == []


def test_compare_fails_on_large_wal_record_drift() -> None:
    baseline = to_baseline([_result(wal_records=5501)])
    # +20%, well past the 10% band.
    failures = compare(to_baseline([_result(wal_records=6601)]), baseline)
    assert len(failures) == 1
    assert "wal_records" in failures[0]


def test_compare_tolerates_lower_wal_records() -> None:
    # Fewer WAL records is less work, never a regression: only the upper bound fails.
    baseline = to_baseline([_result(wal_records=5501)])
    assert compare(to_baseline([_result(wal_records=4000)]), baseline) == []


def test_compare_never_gates_calls_wal_bytes_or_wall_clock() -> None:
    baseline = to_baseline([_result(calls=522, wal_bytes=385408, wall_seconds=2.0)])
    # Total calls (fetch-loop polling), WAL bytes (full-page images) and wall time all
    # swing wildly run to run -- none of them is gated.
    current = to_baseline([_result(calls=894, wal_bytes=639104, wall_seconds=9.0)])
    assert compare(current, baseline) == []


def test_compare_reports_a_missing_scenario() -> None:
    baseline = to_baseline([_result()])
    failures = compare({"runs": {}}, baseline)
    assert len(failures) == 1
    assert "missing" in failures[0].lower()


def test_consumer_and_producer_pass_against_themselves_with_same_exact_keys() -> None:
    # The same EXACT_KEYS set gates both: consumer deletes (insert/select 0), producer
    # inserts (delete 0). Exact-0 == exact-0 passes for the empty buckets.
    consumer = to_baseline([_result()])
    producer = to_baseline([_producer()])
    assert compare(consumer, consumer) == []
    assert compare(producer, producer) == []
    # And they are distinct runs, so a combined baseline keeps both.
    combined = to_baseline([_result(), _producer()])
    assert len(combined["runs"]) == 2
    assert compare(combined, combined) == []


def test_exact_keys_cover_the_structural_totals() -> None:
    assert set(EXACT_KEYS) == {
        "delete_calls",
        "tup_upd",
        "tup_del",
        "tup_ins",
        "insert_calls",
    }


def test_compare_absorbs_one_stray_select_on_a_consumer_run() -> None:
    """INVARIANT: a stray SELECT inside the measured window does not fail the gate.

    ``pg_stat_statements`` is database-wide and the probe excludes only its own queries
    (``NOT ILIKE '%pg_stat%'``), so driver and dialect introspection -- which is SELECT-shaped
    and fires whenever a physical connection is established inside the window -- is counted as
    workload work. SELECT is the one operation the environment also emits, so exact-gating it
    against a baseline of 0 fails the build on a timing accident rather than a regression.
    """
    baseline = to_baseline([_result()])
    assert compare(to_baseline([_result(select_calls=1)]), baseline) == []


def test_compare_still_fails_when_the_consume_path_starts_selecting_per_message() -> None:
    """INVARIANT: the slack absorbs infrastructure noise without hiding a per-message SELECT.

    The regression this metric exists to catch is a SELECT added to the consume or publish
    path, which scales with message count. A slack wide enough to hide that would make the
    metric decorative -- it has to stay far below one statement per message.
    """
    baseline = to_baseline([_result()])
    failures = compare(to_baseline([_result(select_calls=500)]), baseline)
    assert len(failures) == 1
    assert "select_calls" in failures[0]


def test_compare_fails_on_a_producer_losing_pg_notify_dedup() -> None:
    """INVARIANT: the producer's ``select_calls`` still pins per-transaction NOTIFY dedup.

    ``_notify`` emits at most one ``pg_notify`` per ``(transaction, queue)``, so a real
    producer run selects once regardless of message count. Losing the dedup memo turns that
    into one SELECT per publish, which is the regression this number is here for; the slack
    must not reach far enough to let it through.
    """
    baseline = to_baseline([_producer(select_calls=1)])
    failures = compare(to_baseline([_producer(select_calls=500)]), baseline)
    assert len(failures) == 1
    assert "select_calls" in failures[0]


def test_compare_fails_when_the_fetch_loop_claims_one_row_at_a_time() -> None:
    """INVARIANT: ``fetch_calls`` stays within twice its baseline.

    The fetch loop claims once half a batch of in-flight slots is free, so the claim count sits
    between ``messages / fetch_batch_size`` and ``messages / (fetch_batch_size // 2)`` depending
    on how far the loop lags the workers. A loop that refetches whenever one slot frees runs the
    fetch CTE about once per message, far outside that range.
    """
    baseline = to_baseline([_result(fetch_calls=12)])
    assert compare(to_baseline([_result(fetch_calls=24)]), baseline) == []
    failures = compare(to_baseline([_result(fetch_calls=500)]), baseline)
    assert len(failures) == 1
    assert "fetch_calls" in failures[0]


def test_compare_fails_when_the_fetch_connection_opens_transactions() -> None:
    """INVARIANT: ``other_calls`` (BEGIN, COMMIT and other utility statements) stays near its baseline.

    The fetch and writer connections run in autocommit, so a consumer run sends no BEGIN or
    COMMIT. Losing autocommit on either adds two round trips per fetch or per terminal write,
    which only shows up here.
    """
    baseline = to_baseline([_result(other_calls=0)])
    assert compare(to_baseline([_result(other_calls=2)]), baseline) == []
    failures = compare(to_baseline([_result(other_calls=24)]), baseline)
    assert len(failures) == 1
    assert "other_calls" in failures[0]


def test_format_table_labels_gated_vs_informational() -> None:
    table = format_table([_result(), _producer()])
    # Both runs are rendered.
    assert "consumer/w1/b100" in table
    assert "producer/w1/b100" in table
    # Required columns are present.
    for column in ("msg/s", "delete/msg", "WALrec/msg", "WALB/msg", "fpi", "upd", "del", "dead_tup"):
        assert column in table
    # The gated vs informational split is legible in the footer.
    assert "GATED" in table
    assert "INFORMATIONAL" in table
    assert "wal_records" in table
    assert "delete_calls" in table


def test_format_markdown_renders_passing_verdict_and_table() -> None:
    body = format_markdown([_result(), _producer()], [])
    assert body.startswith("## Benchmark gate")
    assert "✅ gate passed" in body
    # Both runs render as rows.
    assert "consumer/w1/b100" in body
    assert "producer/w1/b100" in body
    # GitHub table shape: a header row and a delimiter row.
    assert "| scenario |" in body
    assert "| --- |" in body


def test_format_markdown_renders_failing_verdict_and_bullets() -> None:
    failures = ["consumer/w1/b100: delete_calls changed (exact-gated): baseline 500 -> current 501"]
    body = format_markdown([_result()], failures)
    assert "❌ gate FAILED" in body
    assert "- consumer/w1/b100: delete_calls changed" in body


def test_run_rejects_the_markdown_flag() -> None:
    # --markdown belongs to `check`; subparsers make it a hard error under `run`
    # instead of the old silent no-op.
    with pytest.raises(SystemExit):
        _build_parser().parse_args(["run", "--markdown"])


def test_check_rejects_run_only_flags() -> None:
    # Symmetrically, run-only flags are not accepted by `check`.
    with pytest.raises(SystemExit):
        _build_parser().parse_args(["check", "--write-baseline"])


def test_check_accepts_markdown() -> None:
    args = _build_parser().parse_args(["check", "--markdown"])
    assert args.command == "check"
    assert args.markdown is True


def test_run_accepts_its_flags() -> None:
    args = _build_parser().parse_args(["run", "--messages", "200", "--write-baseline"])
    assert args.command == "run"
    assert args.messages == 200
    assert args.write_baseline is True


def test_a_subcommand_is_required() -> None:
    with pytest.raises(SystemExit):
        _build_parser().parse_args([])


def test_baseline_keys_name_the_interval_and_transaction_variants() -> None:
    default_interval = RunResult(
        scenario="consumer",
        config=RunConfig(messages=500, fetch_interval=(1.0, 10.0)),
        wall_seconds=1.0,
        probe=_probe(),
    )
    per_transaction = RunResult(
        scenario="producer",
        config=RunConfig(messages=500, publishes_per_transaction=1),
        wall_seconds=1.0,
        probe=_probe(),
    )
    assert set(to_baseline([_result(), default_interval, per_transaction])["runs"]) == {
        "consumer/w1/b100",
        "consumer/w1/b100/interval1-10",
        "producer/w1/b100/ppt1",
    }


def test_percentiles_use_nearest_rank() -> None:
    assert percentiles([float(n) for n in range(1, 101)]) == {"p50": 50.0, "p95": 95.0, "p99": 99.0, "max": 100.0}


def test_percentiles_of_nothing_are_zero() -> None:
    assert percentiles([]) == {"p50": 0.0, "p95": 0.0, "p99": 0.0, "max": 0.0}


def test_extend_config_derives_the_ping_interval_and_silent_share() -> None:
    cfg = ExtendConfig(name="p", rate=1_000, timers=2_000)
    assert (cfg.ping_interval, cfg.silent_timers, cfg.silence_at) == (2.0, 100, 7.5)


def test_extend_config_rejects_pings_slower_than_the_timeout() -> None:
    with pytest.raises(ValueError, match=r"pings every 6\.00s"):
        ExtendConfig(name="p", rate=1_000, timers=6_000)


def test_extend_config_rejects_a_run_too_short_for_silent_timers_to_fire() -> None:
    with pytest.raises(ValueError, match="too short"):
        ExtendConfig(name="p", rate=1_000, timers=2_000, seconds=10.0)


def _sample(elapsed: float, *, table_mb: float = 1.0, index_mb: float = 1.0, dead: int = 0, **kw: float) -> Sample:
    fields: dict[str, float] = {"live_tuples": 2_000, "autovacuum_count": 0, "fetch_calls": 0, "fetch_ms": 0.0}
    fields.update(kw)
    return Sample(
        elapsed=elapsed,
        table_bytes=int(table_mb * 2**20),
        index_bytes=int(index_mb * 2**20),
        dead_tuples=dead,
        **fields,
    )


def _extend_result(*, samples: tuple[Sample, ...] = (), **cfg_kwargs: object) -> ExtendResult:
    return ExtendResult(
        config=ExtendConfig(name="r1000-b1", rate=1_000, timers=2_000, **cfg_kwargs),  # ty: ignore[invalid-argument-type]
        wall_seconds=30.0,
        offered=30_000,
        offered_seconds=30.0,
        committed=30_000,
        transactions=30_000,
        leased=2,
        errors={"TimeoutError": 5},
        saturated=True,
        latency={"p50": 0.002, "p95": 0.004, "p99": 0.017, "max": 0.05},
        pool_wait={"p50": 0.0, "p95": 0.0, "p99": 0.0003, "max": 0.001},
        lateness={"p50": 0.98, "p95": 1.0, "p99": 1.0, "max": 1.02},
        expired_silent=100,
        false_expiries=0,
        statement_ms={"insert": (0.024, 0.5), "with": (0.1, 0.4)},
        autovacuum_count=1,
        live_tuples=2_000,
        probe=_probe(insert_calls=30_000, wal_bytes=18_000_000, wal_records=199_000, tup_upd=30_000, tup_hot_upd=0),
        samples=samples,
    )


def test_format_extend_reports_saturation_and_per_ping_wal() -> None:
    table = format_extend([_extend_result()])
    assert "| r1000-b1/tuned (saturated) | 1,000 | 1,000 | 1,000 | 2.0 / 4.0 / 17.0 |" in table
    assert "TimeoutError 5" in table
    assert "| r1000-b1/tuned | 600 | 6.63 |" in table
    assert "| 100 / 100 | 0 | 0.98 / 1.02 | 2 |" in table


def test_extend_accepts_its_flags() -> None:
    args = _build_parser().parse_args(["extend", "--seconds", "12", "--only", "r1000-b1", "--json", "out.json"])
    assert (args.seconds, args.only, args.json) == (12.0, ["r1000-b1"], "out.json")


def test_extend_config_rejects_an_unknown_vacuum_variant() -> None:
    with pytest.raises(ValueError, match="vacuum"):
        ExtendConfig(name="p", rate=1_000, timers=2_000, vacuum="off")  # ty: ignore[invalid-argument-type]


def test_extend_config_rejects_a_non_positive_sample_interval() -> None:
    with pytest.raises(ValueError, match="sample_interval"):
        ExtendConfig(name="p", rate=1_000, timers=2_000, sample_interval=0.0)


@pytest.mark.parametrize(
    ("variant", "expected"),
    [
        ("untuned", None),
        (
            "tuned",
            (
                "ALTER TABLE t SET (autovacuum_vacuum_scale_factor = 0, autovacuum_vacuum_threshold = 1000, "
                "autovacuum_vacuum_insert_scale_factor = 0, autovacuum_vacuum_insert_threshold = 1000)"
            ),
        ),
        (
            "unthrottled",
            (
                "ALTER TABLE t SET (autovacuum_vacuum_scale_factor = 0, autovacuum_vacuum_threshold = 1000, "
                "autovacuum_vacuum_insert_scale_factor = 0, autovacuum_vacuum_insert_threshold = 1000, "
                "autovacuum_vacuum_cost_delay = 0)"
            ),
        ),
    ],
)
def test_autovacuum_ddl_per_variant(variant: str, expected: str | None) -> None:
    cfg = ExtendConfig(name="p", rate=1_000, timers=2_000, vacuum=variant)  # ty: ignore[invalid-argument-type]
    assert autovacuum_ddl(cfg, "t") == expected


def test_fetch_mean_ms_divides_the_window_deltas() -> None:
    samples = [
        _sample(0.0, fetch_calls=0, fetch_ms=0.0),
        _sample(60.0, fetch_calls=100, fetch_ms=20.0),
        _sample(120.0, fetch_calls=300, fetch_ms=120.0),
    ]
    assert fetch_mean_ms(samples, 0.0, 60.0) == pytest.approx(0.2)
    assert fetch_mean_ms(samples, 60.0, 120.0) == pytest.approx(0.5)


def test_fetch_mean_ms_is_zero_without_fetches_in_the_window() -> None:
    assert fetch_mean_ms([_sample(0.0), _sample(60.0)], 0.0, 60.0) == 0.0
    assert fetch_mean_ms([], 0.0, 60.0) == 0.0


def test_format_extend_reports_drift_over_the_run() -> None:
    samples = (
        _sample(0.0, table_mb=1.0, index_mb=0.5, dead=0, fetch_calls=0, fetch_ms=0.0),
        _sample(60.0, table_mb=3.0, index_mb=2.0, dead=50_000, autovacuum_count=1, fetch_calls=100, fetch_ms=25.0),
        _sample(120.0, table_mb=2.5, index_mb=2.5, dead=10_000, autovacuum_count=2, fetch_calls=200, fetch_ms=125.0),
    )
    table = format_extend([_extend_result(vacuum="unthrottled", samples=samples)])
    assert "| r1000-b1/unthrottled | 1.0 / 2.5 / 3.0 | 0.5 / 2.5 / 2.5 | 50,000 | 2 | 0.250 / 1.000 |" in table


def test_format_extend_skips_drift_without_samples() -> None:
    assert "table MB" not in format_extend([_extend_result()])


def test_extend_accepts_vacuum_variants_and_sample_interval() -> None:
    args = _build_parser().parse_args(["extend", "--vacuum", "untuned", "unthrottled", "--sample-interval", "5"])
    assert (args.vacuum, args.sample_interval) == (["untuned", "unthrottled"], 5.0)


def test_extend_points_cross_points_with_vacuum_variants() -> None:
    points = _extend_points(
        only=["r1000-b1", "r3000-b1"], seconds=900.0, vacuum=["untuned", "tuned"], sample_interval=5.0
    )
    assert [(p.name, p.vacuum, p.seconds, p.sample_interval) for p in points] == [
        ("r1000-b1", "untuned", 900.0, 5.0),
        ("r1000-b1", "tuned", 900.0, 5.0),
        ("r3000-b1", "untuned", 900.0, 5.0),
        ("r3000-b1", "tuned", 900.0, 5.0),
    ]


def test_extend_points_default_to_every_point_tuned() -> None:
    points = _extend_points(only=None, seconds=None, vacuum=None, sample_interval=None)
    assert {p.vacuum for p in points} == {"tuned"}
    assert len(points) == 8
