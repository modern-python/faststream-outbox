"""Unit tests for the benchmark harness' pure logic.

Deliberately no Postgres: the pytest CI job's postgres service cannot preload
pg_stat_statements, so anything catalog-dependent is verified by `just bench-check`.
The gate compares RAW STRUCTURAL TOTALS (delete_calls, the tuple counters, wal_records),
which Task 3 proved load-independent -- NOT the total `calls`, which scales with the
fetch loop's wall-clock polling.
"""

import pytest

from benchmarks.config import RunConfig
from benchmarks.probes import ProbeResult
from benchmarks.report import EXACT_KEYS, compare, format_markdown, format_table, normalize, to_baseline
from benchmarks.workload import RunResult


def _probe(
    *,
    delete_calls: int = 500,
    insert_calls: int = 0,
    select_calls: int = 0,
    **overrides: float,
) -> ProbeResult:
    calls_by_op = {
        "select": select_calls,
        "insert": insert_calls,
        "update": 0,
        "delete": delete_calls,
        "with": 12,
        "other": 10,
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


def _producer(**probe_kwargs: float) -> RunResult:
    """Build a producer run: inserts + pg_notify selects, no deletes or updates."""
    return _result(
        "producer",
        delete_calls=0,
        insert_calls=500,
        select_calls=500,
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
        "select_calls",
    }


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
