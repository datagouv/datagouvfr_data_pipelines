"""Tests for the daily DAG summary (Récap quotidien DAGs).

See issue #836: DAGs with failures must be listed last, sorted by failure
ratio (best on top, worst at the bottom).
"""

from datagouvfr_data_pipelines.meta import task_functions as tf


def ok(run_id: str) -> dict:
    return {"success": True, "duration": 1}


def ko(run_id: str) -> dict:
    return {"success": False, "status": "failed", "failed_tasks": {}}


def test_failed_runs_ratio_no_failure():
    assert tf.failed_runs_ratio({"r1": ok("r1")}) == 0.0


def test_failed_runs_ratio_all_failures():
    assert tf.failed_runs_ratio({"r1": ko("r1")}) == 1.0


def test_failed_runs_ratio_mixed():
    attempts = {"r1": ok("r1"), "r2": ok("r2"), "r3": ko("r3")}
    assert tf.failed_runs_ratio(attempts) == 1 / 3


def test_failed_runs_ratio_empty():
    assert tf.failed_runs_ratio({}) == 0.0


def test_order_ok_dags_first_alphabetically():
    dag_a = {"r1": ok("r1")}
    dag_b = {"r1": ok("r1")}
    ordered = [
        dag for dag, _ in tf.sort_dags_by_failure_ratio({"b": dag_b, "a": dag_a})
    ]
    assert ordered == ["a", "b"]


def test_failing_dags_last_sorted_by_ratio():
    # 2 successes / 1 failure  -> ratio ~0.33
    mixed = {"r1": ok("r1"), "r2": ok("r2"), "r3": ko("r3")}
    # 0 success / 1 failure    -> ratio 1.0
    all_ko = {"r4": ko("r4")}
    # 1 success / 3 failures   -> ratio 0.75
    many_ko = {"r5": ok("r5"), "r6": ko("r6"), "r7": ko("r7"), "r8": ko("r8")}
    attempts = {
        "all_ko": all_ko,
        "mixed": mixed,
        "many_ko": many_ko,
        "dag_ok": {"r9": ok("r9")},
    }
    ordered = [dag for dag, _ in tf.sort_dags_by_failure_ratio(attempts)]
    assert ordered == ["dag_ok", "mixed", "many_ko", "all_ko"]


def test_failing_dags_same_ratio_alphabetical():
    # both 1 success / 1 failure -> ratio 0.5, alphabetical tie-break
    ratio_a = {"r1": ok("r1"), "r2": ko("r2")}
    ratio_b = {"r3": ok("r3"), "r4": ko("r4")}
    ordered = [
        dag for dag, _ in tf.sort_dags_by_failure_ratio({"z": ratio_b, "a": ratio_a})
    ]
    assert ordered == ["a", "z"]
