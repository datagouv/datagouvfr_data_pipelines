"""Tests for the daily DAG summary (Récap quotidien DAGs).

See issue #836: DAGs with failures must be listed last, sorted by failure
ratio (best on top, worst at the bottom).
See issue #847: the summary message must be Markdown that stays correctly
nested after rendering, so that a DAG ending with a nested "tâches en échec"
list does not absorb the following DAG one level too deep (Python-Markdown
quirk, see Python-Markdown issue #845).
"""

import re

import markdown

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


def run(ts: str, failed_tasks: dict | None) -> dict:
    """A run state shaped like the values produced by `monitor_dags`."""
    if failed_tasks is None:
        return {
            "success": False,
            "end_date": ts,
            "status": "success",
            "failed_tasks": {},
        }
    return {
        "success": False,
        "end_date": ts,
        "status": "failed",
        "failed_tasks": failed_tasks,
    }


def build_daily_summary_md(dags: list[tuple[str, str]]) -> tuple[str, set[str]]:
    """Build the markdown message and ping set for the given DAGs as
    (config key, actual id).

    Two DAGs are always provided: the first fails with a nested "tâches en
    échec" list and the second (a plain failure), so a regression (issue #847)
    would absorb the second DAG one level too deep.
    """
    dag_a_key, dag_a_id = dags[0]
    dag_b_key, dag_b_id = dags[1]
    runs = {
        dag_a_id: {
            "2024-01-01T10:00:00Z": run(
                "2024-01-01T10:00:00Z", {"intermediate_task": "http://log/1"}
            ),
        },
        dag_b_id: {
            "2024-01-01T11:00:00Z": run("2024-01-01T11:00:00Z", None),
        },
    }
    dag_ids = {dag_a_id: dag_a_key, dag_b_id: dag_b_key}
    return tf.build_daily_summary(runs, dag_ids)


def test_daily_summary_no_dag_absorbed_by_nested_list():
    """Regression test for issue #847: a DAG whose entry ends with a nested
    "tâches en échec" list must not absorb the following DAG one level too
    deep once the message is rendered."""
    dags = [
        ("data_processing_geozones", "data_processing_geozones"),
        ("data_processing_finess", "data_processing_finess"),
    ]
    md, _ = build_daily_summary_md(dags)

    html = markdown.markdown(md)

    # Every DAG header must be a direct child <li> of the top-level <ul>
    # (list nesting depth 1). A regression (issue #847) would push the second
    # DAG one level deeper.
    compact = re.sub(r">\s+<", "><", html)
    depth = 0
    dag_depth: dict[str, int] = {}
    for match in re.finditer(r"(?:<li><strong>[^<]+</strong>|</?ul>)", compact):
        tag = match.group(0)
        if tag.startswith("<li>"):
            dag_depth.setdefault(tag, depth)
        elif tag.startswith("</"):
            depth -= 1
        else:
            depth += 1
    for _, dag_id in dags:
        header = f"<li><strong>{dag_id}</strong>"
        assert dag_depth.get(header) == 1, (
            f"{dag_id} ended up {dag_depth.get(header)} levels deep instead of 1:\n{html}"
        )


def test_daily_summary_pings_owners_of_failing_dags():
    """Both DAGs fail (failure ratio > 2%), so their owners must be pinged.

    Guards issue #847's fix against regressing the ping semantics (mentions
    must be kept inline, on the failure line, and still returned for sending).
    """
    dags = [
        ("data_processing_geozones", "data_processing_geozones"),
        ("data_processing_finess", "data_processing_finess"),
    ]
    md, ping = build_daily_summary_md(dags)

    expected = set()
    for _, dag_id in dags:
        owners = tf.get_owners(tf.config[dag_id])
        expected |= set(owners)
        # mention appears inline on the failure line
        assert all(f"@{owner}" in md for owner in owners)
    assert ping == expected
