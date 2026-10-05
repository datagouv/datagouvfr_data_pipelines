"""Regression test for the CSV export of the dashboard support stats.

Reproduces the bug fixed in datagouv/datagouvfr_data_pipelines#760.

Before the fix, ``gather_and_upload`` exported the (transposed) ``stats``
DataFrame with ``stats.to_csv(...)`` but without an ``index_label``. Because the
``stats`` frame is transposed (``.T``), the original column names become the
index, and this index is written as the CSV's first column with an *empty*
header. The resulting first line looks like::

    ,2026-01,2026-02
    Page support,11,22
    ...

The empty first column header makes ``csv-detective`` fail with
``Could not accurately detect the file's columns``.

The fix adds ``index_label="Métrique"`` so the first column is labeled, which is
exactly what this test asserts. It therefore fails without the fix and passes
with it.
"""

import sys
from unittest.mock import MagicMock

import pandas as pd

# Pop the conftest stub so the *real* task_functions module is imported.
_MFQ = "datagouvfr_data_pipelines.dgv.monitoring.dashboard.task_functions"
sys.modules.pop(_MFQ, None)
from datagouvfr_data_pipelines.dgv.monitoring.dashboard import (  # noqa: E402
    task_functions,
)


def _run_gather_and_upload(tmp_path, monkeypatch):
    """Drive the real ``gather_and_upload`` and return the generated CSV."""
    monkeypatch.setattr(task_functions, "TMP_FOLDER", str(tmp_path) + "/")

    months = ["2026-01", "2026-02", "2026-03"]
    tickets = {"all": [2, 3, 4], "hs": [0, 1, 1], "spam": [0, 0, 0]}
    support = {"support": [10, 20, 30], "/support": [1, 2, 3]}

    ti = MagicMock()

    def xcom_pull(key, task_ids=None):
        if key == "tickets":
            return tickets
        if key == "months":
            return months
        return support[key]

    ti.xcom_pull.side_effect = xcom_pull
    context = {"ti": ti}

    task_functions.gather_and_upload(**context)

    path = tmp_path / "stats_support.csv"
    assert path.exists(), "stats_support.csv was not generated"
    return path


def test_stats_support_first_column_is_labeled(tmp_path, monkeypatch):
    """The exported CSV's first column must carry the 'Métrique' header.

    This is the exact behaviour restored by PR #760: without the fix the first
    header cell is empty, which breaks csv-detective analysis.
    """
    path = _run_gather_and_upload(tmp_path, monkeypatch)
    df = pd.read_csv(path)
    assert df.columns[0] == "Métrique"


def test_stats_support_first_column_header_not_empty(tmp_path, monkeypatch):
    """Assert the header line has no empty first cell (the reproduce-able bug).

    Before the fix the header line began with a comma. We read the raw header
    line and ensure its first field is non-empty.
    """
    path = _run_gather_and_upload(tmp_path, monkeypatch)
    with open(path, encoding="utf-8") as f:
        header = f.readline().strip()
    first_cell = header.split(",")[0]
    assert first_cell == "Métrique"
    assert first_cell != ""


def test_stats_support_csv_keeps_metrics_and_months(tmp_path, monkeypatch):
    """Sanity check: rows are the metrics, columns are the months."""
    path = _run_gather_and_upload(tmp_path, monkeypatch)
    df = pd.read_csv(path)
    assert list(df["Métrique"]) == [
        "Page support",
        "Ouverture de ticket",
        "Ticket hors-sujet",
        "Ticket spam",
    ]
    assert list(df.columns[1:]) == ["2026-01", "2026-02"]
