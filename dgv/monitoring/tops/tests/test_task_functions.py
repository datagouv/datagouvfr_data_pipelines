"""Unit tests for the Matomo "tops" monitoring stats.

`get_stats` computes the Matomo query date range for each period type
(day/week/month/year). The network call is isolated behind
``compute_general``, which these tests monkeypatch.
"""

from datetime import datetime

from datagouvfr_data_pipelines.dgv.monitoring.tops import task_functions as tf
from datagouvfr_data_pipelines.dgv.monitoring.tops.task_functions import get_stats


def run_get_stats(monkeypatch, days, period):
    calls = []
    monkeypatch.setattr(
        tf, "compute_general", lambda date: calls.append(date) or ([], [], [])
    )
    return calls, get_stats(days, period)


def test_get_stats_month_builds_range(monkeypatch):
    days = [datetime(2026, 11, 26)]
    calls, result = run_get_stats(monkeypatch, days, "month")
    assert calls == ["2026-11-26,2026-12-26"]
    assert [len(series) for series in result] == [1, 1, 1]


def test_get_stats_year_builds_range(monkeypatch):
    days = [datetime(2026, 11, 26)]
    calls, result = run_get_stats(monkeypatch, days, "year")
    assert calls == ["2026-11-26,2027-11-26"]
    assert [len(series) for series in result] == [1, 1, 1]


def test_get_stats_day_uses_single_date(monkeypatch):
    days = [datetime(2026, 11, 26)]
    calls, result = run_get_stats(monkeypatch, days, "day")
    assert calls == ["2026-11-26"]
    assert [len(series) for series in result] == [1, 1, 1]


def test_get_stats_week_uses_single_date(monkeypatch):
    days = [datetime(2026, 11, 26)]
    calls, result = run_get_stats(monkeypatch, days, "week")
    assert calls == ["2026-11-26"]
    assert [len(series) for series in result] == [1, 1, 1]
