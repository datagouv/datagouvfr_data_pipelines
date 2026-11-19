"""Unit tests for the resilient per-dataset quality fetch.

``update_dataset_quality`` and ``update_all_qualities`` must distinguish
recoverable failures (transport errors, HTTP errors) from DAG/API-contract bugs
(unexpected payload), while aborting the whole pass when the API stays
unreachable so that a mix of obsolete and fresh metrics is never published.
"""

import logging

import pandas as pd
import pytest
import requests
from datagouvfr_data_pipelines.dgv.monitoring.hvd.task_functions import (
    DatagouvApiUnreachable,
    update_all_qualities,
    update_dataset_quality,
)


class FakeResponse:
    def __init__(self, ok=True, status_code=200, payload=None):
        self.ok = ok
        self.status_code = status_code
        self._payload = payload

    def json(self):
        if self._payload is None:
            raise requests.exceptions.JSONDecodeError("Expecting value", "", 0)
        return self._payload


class FakeTable:
    def __init__(self):
        self.calls = []

    def update_records(self, conditions, new_values):
        self.calls.append((conditions, new_values))


def fake_get(response=None, exc=None):

    def get(url, headers=None, **kwargs):
        if exc is not None:
            raise exc
        assert headers == {"X-fields": "quality"}
        return response

    return get


def test_updates_quality_and_sets_unreachable_false():
    table = FakeTable()
    payload = {"quality": {"score": 0.7, "this_field": 1, "that_field": 2}}
    assert update_dataset_quality(
        "abc123", table, get=fake_get(response=FakeResponse(payload=payload))
    )
    assert table.calls == [
        ({"id2": "abc123"}, {"this_field": 1, "that_field": 2, "unreachable": False})
    ]


def test_http_error_marks_unreachable_and_returns_false(caplog):
    table = FakeTable()
    assert not update_dataset_quality(
        "abc123",
        table,
        get=fake_get(response=FakeResponse(ok=False, status_code=503)),
    )
    assert table.calls == [({"id2": "abc123"}, {"unreachable": True})]
    assert "HTTP 503" in caplog.text


def test_connection_error_raises_after_retries():
    table = FakeTable()
    exc = requests.exceptions.ConnectionError(
        "Remote end closed connection without response"
    )
    with pytest.raises(DatagouvApiUnreachable) as excinfo:
        update_dataset_quality("abc123", table, get=fake_get(exc=exc))
    assert "abc123" in str(excinfo.value)
    assert "ConnectionError" in str(excinfo.value)
    assert table.calls == []


def test_unexpected_payload_raises():
    table = FakeTable()
    with pytest.raises(RuntimeError, match="abc123"):
        update_dataset_quality(
            "abc123",
            table,
            get=fake_get(response=FakeResponse(payload={})),
        )


def test_non_dict_quality_raises():
    table = FakeTable()
    with pytest.raises(RuntimeError, match="abc123"):
        update_dataset_quality(
            "abc123",
            table,
            get=fake_get(response=FakeResponse(payload={"quality": None})),
        )


def test_update_all_qualities_counts_results(caplog):
    caplog.set_level(logging.INFO)
    table = FakeTable()
    payload = {"quality": {"this_field": 1}}
    get = fake_get(response=FakeResponse(payload=payload))
    updated, unreachable = update_all_qualities(
        pd.Series(["a", "b", "c"]), table, get=get
    )
    assert (updated, unreachable) == (3, 0)
    assert len(table.calls) == 3
    assert "updated 3 datasets" in caplog.text


def test_update_all_qualities_mixed(caplog):
    caplog.set_level(logging.INFO)
    table = FakeTable()

    def get_with_mix(url, headers=None, **kwargs):
        if url.endswith("/bad/"):
            return FakeResponse(ok=False, status_code=404)
        return FakeResponse(payload={"quality": {"this_field": 1}})

    updated, unreachable = update_all_qualities(
        pd.Series(["ok1", "bad", "ok2"]), table, get=get_with_mix
    )
    assert (updated, unreachable) == (2, 1)
    assert "updated 2 datasets, marked 1 unreachable" in caplog.text


def test_update_all_qualities_aborts_on_api_down():
    table = FakeTable()
    exc = requests.exceptions.ConnectionError("boom")
    with pytest.raises(DatagouvApiUnreachable):
        update_all_qualities(
            pd.Series(["a", "b"]),
            table,
            get=fake_get(exc=exc),
        )
