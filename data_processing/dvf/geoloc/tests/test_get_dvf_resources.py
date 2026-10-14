"""Unit tests for get_dvf_resources in the DVF geoloc task_functions."""

from datetime import date

import pytest
from datagouvfr_data_pipelines.data_processing.dvf.geoloc.task_functions import (
    get_dvf_resources,
)


class _Resource:
    """Minimal stand-in for a data.gouv.fr resource."""

    def __init__(self, url, type="main", title=None):
        self.url = url
        self.type = type
        self.title = title or url.split("/")[-1]


def _full_year(y):
    return _Resource(f"https://example.com/valeursfoncieres-{y}.txt.zip")


def _semester(y, s):
    return _Resource(f"https://example.com/valeursfoncieres-{y}-{s}.txt.zip")


# October delivery window: oldest last semester + 4 full years + newest first semester
def _october_resources():
    return [
        _semester(2021, "s2"),
        _full_year(2022),
        _full_year(2023),
        _full_year(2024),
        _full_year(2025),
        _semester(2026, "s1"),
    ]


def test_april_delivery_returns_full_years_without_semester():
    resources = [_full_year(y) for y in [2021, 2022, 2023, 2024, 2025]]
    out = get_dvf_resources(resources, delivery="april")
    assert [dvf.file_name for dvf in out] == [
        f"valeursfoncieres-{y}.txt.zip" for y in [2021, 2022, 2023, 2024, 2025]
    ]
    assert [dvf.year for dvf in out] == ["2021", "2022", "2023", "2024", "2025"]
    assert all(dvf.semester is None for dvf in out)
    assert [dvf.resource for dvf in out] == resources


def test_october_delivery_accepts_end_semesters():
    resources = _october_resources()
    out = get_dvf_resources(resources, delivery="october")
    assert len(out) == 6
    assert out[0].file_name == "valeursfoncieres-2021-s2.txt.zip"
    assert out[0].semester == "s2"
    assert out[-1].file_name == "valeursfoncieres-2026-s1.txt.zip"
    assert out[-1].semester == "s1"
    assert [dvf.semester for dvf in out[1:-1]] == [None, None, None, None]


def test_ignores_non_main_resources():
    resources = [_full_year(y) for y in [2021, 2022, 2023, 2024, 2025]]
    resources.append(_Resource("https://example.com/other.csv", type="documentation"))
    out = get_dvf_resources(resources, delivery="april")
    assert all(dvf.file_name != "other.csv" for dvf in out)
    assert len(out) == 5


@pytest.mark.parametrize(
    "delivery, build",
    [
        # april wants exactly five full years
        (
            "april",
            lambda: [_full_year(y) for y in [2020, 2021, 2022, 2023, 2024, 2025]],
        ),
        # october wants six files
        (
            "october",
            lambda: [_full_year(y) for y in [2021, 2022, 2023, 2024, 2025]],
        ),
        # april must not contain semester files
        (
            "april",
            lambda: [
                _full_year(2021),
                _semester(2022, "s2"),
                _full_year(2023),
                _full_year(2024),
                _full_year(2025),
            ],
        ),
        # october needs the oldest year to hold s2 and the newest s1
        (
            "october",
            lambda: [
                _semester(2021, "s1"),
                _full_year(2022),
                _full_year(2023),
                _full_year(2024),
                _full_year(2025),
                _semester(2026, "s2"),
            ],
        ),
        # october must not have a semester in the middle (full years only)
        (
            "october",
            lambda: [
                _semester(2021, "s2"),
                _full_year(2022),
                _semester(2023, "s1"),
                _full_year(2024),
                _full_year(2025),
                _semester(2026, "s1"),
            ],
        ),
    ],
)
def test_rejects_window_mismatch(delivery, build):
    resources = build()
    with pytest.raises(
        ValueError,
        match="Unexpected number of resources|Unexpected semester"
        "|Oldest year|Newest year",
    ):
        get_dvf_resources(resources, delivery=delivery)


def test_rejects_unexpected_number_of_resources():
    resources = [_full_year(y) for y in [2021, 2022, 2023, 2024]]
    with pytest.raises(ValueError, match="Unexpected number of resources"):
        get_dvf_resources(resources, delivery="april")


def test_rejects_unexpected_file_names():
    bad = _Resource("https://example.com/valeursfoncieres-2021.txt", type="main")
    resources = [_full_year(y) for y in [2021, 2022, 2023, 2024, 2025]]
    resources[0] = bad
    with pytest.raises(ValueError, match="Unexpected file name"):
        get_dvf_resources(resources, delivery="april")


def test_rejects_unknown_delivery():
    resources = _october_resources()
    with pytest.raises(ValueError, match="Unknown delivery"):
        get_dvf_resources(resources, delivery="sometime")


def test_delivery_inferred_from_today():
    resources = [_full_year(y) for y in [2021, 2022, 2023, 2024, 2025]]
    out = get_dvf_resources(resources, today=date(2026, 4, 15))
    assert len(out) == 5
