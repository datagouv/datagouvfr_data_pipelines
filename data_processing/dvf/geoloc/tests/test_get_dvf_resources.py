"""Unit tests for get_dvf_resources in the DVF geoloc task_functions."""

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


def _year_resources(years):
    return [
        _Resource(f"https://example.com/valeursfoncieres-{y}.txt.zip") for y in years
    ]


def test_returns_main_resources_with_metadata():
    resources = _year_resources([2021, 2022, 2023, 2024, 2025])
    out = get_dvf_resources(resources)
    assert [dvf.file_name for dvf in out] == [
        f"valeursfoncieres-{y}.txt.zip" for y in [2021, 2022, 2023, 2024, 2025]
    ]
    assert [dvf.year for dvf in out] == ["2021", "2022", "2023", "2024", "2025"]
    assert [dvf.resource for dvf in out] == resources


def test_ignores_non_main_resources():
    resources = _year_resources([2021, 2022, 2023, 2024, 2025])
    resources.append(_Resource("https://example.com/other.csv", type="documentation"))
    out = get_dvf_resources(resources)
    assert all(dvf.file_name != "other.csv" for dvf in out)
    assert len(out) == 5


def test_accepts_six_resources():
    resources = _year_resources([2020, 2021, 2022, 2023, 2024, 2025])
    assert len(get_dvf_resources(resources)) == 6


@pytest.mark.parametrize("count", [0, 1, 4, 7])
def test_rejects_unexpected_number_of_resources(count):
    resources = _year_resources(range(count))
    with pytest.raises(ValueError, match="Unexpected number of resources"):
        get_dvf_resources(resources)


def test_rejects_unexpected_file_names():
    bad = _Resource("https://example.com/valeursfoncieres-2021.txt", type="main")
    resources = _year_resources([2021, 2022, 2023, 2024, 2025])
    resources[0] = bad
    with pytest.raises(ValueError, match="Unexpected file name"):
        get_dvf_resources(resources)


# TODO(SL): fix, because resources in the current dataset contains the semester for 2021 and 2026
# https://static.data.gouv.fr/resources/demandes-de-valeurs-foncieres/20261012-171609/valeursfoncieres-2021-s2.txt.zip
# https://static.data.gouv.fr/resources/demandes-de-valeurs-foncieres/20261012-171648/valeursfoncieres-2026-s1.txt.zip
@pytest.mark.xfail(reason="not implemented")
def test_accepts_semester():
    good = _Resource("https://example.com/valeursfoncieres-2021-s2.txt.zip", type="main")
    resources = _year_resources([2021, 2022, 2023, 2024, 2025])
    resources[0] = good
    out = get_dvf_resources(resources)
    assert len(out) == 5
