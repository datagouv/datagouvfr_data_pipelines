"""Unit tests for get_dvf_resources (utils/dvf_resource).

The window shape validation (count, semester placement, consecutive years) is
already covered in test_delivery.py through the deliveries' ``check_dvf_versions``.
This file focuses on what ``get_dvf_resources`` adds on top: filtering the main
resources, parsing file names into versions, and sorting.
"""

import pytest
from datagouvfr_data_pipelines.data_processing.dvf.geoloc.utils.delivery import (
    AprilDelivery,
    OctoberDelivery,
)
from datagouvfr_data_pipelines.data_processing.dvf.geoloc.utils.dvf_resource import (
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


def test_april_maps_main_resources_to_full_years():
    resources = [_full_year(y) for y in [2021, 2022, 2023, 2024, 2025]]
    out = get_dvf_resources(resources, AprilDelivery())
    assert [dvf.resource for dvf in out] == resources
    assert [dvf.dvf_version.get_zip_file_name() for dvf in out] == [
        f"valeursfoncieres-{y}.txt.zip" for y in [2021, 2022, 2023, 2024, 2025]
    ]
    assert [dvf.dvf_version.year for dvf in out] == [
        "2021",
        "2022",
        "2023",
        "2024",
        "2025",
    ]
    assert all(dvf.dvf_version.semester is None for dvf in out)


def test_october_maps_end_semesters():
    resources = _october_resources()
    out = get_dvf_resources(resources, OctoberDelivery())
    assert len(out) == 6
    assert out[0].dvf_version.get_zip_file_name() == "valeursfoncieres-2021-s2.txt.zip"
    assert out[-1].dvf_version.get_zip_file_name() == "valeursfoncieres-2026-s1.txt.zip"
    assert [dvf.dvf_version.year for dvf in out] == [
        "2021",
        "2022",
        "2023",
        "2024",
        "2025",
        "2026",
    ]


def test_sorts_resources_by_year_then_semester():
    # Shuffled input, still a valid october window after sorting
    resources = [
        _semester(2026, "s1"),
        _full_year(2023),
        _full_year(2022),
        _semester(2021, "s2"),
        _full_year(2024),
        _full_year(2025),
    ]
    out = get_dvf_resources(resources, OctoberDelivery())
    assert [dvf.dvf_version.get_zip_file_name() for dvf in out] == [
        "valeursfoncieres-2021-s2.txt.zip",
        "valeursfoncieres-2022.txt.zip",
        "valeursfoncieres-2023.txt.zip",
        "valeursfoncieres-2024.txt.zip",
        "valeursfoncieres-2025.txt.zip",
        "valeursfoncieres-2026-s1.txt.zip",
    ]


def test_ignores_non_main_resources():
    resources = [_full_year(y) for y in [2021, 2022, 2023, 2024, 2025]]
    resources.append(_Resource("https://example.com/other.csv", type="documentation"))
    out = get_dvf_resources(resources, AprilDelivery())
    assert len(out) == 5
    assert all(dvf.dvf_version.get_zip_file_name() != "other.csv" for dvf in out)


def test_rejects_unexpected_file_names():
    bad = _Resource("https://example.com/valeursfoncieres-2021.txt", type="main")
    resources = [_full_year(y) for y in [2021, 2022, 2023, 2024, 2025]]
    resources[0] = bad
    with pytest.raises(ValueError, match="Unexpected file name"):
        get_dvf_resources(resources, AprilDelivery())


def test_propagates_window_mismatch():
    # A window that fails the delivery check is rejected here too
    resources = [
        _semester(2021, "s2"),
        _full_year(2022),
        _semester(2023, "s1"),  # a semester in a middle year
        _full_year(2024),
        _full_year(2025),
        _semester(2026, "s1"),
    ]
    with pytest.raises(ValueError):
        get_dvf_resources(resources, OctoberDelivery())
