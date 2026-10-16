"""Unit tests for the DVF delivery definitions (utils/delivery)."""

from datetime import date
from typing import Literal

import pytest
from datagouvfr_data_pipelines.data_processing.dvf.geoloc.utils.delivery import (
    DELIVERIES,
    DeliveryInfo,
    check_delivery_versions,
    get_delivery_from_today,
    get_delivery_info,
)
from datagouvfr_data_pipelines.data_processing.dvf.geoloc.utils.dvf_version import (
    DvfVersion,
)


@pytest.mark.parametrize(
    "month, expected",
    [
        (1, "october"),
        (3, "october"),
        (4, "april"),
        (6, "april"),
        (9, "april"),
        (10, "october"),
        (12, "october"),
    ],
)
def test_get_delivery_from_today_boundaries(month, expected):
    assert get_delivery_from_today(date(2026, month, 15)) == expected


def test_april_delivery_info_fields():
    info = get_delivery_info("april", date(2026, 6, 1))
    assert info.delivery == "april"
    assert info.month == 4
    assert info.file_count == 5
    assert info.cadastre_millesime == "03"


def test_october_delivery_info_fields():
    info = get_delivery_info("october", date(2026, 6, 1))
    assert info.delivery == "october"
    assert info.month == 10
    assert info.file_count == 6
    assert info.cadastre_millesime == "06"


def test_deliveries_registry():
    assert set(DELIVERIES) == {"april", "october"}
    for info in DELIVERIES.values():
        assert isinstance(info, DeliveryInfo)


def test_get_delivery_info_none_infers_from_today():
    assert get_delivery_info(None, date(2026, 7, 1)).delivery == "april"
    assert get_delivery_info(None, date(2026, 11, 1)).delivery == "october"


@pytest.mark.parametrize("delivery", ["sometime", "", "2026"])
def test_get_delivery_info_unknown_raises(delivery):
    with pytest.raises(ValueError, match="Unknown delivery"):
        get_delivery_info(delivery, date(2026, 6, 1))


def _full(year: int) -> DvfVersion:
    return DvfVersion(str(year), None)


def _semi(year: int, semester: Literal["s1", "s2"]) -> DvfVersion:
    return DvfVersion(str(year), semester)


class TestCheckDeliveryVersions:
    def test_april_valid(self):
        APRIL_VALID = [_full(y) for y in range(2018, 2023)]
        assert check_delivery_versions("april", APRIL_VALID) is None

    @pytest.mark.parametrize(
        "versions",
        [
            [_full(y) for y in range(2018, 2023)],
            [_full(y) for y in range(1999, 2004)],
        ],
    )
    def test_april_any_consecutive_years_valid(self, versions):
        assert check_delivery_versions("april", versions) is None

    @pytest.mark.parametrize(
        "versions",
        [
            [],  # empty
            [_full(y) for y in range(2018, 2020)],  # only 2
            [_full(y) for y in range(2018, 2024)],  # 6
        ],
    )
    def test_april_wrong_count_raises(self, versions):
        with pytest.raises(ValueError, match="5 full years"):
            check_delivery_versions("april", versions)

    def test_april_with_semester_raises(self):
        versions = [*[_full(y) for y in range(2018, 2022)], _semi(2022, "s1")]
        with pytest.raises(ValueError, match="only contain full years"):
            check_delivery_versions("april", versions)

    def test_april_non_consecutive_raises(self):
        versions = [_full(2018), _full(2019), _full(2021), _full(2022), _full(2023)]
        with pytest.raises(ValueError, match="not consecutive"):
            check_delivery_versions("april", versions)

    def test_october_valid(self):
        OCTOBER_VALID = [
            _semi(2017, "s2"),
            *(_full(y) for y in range(2018, 2022)),
            _semi(2022, "s1"),
        ]
        assert check_delivery_versions("october", OCTOBER_VALID) is None

    @pytest.mark.parametrize(
        "versions",
        [
            [],
            [
                _semi(2017, "s2"),
                *(_full(y) for y in range(2018, 2021)),
                _semi(2021, "s1"),
            ],  # only 4
        ],
    )
    def test_october_wrong_count_raises(self, versions):
        with pytest.raises(ValueError, match="6 files"):
            check_delivery_versions("october", versions)

    def test_october_no_semesters_raises(self):
        versions = [_full(y) for y in range(2017, 2023)]  # 6 full years, no semesters
        with pytest.raises(ValueError, match="second semester"):
            check_delivery_versions("october", versions)

    def test_october_oldest_not_s2_raises(self):
        versions = [
            _semi(2017, "s1"),
            *(_full(y) for y in range(2018, 2022)),
            _semi(2022, "s1"),
        ]
        with pytest.raises(ValueError, match="second semester"):
            check_delivery_versions("october", versions)

    def test_october_newest_not_s1_raises(self):
        versions = [
            _semi(2017, "s2"),
            *(_full(y) for y in range(2018, 2022)),
            _semi(2022, "s2"),
        ]
        with pytest.raises(ValueError, match="first semester"):
            check_delivery_versions("october", versions)

    def test_october_middle_full_year_with_semester_raises(self):
        versions = [
            _semi(2017, "s2"),
            _full(2018),
            _semi(2019, "s1"),
            _full(2020),
            _full(2021),
            _semi(2022, "s1"),
        ]
        with pytest.raises(ValueError, match="full years cannot hold a semester"):
            check_delivery_versions("october", versions)

    def test_october_non_consecutive_raises(self):
        versions = [
            _semi(2017, "s2"),
            *(_full(y) for y in range(2018, 2021)),
            _full(2023),
            _semi(2024, "s1"),
        ]
        with pytest.raises(ValueError, match="not consecutive"):
            check_delivery_versions("october", versions)
