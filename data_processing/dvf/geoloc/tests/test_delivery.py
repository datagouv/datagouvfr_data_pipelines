"""Unit tests for the DVF delivery definitions (utils/delivery)."""

from datetime import date
from typing import Literal

import pytest
from datagouvfr_data_pipelines.data_processing.dvf.geoloc.utils.delivery import (
    AprilDelivery,
    Delivery,
    OctoberDelivery,
)
from datagouvfr_data_pipelines.data_processing.dvf.geoloc.utils.dvf_version import (
    DvfVersion,
)


class TestFromKey:
    @pytest.mark.parametrize(
        "key, expected",
        [
            ("april", AprilDelivery),
            ("october", OctoberDelivery),
        ],
    )
    def test_explicit_key(self, key, expected):
        assert isinstance(Delivery.from_key(key), expected)

    @pytest.mark.parametrize("key", ["sometime", "", "2026", None])
    def test_unknown_key_raises(self, key):
        with pytest.raises(ValueError, match="Unknown delivery"):
            Delivery.from_key(key)


class TestFromDate:
    @pytest.mark.parametrize(
        "month, expected",
        [
            (1, OctoberDelivery),
            (3, OctoberDelivery),
            (4, AprilDelivery),
            (6, AprilDelivery),
            (9, AprilDelivery),
            (10, OctoberDelivery),
            (12, OctoberDelivery),
        ],
    )
    def test_infers_from_month(self, month, expected):
        assert isinstance(Delivery.from_date(date(2026, month, 15)), expected)


class TestSubclassAttributes:
    @pytest.mark.parametrize(
        "delivery, key, month, count, cadastre_millesime",
        [
            (AprilDelivery(), "april", 4, 5, "03"),
            (OctoberDelivery(), "october", 10, 6, "06"),
        ],
    )
    def test_attributes(self, delivery, key, month, count, cadastre_millesime):
        assert delivery.key == key
        assert delivery.month == month
        assert delivery.count == count
        assert delivery.cadastre_millesime == cadastre_millesime


class TestRegistry:
    def test_registry_contains_both_deliveries(self):
        assert set(Delivery._registry) == {"april", "october"}
        assert Delivery._registry["april"] is AprilDelivery
        assert Delivery._registry["october"] is OctoberDelivery


def _full(year: int) -> DvfVersion:
    return DvfVersion(str(year), None)


def _semi(year: int, semester: Literal["s1", "s2"]) -> DvfVersion:
    return DvfVersion(str(year), semester)


class TestAprilCheckDvfVersions:
    def test_valid(self):
        versions = [_full(y) for y in range(2018, 2023)]
        assert AprilDelivery().check_dvf_versions(versions) is None

    @pytest.mark.parametrize(
        "versions",
        [
            [_full(y) for y in range(2018, 2023)],
            [_full(y) for y in range(1999, 2004)],
        ],
    )
    def test_any_consecutive_years_valid(self, versions):
        assert AprilDelivery().check_dvf_versions(versions) is None

    @pytest.mark.parametrize(
        "versions",
        [
            [],  # empty
            [_full(y) for y in range(2018, 2020)],  # only 2
            [_full(y) for y in range(2018, 2024)],  # 6
        ],
    )
    def test_wrong_count_raises(self, versions):
        with pytest.raises(ValueError, match="5 full years"):
            AprilDelivery().check_dvf_versions(versions)

    def test_with_semester_raises(self):
        versions = [*[_full(y) for y in range(2018, 2022)], _semi(2022, "s1")]
        with pytest.raises(ValueError, match="only contain full years"):
            AprilDelivery().check_dvf_versions(versions)

    def test_non_consecutive_raises(self):
        versions = [_full(2018), _full(2019), _full(2021), _full(2022), _full(2023)]
        with pytest.raises(ValueError, match="not consecutive"):
            AprilDelivery().check_dvf_versions(versions)


class TestOctoberCheckDvfVersions:
    def test_valid(self):
        versions = [
            _semi(2017, "s2"),
            *(_full(y) for y in range(2018, 2022)),
            _semi(2022, "s1"),
        ]
        assert OctoberDelivery().check_dvf_versions(versions) is None

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
    def test_wrong_count_raises(self, versions):
        with pytest.raises(ValueError, match="6 files"):
            OctoberDelivery().check_dvf_versions(versions)

    def test_no_semesters_raises(self):
        versions = [_full(y) for y in range(2017, 2023)]  # 6 full years, no semesters
        with pytest.raises(ValueError, match="second semester"):
            OctoberDelivery().check_dvf_versions(versions)

    def test_oldest_not_s2_raises(self):
        versions = [
            _semi(2017, "s1"),
            *(_full(y) for y in range(2018, 2022)),
            _semi(2022, "s1"),
        ]
        with pytest.raises(ValueError, match="second semester"):
            OctoberDelivery().check_dvf_versions(versions)

    def test_newest_not_s1_raises(self):
        versions = [
            _semi(2017, "s2"),
            *(_full(y) for y in range(2018, 2022)),
            _semi(2022, "s2"),
        ]
        with pytest.raises(ValueError, match="first semester"):
            OctoberDelivery().check_dvf_versions(versions)

    def test_middle_full_year_with_semester_raises(self):
        versions = [
            _semi(2017, "s2"),
            _full(2018),
            _semi(2019, "s1"),
            _full(2020),
            _full(2021),
            _semi(2022, "s1"),
        ]
        with pytest.raises(ValueError, match="full years cannot hold a semester"):
            OctoberDelivery().check_dvf_versions(versions)

    def test_non_consecutive_raises(self):
        versions = [
            _semi(2017, "s2"),
            *(_full(y) for y in range(2018, 2021)),
            _full(2023),
            _semi(2024, "s1"),
        ]
        with pytest.raises(ValueError, match="not consecutive"):
            OctoberDelivery().check_dvf_versions(versions)
