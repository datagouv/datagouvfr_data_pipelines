"""Unit tests for the DVF delivery definitions (utils/delivery)."""

from datetime import date

import pytest
from datagouvfr_data_pipelines.data_processing.dvf.geoloc.utils.delivery import (
    DELIVERIES,
    DeliveryInfo,
    get_delivery_from_today,
    get_delivery_info,
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
