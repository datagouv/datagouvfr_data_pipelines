import pytest
from datagouvfr_data_pipelines.dgv.tabular_metrics.task_functions import extract_infos


def test_extract_infos_standard_name():
    assert extract_infos("haproxy-logs-27082025-slb-05.tar.gz") == ("20250827", "05")


def test_extract_infos_single_digit_slb_raises():
    # slb must be exactly two digits
    with pytest.raises(ValueError, match="haproxy-logs-27082025-slb-4.tar.gz"):
        extract_infos("haproxy-logs-27082025-slb-4.tar.gz")


def test_extract_infos_extra_dash_segments():
    assert extract_infos("prod-haproxy-logs-27082025-slb-05.tar.gz") == (
        "20250827",
        "05",
    )


def test_extract_infos_trailing_annotation_raises():
    # a trailing annotation like -old must not be silently accepted
    with pytest.raises(ValueError, match="haproxy-logs-18112026-slb-06-old.tar.gz"):
        extract_infos("haproxy-logs-18112026-slb-06-old.tar.gz")


def test_extract_infos_unexpected_name_raises():
    # a non compliant filename must fail loudly, naming the culprit file
    with pytest.raises(ValueError, match="unexpected-file.tar.gz"):
        extract_infos("unexpected-file.tar.gz")
