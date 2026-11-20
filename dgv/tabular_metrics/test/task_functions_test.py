import pytest
from datagouvfr_data_pipelines.dgv.tabular_metrics.task_functions import (
    extract_infos,
    process_line,
)


def _sample_line(status: int | str) -> str:
    return (
        "2025-08-01T00:01:19.614400+02:00 slb-04 haproxy[345597]: 127.0.0.1:2040 "
        "[01/Aug/2025:00:01:19.373] WILDCARD~ WWW-TABULAR-API-DATAGOUVFR/"
        "www-tabular-api2 0/0/5/235/+240 "
        f"{status} +4896 - - ---- 707/57/4/2/0 0/0 "
        '"GET /api/resources/b363e051-9649-4879-ae78-71ef227d0cc5/data/'
        '?page=5&page_size=20 HTTP/1.1"'
    )


def test_process_line_parses_status():
    result = process_line(_sample_line(200))
    assert result["status"] == 200


def test_process_line_out_of_range_status_is_none():
    result = process_line(_sample_line(600))
    assert result["status"] is None


def test_process_line_invalid_status_is_none():
    result = process_line(_sample_line("not-a-number"))
    assert result["status"] is None


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
