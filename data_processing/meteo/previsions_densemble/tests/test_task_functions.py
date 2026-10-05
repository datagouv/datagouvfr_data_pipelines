"""Tests for the SFTP retention logic in `remove_old_occurrences`.

Reproduces datagouv/datagouvfr_data_pipelines#754: files on the SFTP older
than the retention threshold were not pruned because the cleanup compared the
full filename datetime string (``%Y%m%d%H%M``, 12 chars) against a date-only
string (``%Y%m%d``, 8 chars) using lexicographic ``<``. As a result, files
sharing the same *date* as the threshold but with an earlier *time* were kept
forever, accumulating storage.
"""

from datetime import datetime

from unittest.mock import MagicMock, patch

import task_functions
from task_functions import TIME_DEPTH_TO_KEEP

# Datetime used as the oldest currently-published occurrence.
_OLDEST_PUBLISHED = "202409231200"
# Computing the retention threshold exactly as the DAG does:
_THRESHOLD = datetime.strptime(_OLDEST_PUBLISHED, "%Y%m%d%H%M") - TIME_DEPTH_TO_KEEP

SFTP_FILENAMES = [
    "arome_pecaledonie_202409210600_mb0_ncaled0025_00:00.grib",
    "arome_pecaledonie_202409220300_mb0_ncaled0025_00:00.grib",
    "arome_pecaledonie_202409221200_mb0_ncaled0025_00:00.grib",
    "arome_pecaledonie_202409231200_mb0_ncaled0025_00:00.grib",
    "still_uploading_partial",  # non-grib file, must always be ignored
]

EXPECTED_DELETED = {
    "arome_pecaledonie_202409210600_mb0_ncaled0025_00:00.grib",  # 09-21, well before threshold
    "arome_pecaledonie_202409220300_mb0_ncaled0025_00:00.grib",  # 09-22 03:00 < threshold 12:00
}


def _run_remove_old_occurrences():
    """Run remove_old_occurrences with controlled S3/SFTP/API and return the
    set of SFTP files that were deleted."""
    sftp_client = MagicMock()
    sftp_client.list_files_in_directory.return_value = list(SFTP_FILENAMES)

    with (
        patch.object(
            task_functions,
            "get_current_resources",
            return_value={
                "id1": {"date": _OLDEST_PUBLISHED, "resource_id": "r1"},
            },
        ),
        patch.object(task_functions, "S3Client") as s3_client_cls,
        patch.object(task_functions, "create_client", return_value=sftp_client),
    ):
        s3_instance = s3_client_cls.return_value
        s3_instance.get_folders_from_prefix.return_value = []
        task_functions.remove_old_occurrences(pack="arome", grid="ncaled0025")

    deleted = {
        call.args[0].split("/")[-1] for call in sftp_client.delete_file.call_args_list
    }
    return deleted


def test_sftp_files_older_than_threshold_are_deleted():
    deleted = _run_remove_old_occurrences()
    # Files strictly older than the threshold datetime must be removed.
    assert EXPECTED_DELETED <= deleted


def test_sftp_files_same_date_but_earlier_than_threshold_are_deleted():
    """Regression test for #754.

    `_202409220300` shares the date (20240922) with the threshold but is
    earlier than the threshold datetime (20240922 12:00). The buggy date-only
    string comparison kept it; it must now be deleted.
    """
    deleted = _run_remove_old_occurrences()
    assert "arome_pecaledonie_202409220300_mb0_ncaled0025_00:00.grib" in deleted


def test_sftp_files_at_or_after_threshold_are_kept():
    deleted = _run_remove_old_occurrences()
    for file_name in (
        "arome_pecaledonie_202409221200_mb0_ncaled0025_00:00.grib",
        "arome_pecaledonie_202409231200_mb0_ncaled0025_00:00.grib",
    ):
        assert file_name not in deleted


def test_sftp_non_grib_files_are_ignored():
    deleted = _run_remove_old_occurrences()
    assert "still_uploading_partial" not in deleted
