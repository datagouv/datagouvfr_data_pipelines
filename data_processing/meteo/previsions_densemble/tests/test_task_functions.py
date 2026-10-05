"""Tests for the retention logic in `remove_old_occurrences`.

Retention is applied *per run*: Météo-France computes all the échéances of a
run (00:00..48:00) together for a (pack, grid, date); they share the same date
and are kept or deleted as a whole. A run is deleted when it is older than
``TIME_DEPTH_TO_KEEP`` relative to the newest run of that grid.

Covers datagouv/datagouvfr_data_pipelines#754:

* SFTP: the cleanup compared the full filename datetime string
  (``%Y%m%d%H%M``, 12 chars) against a date-only string (``%Y%m%d``, 8 chars)
  using lexicographic ``<``, so files sharing the same *date* as the threshold
  but with an earlier *time* were kept forever.
* S3: the threshold was anchored to the *oldest* published run (``min``), which
  sits at the floor of the available dates and therefore pruned nothing; it must
  be anchored to the *newest* run (``max``).
"""

from datetime import datetime
from unittest.mock import MagicMock, patch

import task_functions
from task_functions import TIME_DEPTH_TO_KEEP

# Datetime used as the newest currently-published run (anchor of the threshold).
_NEWEST_PUBLISHED = "202409231200"
# Computing the retention threshold exactly as the DAG does:
_THRESHOLD = datetime.strptime(_NEWEST_PUBLISHED, "%Y%m%d%H%M") - TIME_DEPTH_TO_KEEP

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
                "id1": {"date": _NEWEST_PUBLISHED, "resource_id": "r1"},
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


# --- S3 retention (per run) ---

S3_RUN_FOLDERS = [
    "data/arome/ncaled0025/202409231200/",  # newest run (2024-09-23 12:00) -> kept
    "data/arome/ncaled0025/202409221800/",  # recent but not latest, after threshold -> kept
    "data/arome/ncaled0025/202409220000/",  # stale run, before threshold -> deleted
    "data/arome/ncaled0025/202409210000/",  # very stale run -> deleted
]

# Two published resources with different run dates: the 00:00 échéance points
# to the newest run, the 48:00 échéance still points to an older run. This is
# exactly when min != max and the (buggy) min-anchored threshold pruned nothing.
S3_RESOURCES = {
    "arome_ncaled0025_00:00": {"date": "202409231200", "resource_id": "r1"},
    "arome_ncaled0025_48:00": {"date": "202409220000", "resource_id": "r2"},
}

S3_FILES_BY_FOLDER = {
    "data/arome/ncaled0025/202409231200/": [
        "data/arome/ncaled0025/202409231200/arome_ncaled0025_202409231200_00:00.grib",
    ],
    "data/arome/ncaled0025/202409221800/": [
        "data/arome/ncaled0025/202409221800/arome_ncaled0025_202409221800_00:00.grib",
    ],
    "data/arome/ncaled0025/202409220000/": [
        "data/arome/ncaled0025/202409220000/arome_ncaled0025_202409220000_48:00.grib",
    ],
    "data/arome/ncaled0025/202409210000/": [
        "data/arome/ncaled0025/202409210000/arome_ncaled0025_202409210000_48:00.grib",
    ],
}


def _run_s3_prune():
    """Run remove_old_occurrences with no SFTP files and return the set of S3
    files that were deleted."""
    s3_client = MagicMock()
    s3_client.get_folders_from_prefix.return_value = list(S3_RUN_FOLDERS)
    s3_client.get_files_from_prefix.side_effect = (
        lambda prefix, ignore_airflow_env=True: S3_FILES_BY_FOLDER.get(prefix, [])
    )

    sftp_client = MagicMock()
    sftp_client.list_files_in_directory.return_value = []

    with (
        patch.object(
            task_functions,
            "get_current_resources",
            return_value=dict(S3_RESOURCES),
        ),
        patch.object(task_functions, "S3Client", return_value=s3_client),
        patch.object(task_functions, "create_client", return_value=sftp_client),
    ):
        task_functions.remove_old_occurrences(pack="arome", grid="ncaled0025")

    return {call.args[0] for call in s3_client.delete_file.call_args_list}


def test_s3_prunes_runs_older_than_retention():
    """Regression test: anchored to the *oldest* run (``min``), stale runs were
    never pruned; anchored to the *newest* run (``max``), the whole runs older
    than the retention window must each be deleted as a unit."""
    deleted = _run_s3_prune()
    assert (
        "data/arome/ncaled0025/202409220000/arome_ncaled0025_202409220000_48:00.grib"
        in deleted
    )
    assert (
        "data/arome/ncaled0025/202409210000/arome_ncaled0025_202409210000_48:00.grib"
        in deleted
    )


def test_s3_keeps_recent_runs():
    deleted = _run_s3_prune()
    # The newest run, and a recent-but-not-latest run that is still after the
    # retention threshold (20240922 12:00), must both be kept.
    for folder in (
        "data/arome/ncaled0025/202409231200/",  # newest run
        "data/arome/ncaled0025/202409221800/",  # recent, after threshold -> kept
    ):
        assert all(f not in deleted for f in S3_FILES_BY_FOLDER[folder])
