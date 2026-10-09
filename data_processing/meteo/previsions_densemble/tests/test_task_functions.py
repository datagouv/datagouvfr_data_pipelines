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

import logging
from datetime import datetime
from unittest.mock import MagicMock, patch

import task_functions
from task_functions import TIME_DEPTH_TO_KEEP

# --- Shared run dates (format "%Y%m%d%H%M"), used to build both SFTP and S3 data ---

# Newest currently-published run: anchor of the retention threshold.
_NEWEST_RUN = "202409231200"
# Retention threshold exactly as the DAG computes it:
# newest run minus TIME_DEPTH_TO_KEEP (15 days) -> 2024-09-08 12:00.
_THRESHOLD = datetime.strptime(_NEWEST_RUN, "%Y%m%d%H%M") - TIME_DEPTH_TO_KEEP

# Well before the threshold -> must be deleted (used for both SFTP and S3).
_OLD_RUN = "202409020600"
# Same date as the threshold but earlier time -> deleted (regression for #754).
_AT_THRESHOLD_DATE_EARLIER = "202409080300"
# Exactly at the threshold -> kept.
_AT_THRESHOLD = "202409081200"


def _sftp_file(run_date: str, echeance: str = "00:00") -> str:
    return f"arome_pecaledonie_{run_date}_mb0_ncaled0025_{echeance}.grib"


def _s3_folder(run_date: str) -> str:
    return f"data/arome/ncaled0025/{run_date}/"


def _s3_file(run_date: str) -> str:
    return _s3_folder(run_date) + f"arome_ncaled0025_{run_date}_00:00.grib"


SFTP_FILENAMES = [
    _sftp_file(_OLD_RUN),
    _sftp_file(_AT_THRESHOLD_DATE_EARLIER),
    _sftp_file(_AT_THRESHOLD),
    _sftp_file(_NEWEST_RUN),
    "still_uploading_partial",  # non-grib file, must always be ignored
]

EXPECTED_DELETED = {
    _sftp_file(_OLD_RUN),
    _sftp_file(_AT_THRESHOLD_DATE_EARLIER),
}


def _run_remove_old_occurrences(dry_run: bool = False):
    """Run remove_old_occurrences with controlled S3/SFTP/API and return the
    set of SFTP files that were deleted."""
    sftp_client = MagicMock()
    sftp_client.list_files_in_directory.return_value = list(SFTP_FILENAMES)
    sftp_client.get_file_stats.return_value.st_size = 100

    with (
        patch.object(task_functions, "DRY_RUN_RETENTION", dry_run),
        patch.object(
            task_functions,
            "get_current_resources",
            return_value={
                "id1": {"date": _NEWEST_RUN, "resource_id": "r1"},
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

    ``_AT_THRESHOLD_DATE_EARLIER`` shares the date with the threshold but is
    earlier than the threshold datetime. The buggy date-only string comparison
    kept it; it must now be deleted.
    """
    deleted = _run_remove_old_occurrences()
    assert _sftp_file(_AT_THRESHOLD_DATE_EARLIER) in deleted


def test_sftp_files_at_or_after_threshold_are_kept():
    deleted = _run_remove_old_occurrences()
    for file_name in (
        _sftp_file(_AT_THRESHOLD),
        _sftp_file(_NEWEST_RUN),
    ):
        assert file_name not in deleted


def test_sftp_non_grib_files_are_ignored():
    deleted = _run_remove_old_occurrences()
    assert "still_uploading_partial" not in deleted


# --- S3 retention (per run) ---

S3_RUN_FOLDERS = [
    _s3_folder(_NEWEST_RUN),  # newest run -> kept
    _s3_folder(_AT_THRESHOLD),  # at threshold -> kept
    _s3_folder(
        _AT_THRESHOLD_DATE_EARLIER
    ),  # same date as threshold, earlier time -> deleted
    _s3_folder(_OLD_RUN),  # run well before the threshold -> deleted
]

# Two published resources with different run dates: the 00:00 échéance points
# to the newest run, the 48:00 échéance still points to an older run. This is
# exactly when min != max and the (buggy) min-anchored threshold pruned nothing.
S3_RESOURCES = {
    "arome_ncaled0025_00:00": {"date": _NEWEST_RUN, "resource_id": "r1"},
    "arome_ncaled0025_48:00": {"date": _OLD_RUN, "resource_id": "r2"},
}

S3_FILES_BY_FOLDER = {
    _s3_folder(_NEWEST_RUN): [_s3_file(_NEWEST_RUN)],
    _s3_folder(_AT_THRESHOLD): [_s3_file(_AT_THRESHOLD)],
    _s3_folder(_AT_THRESHOLD_DATE_EARLIER): [_s3_file(_AT_THRESHOLD_DATE_EARLIER)],
    _s3_folder(_OLD_RUN): [_s3_file(_OLD_RUN)],
}


def _run_s3_prune(dry_run: bool = False):
    """Run remove_old_occurrences with no SFTP files and return the set of S3
    files that were deleted."""
    s3_client = MagicMock()
    s3_client.get_folders_from_prefix.return_value = list(S3_RUN_FOLDERS)

    def _files_for(prefix, ignore_airflow_env=True, as_objects=False):
        keys = S3_FILES_BY_FOLDER.get(prefix, [])
        if as_objects:
            return [_s3_obj(key) for key in keys]
        return list(keys)

    s3_client.get_files_from_prefix.side_effect = _files_for

    sftp_client = MagicMock()
    sftp_client.list_files_in_directory.return_value = []

    with (
        patch.object(task_functions, "DRY_RUN_RETENTION", dry_run),
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


def _s3_obj(key: str) -> MagicMock:
    obj = MagicMock()
    obj.key = key
    obj.size = 100
    return obj


def test_s3_prunes_runs_older_than_retention():
    """Regression test: anchored to the *oldest* run (``min``), stale runs were
    never pruned; anchored to the *newest* run (``max``), the whole runs older
    than the retention window must each be deleted as a unit."""
    deleted = _run_s3_prune()
    assert _s3_file(_AT_THRESHOLD_DATE_EARLIER) in deleted
    assert _s3_file(_OLD_RUN) in deleted


def test_s3_keeps_recent_runs():
    deleted = _run_s3_prune()
    # The newest run, and a run sitting exactly at the retention threshold, must
    # both be kept.
    for folder in (
        _s3_folder(_NEWEST_RUN),  # newest run
        _s3_folder(_AT_THRESHOLD),  # at threshold -> kept
    ):
        assert all(f not in deleted for f in S3_FILES_BY_FOLDER[folder])


# --- real-deletion stats ---


def test_real_deletion_sftp_logs_correct_stats(caplog):
    """In real-deletion mode, the SFTP summary must state 'deleted' with the
    right count, total and freed size (2 files of 100 B -> 200 B)."""
    with caplog.at_level(logging.INFO):
        _run_remove_old_occurrences(dry_run=False)
    assert "deleted 2/4 SFTP file(s) (200.0 B)" in caplog.text
    first = _sftp_file(_OLD_RUN)
    last = _sftp_file(_AT_THRESHOLD_DATE_EARLIER)
    assert (
        f"(first: {first.split('/')[-1]}, last: {last.split('/')[-1]})" in caplog.text
    )


def test_real_deletion_s3_logs_correct_stats(caplog):
    """In real-deletion mode, the S3 summary must state 'deleted' with the right
    count, total and freed size (2 run folders of 100 B -> 200 B)."""
    with caplog.at_level(logging.INFO):
        _run_s3_prune(dry_run=False)
    assert "deleted 2/4 S3 run folder(s) (200.0 B)" in caplog.text
    first = _s3_folder(_AT_THRESHOLD_DATE_EARLIER)
    last = _s3_folder(_OLD_RUN)
    assert f"first: {first}" in caplog.text
    assert f"last: {last}" in caplog.text


# --- DRY_RUN_RETENTION mode ---


def test_dry_run_sftp_does_not_delete(caplog):
    """With DRY_RUN_RETENTION enabled, the SFTP cleanup must not call delete_file, and a
    concise DRY RUN summary must be logged (with 'would delete')."""
    with caplog.at_level(logging.INFO):
        deleted = _run_remove_old_occurrences(dry_run=True)
    assert deleted == set()
    assert "DRY RUN (retention): would delete" in caplog.text
    assert "SFTP file(s)" in caplog.text


def test_dry_run_s3_does_not_delete(caplog):
    """With DRY_RUN_RETENTION enabled, the S3 cleanup must not call delete_file, and a
    concise DRY RUN summary must be logged (with 'would delete')."""
    with caplog.at_level(logging.INFO):
        deleted = _run_s3_prune(dry_run=True)
    assert deleted == set()
    assert "DRY RUN (retention): would delete" in caplog.text
    assert "S3 run folder(s)" in caplog.text


def test_real_deletion_still_verified_when_dry_run_forced_off():
    """DRY_RUN_RETENTION defaults to True; the real-deletion tests force it to False so
    actual delete_file calls are still exercised."""
    deleted = _run_remove_old_occurrences(dry_run=False)
    assert EXPECTED_DELETED <= deleted


def test_sftp_size_failure_warns(caplog):
    """If a file size cannot be fetched from the SFTP, a warning must tell us
    the freed size is incomplete."""
    sftp_client = MagicMock()
    sftp_client.list_files_in_directory.return_value = list(SFTP_FILENAMES)
    sftp_client.get_file_stats.side_effect = IOError("stat failed")

    with (
        patch.object(task_functions, "DRY_RUN_RETENTION", True),
        patch.object(
            task_functions,
            "get_current_resources",
            return_value={
                "id1": {"date": _NEWEST_RUN, "resource_id": "r1"},
            },
        ),
        patch.object(task_functions, "S3Client") as s3_client_cls,
        patch.object(task_functions, "create_client", return_value=sftp_client),
    ):
        s3_instance = s3_client_cls.return_value
        s3_instance.get_folders_from_prefix.return_value = []
        with caplog.at_level(logging.INFO):
            task_functions.remove_old_occurrences(pack="arome", grid="ncaled0025")

    assert "Could not compute the size" in caplog.text
    assert "DRY RUN (retention): would delete" in caplog.text


# --- _human_size ---


def test_human_size_bytes():
    assert task_functions._human_size(0) == "0.0 B"
    assert task_functions._human_size(512) == "512.0 B"
    assert task_functions._human_size(1023) == "1023.0 B"


def test_human_size_scales_units():
    assert task_functions._human_size(1024) == "1.0 KiB"
    assert task_functions._human_size(1024 * 1024) == "1.0 MiB"
    assert task_functions._human_size(1024**3) == "1.0 GiB"
    assert task_functions._human_size(1024**4) == "1.0 TiB"


def test_human_size_rounds_to_one_decimal():
    assert task_functions._human_size(1536) == "1.5 KiB"
    assert task_functions._human_size(1024 * 1024 + 524288) == "1.5 MiB"


def test_human_size_does_not_overflow_past_tib():
    # Beyond TiB the result stays in TiB rather than crashing.
    assert task_functions._human_size(1024**5) == "1024.0 TiB"
