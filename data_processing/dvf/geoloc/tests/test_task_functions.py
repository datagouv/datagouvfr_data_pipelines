"""Unit tests for publish_yearly / publish_concat / download_published_yearly_files
and the ``only`` gating helpers (task_functions).

The publish steps turn the built ``full-*.csv.gz`` files into resources: they parse
each file name into a DvfVersion, pick the right title (including the semester),
create or update the matching resource, and delete the resources that fell out of the
rolling window. The ``only`` helpers decide, from the run ``params``, whether a task is
gated (skipped/run) for ``only-concatenated`` / ``only-yearly`` runs.
"""

from datetime import date

import pytest
from datagouvfr_data_pipelines.data_processing.dvf.geoloc import task_functions

OCTOBER_WINDOW = [
    "full-2021-s2.csv.gz",
    "full-2022.csv.gz",
    "full-2023.csv.gz",
    "full-2024.csv.gz",
    "full-2025.csv.gz",
    "full-2026-s1.csv.gz",
]


class _Resource:
    def __init__(self, url):
        self.url = url
        self.type = "main"
        self.updates = []
        self.id = f"resource-{url}"

    def update(self, **kwargs):
        self.updates.append(kwargs)

    def delete(self):
        self.deleted = True


class _Dataset:
    def __init__(self, resources):
        self.resources = resources
        self.created = []

    def create_static(self, **kwargs):
        self.created.append(kwargs)


class _FakeClient:
    def __init__(self, dataset):
        self._dataset = dataset

    def dataset(self, _id):
        return self._dataset


class _FakeDate(date):
    """datetime.date whose today() is pinned, to make the delivery deterministic."""

    @classmethod
    def today(cls):
        return date(2026, 10, 15)


@pytest.fixture
def setup(monkeypatch):
    monkeypatch.setattr(task_functions.os, "listdir", lambda _: OCTOBER_WINDOW)
    # keep the delivery deterministic (an october window) regardless of the run date
    monkeypatch.setattr(task_functions, "date", _FakeDate)
    dataset = _Dataset(
        [
            _Resource("https://example.org/full-2023.csv.gz"),  # in window: updated
            _Resource("https://example.org/full-2019.csv.gz"),  # out: deleted
        ]
    )
    monkeypatch.setattr(task_functions, "publish_client", _FakeClient(dataset))
    return dataset


# --- publish_yearly (the yearly part) ---


def test_parses_each_file_and_uses_semester_titles(setup):
    task_functions.publish_yearly(params={})

    # the per-year resources created for each file (excluding the single FULL_FILE)
    created = {
        c["file_to_upload"].rsplit("/", 1)[-1]: c
        for c in setup.created
        if c["file_to_upload"].rsplit("/", 1)[-1].startswith("full-")
    }
    assert set(created) == set(OCTOBER_WINDOW) - {"full-2023.csv.gz"}
    assert (
        created["full-2021-s2.csv.gz"]["payload"]["title"] == "DVF 2021 (2nd semestre)"
    )
    assert (
        created["full-2026-s1.csv.gz"]["payload"]["title"] == "DVF 2026 (1er semestre)"
    )
    assert created["full-2025.csv.gz"]["payload"]["title"] == "DVF 2025"


def test_updates_existing_year_and_deletes_out_of_window(setup):
    task_functions.publish_yearly(params={})

    in_2023 = setup.resources[0]
    assert in_2023.updates == [
        {
            "payload": {"title": "DVF 2023"},
            "file_to_upload": task_functions.TMP_FOLDER + "full-2023.csv.gz",
        }
    ]
    assert setup.resources[1].deleted is True


def test_validates_window_with_the_matched_delivery(setup):
    # the october window must be accepted by the built-in delivery check
    task_functions.publish_yearly(params={})
    assert setup.created


# --- publish_concat (the concat part) ---


def test_publish_concat_derives_years_and_publishes_full_file(setup, monkeypatch):
    published = {}
    monkeypatch.setattr(
        task_functions,
        "publish_full_file",
        lambda dataset, years: published.update(years=years),
    )
    task_functions.publish_concat(params={})
    # october window: 5 distinct years (2021 involved through its s2 semester)
    assert published["years"] == ["2021", "2022", "2023", "2024", "2025", "2026"]


def test_publish_concat_skips_when_year_to_run_is_set(setup, monkeypatch):
    called = []
    monkeypatch.setattr(
        task_functions, "publish_full_file", lambda dataset, years: called.append(years)
    )
    task_functions.publish_concat(params={"year_to_run": 2023})
    assert called == []


# --- download_published_yearly_files (concat-only source) ---


def test_download_published_yearly_files_only_main_matching_files(monkeypatch):
    resources = [
        _Resource(
            "https://example.org/full-2023.csv.gz"
        ),  # main + yearly -> downloaded
        _Resource(
            "https://example.org/full-2022.csv.gz"
        ),  # main + yearly -> downloaded
        _Resource("https://example.org/dvf.csv.gz"),  # main but not a yearly file
    ]
    # a non-main resource must be ignored even if its name matches
    resources[0].type = "attached"
    monkeypatch.setattr(
        task_functions, "publish_client", _FakeClient(_Dataset(resources))
    )
    downloaded = []
    monkeypatch.setattr(
        task_functions,
        "download_resource",
        lambda res, dest_path: downloaded.append(dest_path.rsplit("/", 1)[-1]),
    )
    task_functions.download_published_yearly_files()
    assert downloaded == ["full-2022.csv.gz"]


# --- `only` gating helpers ---


@pytest.mark.parametrize(
    "only,concat_expected,yearly_expected",
    [
        ("none", False, False),
        ("only-concatenated", True, False),
        ("only-yearly", False, True),
        (None, False, False),
    ],
)
def test_only_gating_helpers(only, concat_expected, yearly_expected):
    context = {"params": {"only": only}}
    assert task_functions._only_concatenated(context) is concat_expected, (
        f"only={only!r} _only_concatenated"
    )
    assert task_functions._only_yearly(context) is yearly_expected, (
        f"only={only!r} _only_yearly"
    )
