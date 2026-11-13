import importlib
import sys

import pytest
from bs4 import BeautifulSoup


def _import_task_functions(monkeypatch):
    """Import the real task_functions module so the tests exercise the parsing
    code exactly as it runs in the DAG."""
    module_name = (
        "datagouvfr_data_pipelines.data_processing.assemblee_nationale.petitions"
        ".task_functions"
    )
    if module_name in sys.modules:
        del sys.modules[module_name]
    return importlib.import_module(module_name)


@pytest.fixture()
def tf(monkeypatch):
    return _import_task_functions(monkeypatch)


# Minimal but realistic HTML fragments mirroring the structure of
# https://petitions.assemblee-nationale.fr/initiatives/i-<id>
_OG_DESCRIPTION = "Une description représentative de la pétition."


def _page(
    status_class="",
    status_text="",
    votes="1 066 909",
    publish_date="10/06/2026",
    phase_date="10/11/2026",
    commission="Commission des affaires culturelles et de l'éducation",
    decision_text="",
    rejection="",
):
    status_span = (
        f'<span class="{status_class} label initiative-status">{status_text}</span>'
    )
    commission_link = (
        f'<a href="/initiatives?filter%5Barea_id%5D%5B%5D=2">{commission}</a>'
    )
    decision_p = f"<p>{decision_text}</p>" if decision_text else ""
    rejection_h5 = (
        f"<h5>Cette pétition a été rejetée {rejection}</h5>" if rejection else ""
    )
    archive_header = (
        '<div class="archive-header"></div>' if status_class == "archive-header" else ""
    )
    return f"""<html><head>
<meta property="og:description" content="{_OG_DESCRIPTION}"/>
</head><body>
<h2 class="heading2" id="">Titre de la pétition</h2>
<div class="publish-date">{publish_date}</div>
<span class="progress__bar__number">{votes}</span>
<span class="phase-date">{phase_date}</span>
{status_span}
{commission_link}
{decision_p}
{rejection_h5}
{archive_header}
</body></html>"""


def _soup(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "html.parser")


# ---------------------------------------------------------------------------
# standardize_date / remove_duplicate_blanks
# ---------------------------------------------------------------------------
def test_standardize_date(tf):
    assert tf.standardize_date("10/06/2026") == "2026-06-10"


def test_remove_duplicate_blanks(tf):
    assert tf.remove_duplicate_blanks("a\n  b\t c") == "a b c"


# ---------------------------------------------------------------------------
# build_status
# ---------------------------------------------------------------------------
def test_build_status_ouverte(tf):
    assert tf.build_status(_soup(_page())) == "ouverte"


def test_build_status_archivee(tf):
    html = _page(status_class="archive-header", status_text="Archivée")
    assert tf.build_status(_soup(html)) == "archivee"


def test_build_status_classee(tf):
    html = _page(status_class="alert", status_text="Classée par la commission")
    assert tf.build_status(_soup(html)) == "classee"


def test_build_status_expiree(tf):
    html = _page(status_class="warning", status_text="Expirée")
    assert tf.build_status(_soup(html)) == "expiree"


# ---------------------------------------------------------------------------
# get_labels
# ---------------------------------------------------------------------------
def test_get_labels_commission_only(tf):
    labels = tf.get_labels(_soup(_page(commission="Commission des affaires sociales")))
    assert labels["commission"] == "Commission des affaires sociales"
    assert labels["legislature"] is None


def test_get_labels_with_legislature(tf):
    html = (
        _page()
        + '<a href="/initiatives?filter%5Bfoo%5D%5B%5D=1">Législature 2022-2024</a>'
    )
    labels = tf.get_labels(_soup(html))
    assert (
        labels["commission"] == "Commission des affaires culturelles et de l'éducation"
    )
    assert labels["legislature"] == "2022-2024"


# ---------------------------------------------------------------------------
# get_votes / get_limit_date
# ---------------------------------------------------------------------------
def test_get_votes_with_spaces(tf):
    assert tf.get_votes(_soup(_page(votes="1 066 909"))) == 1066909


def test_get_votes_missing(tf):
    # no progress__bar__number element -> None (element absent)
    html = _page().replace('<span class="progress__bar__number">1 066 909</span>', "")
    assert tf.get_votes(_soup(html)) is None


def test_get_limit_date(tf):
    assert tf.get_limit_date(_soup(_page(phase_date="10/11/2026"))) == "2026-11-10"


def test_get_limit_date_missing(tf):
    # no phase-date element -> None (element absent)
    html = _page().replace('<span class="phase-date">10/11/2026</span>', "")
    assert tf.get_limit_date(_soup(html)) is None


# ---------------------------------------------------------------------------
# get_decision
# ---------------------------------------------------------------------------
def test_get_decision_article_148(tf):
    text = "En application de l’article 148 du Règlement de l’Assemblée nationale, classée."
    assert tf.get_decision(_soup(_page(decision_text=text))) == text.replace("\n", "")


def test_get_decision_rejetee(tf):
    assert tf.get_decision(_soup(_page(rejection="le 1 janvier 2026"))) == (
        "Cette pétition a été rejetée le 1 janvier 2026"
    )


def test_get_decision_none(tf):
    assert tf.get_decision(_soup(_page())) is None


# ---------------------------------------------------------------------------
# get_row
# ---------------------------------------------------------------------------
class _FakeResponse:
    def __init__(self, text="", headers=None):
        self.text = text
        self.headers = headers or {}


class _FakeSession:
    def __init__(self, page_html, unpublished=False):
        self._html = page_html
        self._unpublished = unpublished

    def head(self, url):
        loc = "https://petitions.assemblee-nationale.fr/" if self._unpublished else None
        return _FakeResponse(headers={"Location": loc} if self._unpublished else {})

    def get(self, url):
        return _FakeResponse(text=self._html)


def test_get_row_parses_fields(tf):
    session = _FakeSession(_page())
    row = tf.get_row(5820, session)
    assert row is not None
    assert row["identifiant"] == 5820
    assert row["titre"] == "Titre de la pétition"
    assert row["description"] == _OG_DESCRIPTION
    assert row["date_publication"] == "2026-06-10"
    assert row["nb_votes"] == 1066909
    assert row["statut"] == "ouverte"
    assert row["date_limite_vote"] == "2026-11-10"
    assert row["commission"] == "Commission des affaires culturelles et de l'éducation"
    assert row["url"] == "https://petitions.assemblee-nationale.fr/initiatives/i-5820"


def test_get_row_unpublished_returns_none(tf):
    # when the id redirects to the homepage, the petition is flagged as not
    # published and no row is produced
    session = _FakeSession(_page(), unpublished=True)
    assert tf.get_row(999999, session) is None


def test_gather_petitions_probes_from_zero_without_reusing_file(tf):
    """Regression test guarding the fix for #839.

    gather_petitions (Airflow @task) hard-skipped any id below the previous
    max that was absent from the previously produced CSV (the unused_ids
    mechanism). Because an id is assigned before its petition is published,
    that permanently dropped later-published petitions. The fix is to probe
    every id from 0 to the latest id found on the website and rely on get_row
    returning None for unpublished ids.

    We assert the fixed source no longer contains the old mechanism, which is
    easy to reintroduce by accident and impossible to exercise through the
    @task wrapper at unit level.
    """
    import inspect

    src = inspect.getsource(tf.gather_petitions)
    assert "unused_ids" not in src
    assert "read_csv" not in src
    assert "get_latest_petition_id" in src
    assert "range(0, max_id + 1)" in src
