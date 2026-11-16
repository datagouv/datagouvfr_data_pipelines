import importlib
import sys

import pytest
from bs4 import BeautifulSoup


def _import_task_functions(monkeypatch):
    """Import the real task_functions module so the tests exercise the parsing
    code exactly as it runs in the DAG."""
    module_name = (
        "datagouvfr_data_pipelines.data_processing.senat.petitions.task_functions"
    )
    if module_name in sys.modules:
        del sys.modules[module_name]
    return importlib.import_module(module_name)


@pytest.fixture()
def tf(monkeypatch):
    return _import_task_functions(monkeypatch)


# Minimal but realistic HTML fragments mirroring the structure of
# https://petitions.senat.fr/initiatives/i-<id>
_OG_DESCRIPTION = "Une description représentative de la pétition du Sénat."


def _page(
    status_class="initiative-status",
    status_text="Seuil non atteint",
    votes="147",
    publish_date="30/01/2020",
    phase_date="30/01/2020 - 30/07/2020",
    type_text="Texte législatif",
):
    status_span = f'<span class="{status_class}">{status_text}</span>'
    return f"""<html><head>
<meta property="og:description" content="{_OG_DESCRIPTION}"/>
</head><body>
<h1 class="heading2" id="">Titre de la pétition</h1>
<h2 class="heading-small">{type_text}</h2>
<div class="publish-date">{publish_date}</div>
<span class="progress__bar__number">{votes}</span>
<span class="phase-date">{phase_date}</span>
{status_span}
</body></html>"""


def _soup(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "html.parser")


# ---------------------------------------------------------------------------
# standardize_date / remove_duplicate_blanks
# ---------------------------------------------------------------------------
def test_standardize_date(tf):
    assert tf.standardize_date("30/07/2020") == "2020-07-30"


def test_remove_duplicate_blanks(tf):
    assert tf.remove_duplicate_blanks("a\n  b\t c") == "a b c"


# ---------------------------------------------------------------------------
# build_status
# ---------------------------------------------------------------------------
def test_build_status_text(tf):
    assert tf.build_status(_soup(_page(status_text="Seuil non atteint"))) == (
        "Seuil non atteint"
    )


def test_build_status_default_when_absent(tf):
    html = _page().replace(
        '<span class="initiative-status">Seuil non atteint</span>', ""
    )
    assert tf.build_status(_soup(html)) == "Ouverte"


# ---------------------------------------------------------------------------
# get_votes / get_limit_date
# ---------------------------------------------------------------------------
def test_get_votes(tf):
    assert tf.get_votes(_soup(_page(votes="1 234"))) == 1234


def test_get_votes_missing(tf):
    html = _page().replace('<span class="progress__bar__number">147</span>', "")
    assert tf.get_votes(_soup(html)) is None


def test_get_limit_date_takes_period_end(tf):
    # senat displays "start - end"; the parser keeps the end date
    assert tf.get_limit_date(_soup(_page(phase_date="30/01/2020 - 30/07/2020"))) == (
        "2020-07-30"
    )


def test_get_limit_date_missing(tf):
    html = _page().replace(
        '<span class="phase-date">30/01/2020 - 30/07/2020</span>', ""
    )
    assert tf.get_limit_date(_soup(html)) is None


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
        loc = "https://petitions.senat.fr/" if self._unpublished else None
        return _FakeResponse(headers={"Location": loc} if self._unpublished else {})

    def get(self, url):
        return _FakeResponse(text=self._html)


def test_get_row_parses_fields(tf):
    session = _FakeSession(_page())
    row = tf.get_row(132, session)
    assert row is not None
    assert row["identifiant"] == 132
    assert row["titre"] == "Titre de la pétition"
    assert row["description"] == _OG_DESCRIPTION
    assert row["date_publication"] == "2020-01-30"
    assert row["nb_votes"] == 147
    assert row["statut"] == "Seuil non atteint"
    assert row["date_limite_vote"] == "2020-07-30"
    assert row["type"] == "Texte législatif"
    assert row["url"] == "https://petitions.senat.fr/initiatives/i-132"


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
