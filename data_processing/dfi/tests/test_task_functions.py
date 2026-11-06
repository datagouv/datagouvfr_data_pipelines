import importlib
import os
import shutil
import sys
import tempfile

import duckdb
import pytest

DFI_DIR = os.path.normpath(os.path.join(os.path.dirname(__file__), ".."))
CONFIG_JSON = os.path.join(DFI_DIR, "config.json")

METRO_1_19 = {f"{n:02d}" for n in range(1, 20)}
METRO_21_95 = {f"{n:02d}" for n in range(21, 96)}
CORSICA = {"2A", "2B"}
DOM = {"971", "972", "973", "974", "976"}


def _import_task_functions(monkeypatch):
    """Import the real task_functions module. It reads config.json at import
    time from AIRFLOW_DAG_HOME, so point it at a temp dir holding a copy."""
    tmp = tempfile.mkdtemp()
    dest = os.path.join(tmp, "datagouvfr_data_pipelines", "data_processing", "dfi")
    os.makedirs(dest, exist_ok=True)
    shutil.copy(CONFIG_JSON, os.path.join(dest, "config.json"))

    import datagouvfr_data_pipelines.config as cfg

    monkeypatch.setattr(cfg, "AIRFLOW_DAG_HOME", tmp + "/")

    module_name = "datagouvfr_data_pipelines.data_processing.dfi.task_functions"
    if module_name in sys.modules:
        del sys.modules[module_name]
    return importlib.import_module(module_name)


def _insert(con, departments):
    con.execute("CREATE TABLE dfi (code_departement VARCHAR)")
    for dep in departments:
        con.execute("INSERT INTO dfi VALUES (?)", [dep])


@pytest.fixture()
def dfi_module(monkeypatch):
    return _import_task_functions(monkeypatch)


def test_expected_departments_are_the_french_df_codes(dfi_module):
    expected = METRO_1_19 | METRO_21_95 | CORSICA | DOM
    assert len(expected) == 96 + 5
    assert "20" not in expected  # Corse (20) is split into 2A/2B
    assert expected == dfi_module.EXPECTED_DEPARTMENTS


def test_coverage_ok_when_all_departments_present(dfi_module):
    con = duckdb.connect()
    _insert(con, dfi_module.EXPECTED_DEPARTMENTS)
    dfi_module.check_department_coverage(con)  # should not raise


def test_coverage_raises_when_departments_missing(dfi_module):
    con = duckdb.connect()
    _insert(con, dfi_module.EXPECTED_DEPARTMENTS - {"92", "93", "94", "95", "976"})
    with pytest.raises(RuntimeError) as exc:
        dfi_module.check_department_coverage(con)
    assert "92" in str(exc.value)
    assert "976" in str(exc.value)


def test_coverage_raises_when_empty(dfi_module):
    con = duckdb.connect()
    _insert(con, [])
    with pytest.raises(RuntimeError):
        dfi_module.check_department_coverage(con)


class _FakeHeadResponse:
    def __init__(self, status_code):
        self.status_code = status_code


def _patch_head(monkeypatch, dfi_module, status_by_url):
    def fake_head(url, *args, **kwargs):
        status = status_by_url.get(url)
        if status is None:
            raise dfi_module.requests.RequestException("unreachable")
        return _FakeHeadResponse(status)

    monkeypatch.setattr(dfi_module.requests, "head", fake_head)


def test_extract_period_from_url(dfi_module):
    res = {
        "title": "some title",
        "url": "https://x/attachments/situation_janvier_2025_zip",
    }
    assert dfi_module.extract_period(res) == (2025, "janvier")


def test_extract_period_matches_real_prod_url(dfi_module):
    res = {
        "url": "https://data.economie.gouv.fr/api/v2/catalog/datasets/"
        "documents-de-filiation-informatises-dfi-des-parcelles/attachments/"
        "documents_de_filiation_informatises_situation_octobre_2026_dept2a_a_dept49zip"
    }
    assert dfi_module.extract_period(res) == (2026, "octobre")


def test_extract_period_handles_ignores_typo_year(dfi_module):
    # "juillet 20266" typo: keep the first four digits of the year.
    res = {"url": "https://x/..._situation_juillet_20266_dept_50_a_dept_976zip"}
    assert dfi_module.extract_period(res) == (2026, "juillet")


def test_extract_period_raises_when_unknown(dfi_module):
    res = {
        "id": "r1",
        "url": "https://x/attachments/documents_de_filiation_informatises_zip",
    }
    with pytest.raises(RuntimeError) as exc:
        dfi_module.extract_period(res)
    assert "r1" in str(exc.value)


def test_assert_same_period_passes_for_same_release(dfi_module):
    shard1 = {
        "title": "DFI (situation octobre 2026)",
        "url": "https://x/a_situation_octobre_2026_zip",
        "id": "a",
    }
    shard2 = {
        "title": "DFI (situation octobre 2026)",
        "url": "https://x/b_situation_octobre_2026_zip",
        "id": "b",
    }
    dfi_module.assert_same_period([shard1, shard2])  # should not raise


def test_assert_same_period_raises_on_different_releases(dfi_module):
    july = {
        "title": "DFI (situation juillet 2026)",
        "url": "https://x/a_situation_juillet_2026_zip",
        "id": "a",
    }
    october = {
        "title": "DFI (situation octobre 2026)",
        "url": "https://x/b_situation_octobre_2026_zip",
        "id": "b",
    }
    with pytest.raises(RuntimeError) as exc:
        dfi_module.assert_same_period([july, october])
    assert "(2026, 'juillet')" in str(exc.value)
    assert "(2026, 'octobre')" in str(exc.value)


def test_assert_resources_available_passes_when_all_200(dfi_module, monkeypatch):
    a = {"title": "x", "url": "https://x/a", "id": "a"}
    b = {"title": "y", "url": "https://x/b", "id": "b"}
    _patch_head(monkeypatch, dfi_module, {a["url"]: 200, b["url"]: 200})
    dfi_module.assert_resources_available([a, b])  # should not raise


def test_assert_resources_available_raises_on_404(dfi_module, monkeypatch):
    a = {"title": "x", "url": "https://x/a", "id": "a"}
    broken = {"title": "y", "url": "https://x/broken", "id": "broken"}
    _patch_head(monkeypatch, dfi_module, {a["url"]: 200, broken["url"]: 404})
    with pytest.raises(RuntimeError) as exc:
        dfi_module.assert_resources_available([a, broken])
    assert "broken" in str(exc.value)


def test_assert_resources_available_raises_on_request_error(dfi_module, monkeypatch):
    a = {"title": "x", "url": "https://x/a", "id": "a"}
    down = {"title": "z", "url": "https://x/down", "id": "down"}
    _patch_head(monkeypatch, dfi_module, {a["url"]: 200})
    with pytest.raises(RuntimeError) as exc:
        dfi_module.assert_resources_available([a, down])
    assert "down" in str(exc.value)
