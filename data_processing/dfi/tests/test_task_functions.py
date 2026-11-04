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
