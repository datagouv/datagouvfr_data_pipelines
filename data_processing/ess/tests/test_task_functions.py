from io import BytesIO
from unittest.mock import MagicMock

import pandas as pd
import pytest
from datagouvfr_data_pipelines.data_processing.ess import task_functions
from datagouvfr_data_pipelines.data_processing.ess.task_functions import (
    COLUMNS,
    gather_data,
    get_files_updated_date,
    get_source_urls,
    normalize_departement_code,
    normalize_postal_code,
    read_source_file,
)

HTML = """
<a href="/system/files/2026-06/Liste2OccIdf_2.xlsx" class="link link--download">Liste 2</a>
<a href="/system/files/2026-06/Liste1CvlNorHdfPdl_0.xlsx" class="link link--download">Liste 1</a>
<a href="/system/files/2025-07/FAQ%20-%20Publication%20liste%20LDDS.pdf">FAQ</a>
"""


def test_get_source_urls_returns_absolute_sorted_excel_links():
    assert get_source_urls(HTML) == [
        "https://www.ess-france.org/system/files/2026-06/Liste1CvlNorHdfPdl_0.xlsx",
        "https://www.ess-france.org/system/files/2026-06/Liste2OccIdf_2.xlsx",
    ]


def test_get_source_urls_fails_without_excel_link():
    with pytest.raises(ValueError):
        get_source_urls("<p>Page en maintenance</p>")


def test_get_period_uses_most_recent_upload_month():
    assert (
        get_files_updated_date(
            [
                "https://www.ess-france.org/system/files/2026-06/Liste1CvlNorHdfPdl_0.xlsx",
                "https://www.ess-france.org/system/files/2026-07/Liste2OccIdf_2.xlsx",
            ]
        )
        == "2026-07"
    )


def test_get_period_fails_without_month_in_url():
    with pytest.raises(ValueError):
        get_files_updated_date(["https://www.ess-france.org/files/Liste1.xlsx"])


@pytest.mark.parametrize(
    "code, expected",
    [
        ("44570", "44570"),
        ("18000.0", "18000"),
        ("3000.0", "03000"),
        ("02870", "02870"),
        ("0[ND]", "[ND]"),
        ("[ND]", "[ND]"),
    ],
)
def test_normalize_postal_code(code, expected):
    assert normalize_postal_code(code) == expected


@pytest.mark.parametrize(
    "code, expected",
    [
        ("2", "02"),
        ("09", "09"),
        ("021", "21"),
        ("001", "01"),
        ("02A", "2A"),
        ("44", "44"),
        ("971", "971"),
    ],
)
def test_normalize_departement_code(code, expected):
    assert normalize_departement_code(code) == expected


def test_read_source_file(tmp_path):
    path = tmp_path / "liste.xlsx"
    pd.DataFrame(
        {
            "N°SIREN": ["015650955"],
            "Nom ou raison sociale de l'entreprise": ["IMPRIMERIE COOPERATIVE "],
            "Famille juridique de l'entreprise": ["COOPERATIVES"],
            "Libellé de la commune de l'établissement": ["MOULINS"],
            "Code postal": ["3000.0"],
            "Code du département de l'établissement": ["003"],
            "Département de l'établissement": ["ALLIER"],
            "Région de l'établissement": ["AUVERGNE RHONE ALPES"],
        }
    ).to_excel(path, index=False, sheet_name="Liste")
    rows = list(read_source_file(str(path)))
    assert len(rows) == 1
    assert dict(zip(COLUMNS, rows[0])) == {
        "SIREN": "015650955",
        "Nom ou raison sociale de l'entreprise": "IMPRIMERIE COOPERATIVE ",
        "Famille juridique de l'entreprise": "COOPERATIVES",
        "Code postal": "03000",
        "Libellé de la commune de l'établissement": "MOULINS",
        "Code du département de l'établissement": "03",
        "Département de l'établissement": "ALLIER",
        "Région de l'établissement": "AUVERGNE RHONE ALPES",
    }


def test_read_source_file_fails_on_unexpected_columns(tmp_path):
    path = tmp_path / "liste.xlsx"
    pd.DataFrame({"N°SIREN": ["015650955"], "Autre": ["x"]}).to_excel(path, index=False)
    with pytest.raises(ValueError):
        list(read_source_file(str(path)))


def _excel_bytes(sirens: list[str]) -> bytes:
    buffer = BytesIO()
    pd.DataFrame(
        {
            "N°SIREN": sirens,
            "Nom ou raison sociale de l'entreprise": ["NOM"] * len(sirens),
            "Famille juridique de l'entreprise": ["ASSOCIATIONS"] * len(sirens),
            "Libellé de la commune de l'établissement": ["PARIS"] * len(sirens),
            "Code postal": ["75001"] * len(sirens),
            "Code du département de l'établissement": ["75"] * len(sirens),
            "Département de l'établissement": ["PARIS"] * len(sirens),
            "Région de l'établissement": ["ILE DE FRANCE"] * len(sirens),
        }
    ).to_excel(buffer, index=False)
    return buffer.getvalue()


def test_gather_data_appends_all_files_with_a_single_header(tmp_path, monkeypatch):
    pages = {
        task_functions.config["source_page"]: HTML,
        "https://www.ess-france.org/system/files/2026-06/Liste1CvlNorHdfPdl_0.xlsx": _excel_bytes(
            ["000000001", "000000002"]
        ),
        "https://www.ess-france.org/system/files/2026-06/Liste2OccIdf_2.xlsx": _excel_bytes(
            ["000000002"]
        ),
    }
    monkeypatch.setattr(
        task_functions.requests,
        "get",
        lambda url: MagicMock(text=pages[url], content=pages[url]),
    )
    monkeypatch.setattr(task_functions, "TMP_FOLDER", f"{tmp_path}/")
    ti = MagicMock()
    gather_data(ti=ti)
    df = pd.read_csv(tmp_path / task_functions.OUTPUT_FILE, dtype=str)
    assert list(df.columns) == COLUMNS
    # duplicated SIREN are kept as is
    assert list(df["SIREN"]) == ["000000001", "000000002", "000000002"]
    ti.xcom_push.assert_called_once_with(key="period", value="2026-06")
