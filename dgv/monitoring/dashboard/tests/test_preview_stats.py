"""Unit tests for the dashboard preview-stats computation.

These tests drive the real pipeline (``compute_stats``) from a ~100-row sample
of the *real* data.gouv.fr catalog export (``fixtures/export_resource.csv``), so
the ``extras`` JSON shapes and per-branch behavior are authentic. Assertions are
on computed *values* (preview status, error flags, families, sources, aggregate
stats), not on renames.
"""

from pathlib import Path

import pandas as pd
import pytest
from datagouvfr_data_pipelines.dgv.monitoring.dashboard import preview_stats as ps

FIXTURES = Path(__file__).parent / "fixtures"
RESOURCE_EXPORT = FIXTURES / "export_resource.csv"


@pytest.fixture(scope="module")
def computed():
    df = ps._read_export(str(RESOURCE_EXPORT))
    df_res, stats = ps.compute_stats(df)
    return df_res.set_index("id"), stats


@pytest.fixture(scope="module")
def detail(computed):
    df_res, _ = computed
    return df_res


@pytest.fixture(scope="module")
def stats(computed):
    _, stats = computed
    return stats


# ---------------------------------------------------------------------------
# Detail table: clean per-row outcomes (each real row maps to one branch)
# ---------------------------------------------------------------------------


def test_csv_has_tabular_preview(detail):
    row = detail.loc["7d3fc912-16d2-4e38-bbba-18925ad1e536"]
    assert row["famille"] == "Tabulaire"
    assert row["format normalisé"] == "csv"
    assert row["a un aperçu"]
    assert row["aperçus actifs"] == "tabular"


def test_wms_has_map_preview(detail):
    row = detail.loc["516224c3-8d19-4260-92ce-ba204449947f"]
    assert row["famille"] == "API"
    assert row["aperçus actifs"] == "map"


def test_json_has_json_preview(detail):
    row = detail.loc["22d4a279-55ec-4343-9017-21219ceaaafd"]
    assert row["famille"] == "Données structurées"
    assert row["aperçus actifs"] == "json"


def test_pdf_has_pdf_preview(detail):
    row = detail.loc["08f143f0-e125-45b1-a727-83b6576cef60"]
    assert row["famille"] == "Document"
    assert row["aperçus actifs"] == "pdf"


def test_zip_too_large_has_no_preview_and_no_other_error(detail):
    row = detail.loc["b0ec2557-b704-40e6-932c-acba5275eae4"]
    assert row["famille"] == "Archive"
    assert row["format normalisé"] == "zip"
    assert not row["a un aperçu"]
    assert row["erreur fichier trop volumineux"]
    assert not row["a une erreur"]


def test_parsing_error(detail):
    row = detail.loc["87eede98-1f69-412e-9d5c-8f674b028099"]
    assert row["erreur analyse"]
    assert row["a une erreur"]
    assert not row["a un aperçu"]


def test_source_unreachable_error(detail):
    row = detail.loc["b04e7bf3-f1c0-4095-b5f4-08a1948da124"]
    assert row["erreur source inaccessible"]
    assert row["a une erreur"]


def test_cors_no_error(detail):
    row = detail.loc["1ff1bc41-6404-4099-85a2-5d17e21aafb2"]
    assert not row["erreur cors bloqué"]
    assert not row["a une erreur"]


def test_cors_blocked_error(detail):
    # non-trusted allow-origin (not data.gouv.fr) blocks pdf/xml/json previews
    for rid in [
        "12698089-218d-49b1-a01c-a11c52240528",  # pdf, datasets.grandbesancon.fr
        "ca4e299b-bbb8-49f5-92af-cb68ff0a025c",  # pdf, colmar.ixbus.net
        "0af9a1cc-facf-47a6-afa6-009112baa347",  # xml, datasets.grandbesancon.fr
        "4b711039-a8b7-4826-b8ca-29c875dafc58",  # json, datasets.grandbesancon.fr
    ]:
        row = detail.loc[rid]
        assert row["erreur cors bloqué"]
        assert row["a une erreur"]
        assert not row["a un aperçu"]


def test_cors_missing_header_error(detail):
    row = detail.loc["4b62eac9-f991-41fe-b04a-02bd8b5aaa29"]
    assert row["erreur cors header manquant"]
    assert row["a une erreur"]


def test_cors_unknown_error(detail):
    row = detail.loc["7054b0f2-dce9-43e6-aeb6-53a364453fe8"]
    assert row["erreur cors inconnu"]


def test_remote_shp_without_extras_is_missing(detail):
    row = detail.loc["1a5ed1fa-ff16-4df2-83e3-d9ad67aef040"]
    assert row["famille"] == "Données structurées"
    assert row["format normalisé"] == "shp"
    assert not row["a un aperçu"]
    assert not row["a une erreur"]
    assert row["aperçu manquant"]


# ---------------------------------------------------------------------------
# Detail table: aggregate counts over the whole sample
# ---------------------------------------------------------------------------


def test_archived_resources_are_excluded(detail, stats):
    # fixture has 120 rows, 5 archived -> 115 resources are kept
    assert len(detail) == 115
    assert stats["nombre"].sum() == 115


def test_detail_source_distribution(detail):
    # static resources are hosted on static.data.gouv.fr; harvested resources
    # carry a harvest.last_update timestamp; the rest are remote
    assert detail["source"].value_counts().to_dict() == {
        "harvest": 71,
        "static": 25,
        "remote": 19,
    }


def test_detail_family_distribution(detail):
    assert detail["famille"].value_counts().to_dict() == {
        "Données structurées": 42,
        "Document": 20,
        "Tabulaire": 16,
        "Autre": 13,
        "API": 11,
        "Liens": 8,
        "Archive": 5,
    }


def test_detail_error_totals(detail):
    assert int(detail["a une erreur"].sum()) == 22
    assert int(detail["aperçu manquant"].sum()) == 15
    assert int(detail["erreur source inaccessible"].sum()) == 5
    assert int(detail["erreur analyse"].sum()) == 8
    assert int(detail["erreur cors bloqué"].sum()) == 4
    assert int(detail["erreur cors header manquant"].sum()) == 5
    assert int(detail["erreur cors inconnu"].sum()) == 5
    assert int(detail["erreur fichier trop volumineux"].sum()) == 12


# ---------------------------------------------------------------------------
# Aggregate stats
# ---------------------------------------------------------------------------


def test_stats_columns(stats):
    assert list(stats.columns) == [
        "famille",
        "format normalisé",
        "nombre",
        "prévisualisable",
        "% catalogue",
        "% prévisualisable",
        "% erreur",
        "% trop volumineux",
        "% prévisualisation manquante",
        "mois",
    ]


def test_stats_rows_and_counts(stats):
    by = stats.set_index(["famille", "format normalisé"])
    assert by.loc[("Document", "pdf"), "nombre"] == 20
    assert by.loc[("Données structurées", "json"), "nombre"] == 27
    assert by.loc[("Autre", "Autre"), "nombre"] == 68
    assert stats["nombre"].sum() == 115


def test_stats_percentages(stats):
    by = stats.set_index(["famille", "format normalisé"])
    pdf = by.loc[("Document", "pdf")]
    assert pdf["% catalogue"] == pytest.approx(17.4)
    assert pdf["% prévisualisable"] == pytest.approx(75.0)
    assert pdf["% erreur"] == pytest.approx(15.0)
    assert pdf["% trop volumineux"] == pytest.approx(5.0)

    autre = by.loc[("Autre", "Autre")]
    assert autre["prévisualisable"] == 31
    assert autre["% prévisualisable"] == pytest.approx(45.6)
    assert autre["% trop volumineux"] == pytest.approx(11.8)
    assert autre["% prévisualisation manquante"] == pytest.approx(22.1)

    assert (stats["mois"].str.fullmatch(r"\d{4}-\d{2}")).all()
