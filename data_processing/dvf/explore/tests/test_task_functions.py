import pandas as pd
from datagouvfr_data_pipelines.data_processing.dvf.explore.task_functions import (
    add_communes_plm,
    build_types_bien,
)


def _mutations():
    """Une mutation par ville à arrondissements, plus une commune ordinaire."""
    return pd.DataFrame(
        {
            "code_commune": ["75101", "75102", "69381", "13201", "31555"],
            "month": [1, 2, 3, 4, 5],
            "code_type_local": [2, 2, 1, 1, 2],
            "prix_m2": [10000.0, 12000.0, 5000.0, 4000.0, 3000.0],
        }
    )


def test_plm_communes_have_their_own_mutations():
    out = add_communes_plm(_mutations())
    assert sorted(out.loc[out["code_commune"] == "75056", "prix_m2"]) == [
        10000.0,
        12000.0,
    ]
    assert list(out.loc[out["code_commune"] == "69123", "prix_m2"]) == [5000.0]
    assert list(out.loc[out["code_commune"] == "13055", "prix_m2"]) == [4000.0]


def test_arrondissements_and_other_communes_are_kept_once():
    out = add_communes_plm(_mutations())
    for code in ["75101", "75102", "69381", "13201", "31555"]:
        assert (out["code_commune"] == code).sum() == 1


def test_duplicated_rows_keep_their_other_columns():
    """Seul le code commune change.

    La duplication doit être transparente pour le reste de la ligne, sans quoi
    les stats de la ville porteraient sur des mutations amputées.
    """
    out = add_communes_plm(_mutations())
    paris_1er = out.loc[out["code_commune"] == "75101"].iloc[0]
    duplicate = out.loc[
        (out["code_commune"] == "75056") & (out["prix_m2"] == paris_1er["prix_m2"])
    ].iloc[0]
    assert duplicate["month"] == paris_1er["month"]
    assert duplicate["code_type_local"] == paris_1er["code_type_local"]


def test_communes_without_arrondissements_are_untouched():
    mutations = pd.DataFrame(
        {
            "code_commune": ["31555", "44109"],
            "prix_m2": [3000.0, 2500.0],
        }
    )
    pd.testing.assert_frame_equal(add_communes_plm(mutations), mutations)


def test_build_types_bien_one_label_per_code_type():
    df = pd.DataFrame(
        {
            "code_type_local": [2, 1, 2, 1, 4],
            "type_local": [
                "Appartement",
                "Maison",
                "Appartement",
                "Maison",
                "Local",
            ],
        }
    )
    assert build_types_bien(df) == {
        2: "Appartement",
        1: "Maison",
        4: "Local",
    }


def test_build_types_bien_ignores_rows_with_na_pair():
    """Les lignes sans type_local ne doivent pas polluer le mapping."""
    df = pd.DataFrame(
        {
            "code_type_local": [2, 2, None],
            "type_local": ["Appartement", "Appartement", "Dépendance"],
        }
    )
    assert build_types_bien(df) == {2: "Appartement"}


def test_build_types_bien_first_label_wins_for_ambiguous_code():
    """Si un même code_type_local porte plusieurs libellés, le premier gagne.

    value_counts() est stable à comptage égal, donc le libellé de la première
    occurrence dans le fichier est retenu.
    """
    df = pd.DataFrame(
        {
            "code_type_local": [3, 3],
            "type_local": ["Dépendance", "Autre"],
        }
    )
    assert build_types_bien(df) == {3: "Dépendance"}
