import pandas as pd

from datagouvfr_data_pipelines.data_processing.dvf.explore.task_functions import (
    set_communes_parent,
)

METROPOLE_LYON = "200046977"
AIX_MARSEILLE_PROVENCE = "200054807"
METROPOLE_GRAND_PARIS = "200054781"
CA_OUEST_RHODANIEN = "200040566"
CA_ARLES = "241300417"


def _parents(codes: list[str], epci: dict[str, str]) -> dict[str, str]:
    communes = pd.DataFrame({"code_geo": codes})
    epci_communes = pd.DataFrame(
        {"code_geo": list(epci), "code_parent": list(epci.values())}
    )
    out = set_communes_parent(communes, epci_communes)
    return dict(zip(out["code_geo"], out["code_parent"]))


def test_arrondissements_are_attached_to_their_metropole():
    """Les arrondissements municipaux ne sont membres d'aucun EPCI : ils prennent
    celui de leur ville."""
    parents = _parents(["75101", "69381", "13201"], {})
    assert parents == {
        "75101": METROPOLE_GRAND_PARIS,
        "69381": METROPOLE_LYON,
        "13201": AIX_MARSEILLE_PROVENCE,
    }


def test_communes_outside_the_metropole_keep_their_own_epci():
    """Affoux (Rhône) et Arles (Bouches-du-Rhône) partagent le préfixe de
    département de Lyon et Marseille sans appartenir à leur métropole."""
    parents = _parents(
        ["69001", "13004"],
        {"69001": CA_OUEST_RHODANIEN, "13004": CA_ARLES},
    )
    assert parents == {"69001": CA_OUEST_RHODANIEN, "13004": CA_ARLES}


def test_communes_without_epci_fall_back_to_their_departement():
    assert _parents(["69999", "31555"], {}) == {"69999": "69", "31555": "31"}
