import logging

import requests
from airflow.sdk import task
from datagouvfr_data_pipelines.utils.datagouv import local_client
from datagouvfr_data_pipelines.utils.grist import GristTable
from datagouvfr_data_pipelines.utils.tchap import send_message

# topic "univers-culture-deps" : source du périmètre, curé dans data.gouv.fr.
# Le DAG le lit, il ne l'écrit jamais.
topic_id = "69f2fca0f4f30af95d4bab8a"

# document Grist "Portail-Culture-univers-DEPS", reconsommé par la verticale
GRIST_DOC_ID = "mrvekg9fyhQZ"
GRIST_TOPS_TABLE = "Tops"

# La colonne libellée « id » à l'export est exposée par l'API sous le colId
# `id2` : Grist réserve `id` pour l'identifiant de ligne.
GRIST_ID_COLUMN = "id2"

# Critère de tri de chaque top, appliqué à l'objet renvoyé par l'API.
# Les dates ISO 8601 se trient correctement en chaînes de caractères.
TOPS = {
    "top-datasets": lambda dataset: dataset["metrics"]["resources_downloads"],
    "top-reuses": lambda dataset: dataset["metrics"]["reuses"],
    "new-datasets": lambda dataset: dataset["created_at"],
}


@task()
def get_perimeter(**context):
    """Récupère les identifiants des jeux de données rattachés au topic.

    Le contenu d'un topic est exposé par la sous-ressource paginée `/elements/`.
    Un élément peut être marqué « donnée non trouvée », d'où le champ `element`
    potentiellement nul.
    """
    ids = [
        element["element"]["id"]
        for element in local_client.get_all_from_api_query(
            f"api/2/topics/{topic_id}/elements/?class=Dataset"
        )
        if element.get("element")
    ]

    if not ids:
        raise ValueError("Aucun jeu de données dans le topic univers-culture-deps")

    ids = sorted(set(ids))

    logging.info(f"> {len(ids)} jeux de données dans le périmètre DEPS")
    context["ti"].xcom_push(key="datasets", value=ids)


@task()
def refresh_tops(**context):
    """Recalcule les trois tops et met à jour les 9 lignes préexistantes de la
    table Grist, identifiées par (type, type_content, ordre).
    """
    ids = context["ti"].xcom_pull(key="datasets", task_ids="get_perimeter")

    datasets = []
    for obj_id in ids:
        r = requests.get(f"{local_client.base_url}/api/2/datasets/{obj_id}/")
        if r.status_code == 404:
            # Jeu de données supprimé mais toujours rattaché au topic.
            logging.warning(f"Jeu de données introuvable : {obj_id}")
            continue

        r.raise_for_status()
        datasets.append(r.json())

    logging.info(f"> {len(datasets)} jeux de données récupérés")

    table = GristTable(GRIST_DOC_ID, GRIST_TOPS_TABLE)

    for top_type, sort_key in TOPS.items():
        top = sorted(datasets, key=sort_key, reverse=True)[:3]

        for ordre, dataset in enumerate(top, start=1):
            table.update_records(
                conditions={
                    "type": top_type,
                    "type_content": "datasets",
                    "ordre": ordre,
                },
                new_values={
                    "titre": dataset["title"],
                    "slug": dataset["slug"],
                    GRIST_ID_COLUMN: dataset["id"],
                },
            )


@task()
def notification():
    send_message(text="📊 Tops de l'univers DEPS mis à jour.")
