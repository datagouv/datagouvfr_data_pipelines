import logging

import pandas as pd
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
GRIST_ID_COLUMN = "id2"

DATASETS_CATALOG_ID = "f868cca6-8da1-4369-a78d-47463f19a9a3"

TOPS = {
    "top-datasets": "metric.resources_downloads",
    "top-reuses": "metric.reuses",
    "new-datasets": "created_at",
}


@task()
def get_perimeter(**context):
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
    """Recalcule les trois tops de l'univers DEPS."""
    ids = context["ti"].xcom_pull(key="datasets", task_ids="get_perimeter")

    logging.info("Loading catalog...")
    datasets_catalog = pd.read_csv(
        f"https://www.data.gouv.fr/api/1/datasets/r/{DATASETS_CATALOG_ID}",
        sep=";",
        dtype=str,
        usecols=[
            "id",
            "title",
            "slug",
            "created_at",
            "metric.reuses",
            "metric.resources_downloads",
        ],
    )

    datasets_catalog = datasets_catalog.loc[datasets_catalog["id"].isin(ids)]

    logging.info(f"> {len(datasets_catalog)} jeux de données récupérés")

    table = GristTable(GRIST_DOC_ID, GRIST_TOPS_TABLE)

    for top_type, column in TOPS.items():
        top = datasets_catalog.sort_values(by=column, ascending=False).head(3)

        for ordre, (_, dataset) in enumerate(top.iterrows(), start=1):
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
