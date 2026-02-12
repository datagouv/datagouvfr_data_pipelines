from datetime import datetime, timedelta
import logging
from airflow.decorators import task
from airflow.models import DAG

from datagouvfr_data_pipelines.utils.datagouv import datagouv_session, local_client
from datagouvfr_data_pipelines.utils.mattermost import send_message
from datagouvfr_data_pipelines.utils.grist import GristTable
from datagouvfr_data_pipelines.config import (
    AIRFLOW_ENV,
    MATTERMOST_DATAGOUV_MOISSONNAGE,
)

PAD_AWAITING_VALIDATION = "https://pad.incubateur.net/173bEiKKTi2laBNyHwIPlQ"
doc_id = "6xrGmKARsDFR" if AIRFLOW_ENV == "prod" else "fdg8zhb22dTp"
table = GristTable(doc_id, "Moissonneurs")


@task()
def get_pending_harvesters(**context):
    harvesters = local_client.get_all_from_api_query("api/1/harvest/sources/")
    harvesters = [
        {
            "name": harvest["name"],
            "url": harvest["url"],
            "owner_type": "organization"
            if harvest["organization"]
            else "user"
            if harvest["owner"]
            else None,
            "owner_name": (
                harvest["organization"]["name"]
                if harvest["organization"]
                else f"{harvest['owner']['first_name']} {harvest['owner']['last_name']}"
                if harvest["owner"]
                else None
            ),
            "owner_id": (
                harvest["organization"]["id"]
                if harvest["organization"]
                else harvest["owner"]["id"]
                if harvest["owner"]
                else None
            ),
            "id": harvest["id"],
            "backend": harvest["backend"],
            "created_at": harvest["created_at"][:10],
        }
        for harvest in harvesters
        if harvest["validation"]["state"] == "pending"
    ]
    context["ti"].xcom_push(key="harvesters", value=harvesters)


@task()
def get_preview_state(**context):
    harvesters = context["ti"].xcom_pull(key="harvesters", task_ids="get_pending_harvesters")
    for idx, harvester in enumerate(harvesters):
        if idx > 0 and idx % 5 == 0:
            logging.info(f"> {idx}/{len(harvesters)} processed")
        try:
            r = datagouv_session.get(
                f"https://www.data.gouv.fr/api/1/harvest/source/{harvester['id']}/preview",
                timeout=60,
            )
            harvester["preview"] = r.json()["status"]
        except Exception:
            logging.warning("error on " + harvester["id"])
            harvester["preview"] = "timeout"
    context["ti"].xcom_push(key="harvesters_complete", value=harvesters)


@task()
def fill_in_grist(**context):
    harvesters = context["ti"].xcom_pull(key="harvesters_complete", task_ids="get_preview_state")
    current_table = table.to_dataframe(
        columns_labels=False,
        usecols=[
            "harvester_id",
            "Lien_admin",
            "Statut_config_moissonnage",
        ],
    )
    new = []
    issues = []
    for harvester in harvesters:
        to_update = {}
        rows = current_table.loc[current_table["harvester_id"] == harvester["id"]]
        # handling new harvesters
        if len(rows) == 0:
            new.append(harvester)
            to_update = {
                "harvester_id": harvester["id"],
                "Organisation": harvester["owner_name"] or "",
                "Lien_organisation": (
                    f"https://www.data.gouv.fr/{harvester['owner_type']}s/{harvester['owner_id']}/"
                    if harvester["owner_type"]
                    else ""
                ),
                "Statut_config_moissonnage": harvester["preview"],
                "Statut_bizdev": "🆕 Nouveau",
                "Date_de_creation": harvester["created_at"],
                "Nom": harvester["name"],
            }
            logging.info(f"New harvester: {harvester['id']}")
        # handling existing ones
        elif len(rows) == 1:
            row = current_table.loc[
                current_table["harvester_id"] == harvester["id"]
            ].iloc[0]
            if row["Statut_config_moissonnage"] != harvester["preview"]:
                to_update = {"Statut_config_moissonnage": harvester["preview"]}
            # to be removed, just filling in the stock
            to_update["Nom"] = harvester["name"]
            if to_update:
                logging.info(f"Updating harvester: {harvester['id']}")
        else:
            issues.append(harvester)
            logging.warning(f"Too many rows for harvester: {harvester['id']}")
        table.update_records(
            conditions={"harvester_id": harvester["id"]},
            new_values=to_update,
        )
    context["ti"].xcom_push(key="new", value=new)
    context["ti"].xcom_push(key="issues", value=issues)


@task()
def publish_mattermost(**context):
    pending_harvesters = context["ti"].xcom_pull(
        key="harvesters_complete", task_ids="get_preview_state"
    )
    new = context["ti"].xcom_pull(key="new", task_ids="fill_in_grist")
    issues = context["ti"].xcom_pull(key="issues", task_ids="fill_in_grist")

    text = (
        ":mega: Rapport hebdo sur l'état des moissonneurs en attente"
        " ([lien grist](https://grist.numerique.gouv.fr/o/datagouv/6xrGmKARsDFR/Suivi-moissonneurs/p/2)):\n"
        f"- {len(pending_harvesters)} moissonneurs en attente\n"
    )
    if new:
        text += f"- {len(new)} nouveaux moissonneurs :new:\n"
        for harvester in new:
            text += (
                f"   * [{harvester['owner_name']}](https://www.data.gouv.fr/{harvester['owner_type']}s/{harvester['owner_id']}/)"
                f" - moissonneur {harvester['backend'].upper()}"
                f" - [admin](https://www.data.gouv.fr/admin/harvesters/{harvester['id']})\n"
            )
    if issues:
        text += "\n\n"
        for harvester in issues:
            text += f":warning: plusieurs lignes dans la table grist pour {harvester['name']} ({harvester['id']})\n"
    send_message(text, MATTERMOST_DATAGOUV_MOISSONNAGE)


with DAG(
    dag_id="dgv_harvester_notification",
    schedule="0 9 * * WED",
    start_date=datetime(2024, 8, 10),
    dagrun_timeout=timedelta(minutes=60),
    tags=["weekly", "harvester", "mattermost", "notification"],
    catchup=False,
):
    (
        get_pending_harvesters()
        >> get_preview_state()
        >> fill_in_grist()
        >> publish_mattermost()
    )
