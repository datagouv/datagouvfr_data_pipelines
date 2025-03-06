from datetime import datetime, timedelta
import logging
from airflow.models import DAG
from airflow.operators.python import PythonOperator

from datagouvfr_data_pipelines.utils.datagouv import (
    get_all_from_api_query,
    datagouv_session,
)
from datagouvfr_data_pipelines.utils.mattermost import send_message
from datagouvfr_data_pipelines.utils.grist import (
    get_table_as_df,
    update_records,
)
from datagouvfr_data_pipelines.config import (
    AIRFLOW_ENV,
    MATTERMOST_DATAGOUV_MOISSONNAGE,
)

DAG_NAME = "dgv_harvester_notification"
PAD_AWAITING_VALIDATION = "https://pad.incubateur.net/173bEiKKTi2laBNyHwIPlQ"
doc_id = "6xrGmKARsDFR" if AIRFLOW_ENV == "prod" else "fdg8zhb22dTp"
table_id = "Moissonneurs"

default_args = {"email": ["geoffrey.aldebert@data.gouv.fr"], "email_on_failure": False}


def get_pending_harvesters(ti):
    harvesters = get_all_from_api_query("https://www.data.gouv.fr/api/1/harvest/sources/")
    harvesters = [
        {
            "name": harvest["name"],
            "url": harvest["url"],
            "owner_type": "organization" if harvest["organization"] else "user" if harvest["owner"] else None,
            "owner_name": (
                harvest['organization']['name'] if harvest["organization"]
                else f"{harvest['owner']['first_name']} {harvest['owner']['last_name']}"
                if harvest["owner"] else None
            ),
            "owner_id": (
                harvest['organization']['id'] if harvest["organization"]
                else harvest['owner']['id'] if harvest["owner"] else None
            ),
            "id": harvest["id"],
            "backend": harvest["backend"],
        }
        for harvest in harvesters
        if harvest["validation"]["state"] == "pending"
    ]
    ti.xcom_push(key="harvesters", value=harvesters)


def get_preview_state(ti):
    harvesters = ti.xcom_pull(key="harvesters", task_ids="get_pending_harvesters")
    for idx, harvester in enumerate(harvesters):
        if idx > 0 and idx % 5 == 0:
            logging.info(f"> {idx}/{len(harvesters)} processed")
        try:
            r = datagouv_session.get(
                f"https://www.data.gouv.fr/api/1/harvest/source/{harvester['id']}/preview",
                timeout=60,
            )
            harvester["preview"] = r.json()["status"]
        except:
            logging.warning("error on " + harvester["id"])
            harvester["preview"] = "timeout"
    ti.xcom_push(key="harvesters_complete", value=harvesters)


def fill_in_grist(ti):
    harvesters = ti.xcom_pull(key="harvesters_complete", task_ids="get_preview_state")
    current_table = get_table_as_df(
        doc_id=doc_id,
        table_id=table_id,
        columns_labels=False,
        usecols=[
            "harvester_id",
            "Lien_ancienne_admin",
            "Statut",
        ],
    )
    new = []
    issues = []
    for harvester in harvesters:
        to_update = {}
        if harvester["id"] not in current_table["harvester_id"].to_list():
            pivot, value = "Lien_ancienne_admin", f"https://www.data.gouv.fr/fr/admin/harvester/{harvester['id']}"
        else:
            pivot, value = "harvester_id", harvester["id"]
        rows = current_table.loc[current_table[pivot] == value]
        # handling new harvesters
        if len(rows) == 0:
            new.append(harvester)
            to_update = {
                "harvester_id": harvester["id"],
                "Lien_ancienne_admin": f"https://www.data.gouv.fr/fr/admin/harvester/{harvester['id']}",
                "Organisation": harvester["owner_name"] or "",
                "Lien_organisation": (
                    f"https://www.data.gouv.fr/fr/{harvester['owner_type']}s/{harvester['owner_id']}/"
                    if harvester["owner_type"] else ""
                ),
                "Statut": harvester["preview"],
                "Statut_bizdev": ["L", "🆕 Nouveau"],
            }
            logging.info(f"New harvester: {harvester['id']}")
        # handling existing ones
        elif len(rows) == 1:
            row = current_table.loc[current_table[pivot] == value].iloc[0]
            if row["Statut"] != harvester["preview"]:
                to_update = {"Statut": harvester["preview"]}
            if pivot != "harvester_id":
                # adding the id for future runs, it's cleaner
                # and will be used to created other columns in grist
                to_update["harvester_id"] = harvester["id"]
            if to_update:
                logging.info(f"Updating harvester: {harvester['id']}")
        else:
            issues.append(harvester)
            logging.warning(f"Too many rows for harvester: {harvester['id']}")
        update_records(
            doc_id=doc_id,
            table_id=table_id,
            conditions={pivot: value},
            new_values=to_update,
        )
    ti.xcom_push(key="new", value=new)
    ti.xcom_push(key="issues", value=issues)


def publish_mattermost(ti):
    pending_harvesters = ti.xcom_pull(key="harvesters_complete", task_ids="get_preview_state")
    new = ti.xcom_pull(key="new", task_ids="fill_in_grist")
    issues = ti.xcom_pull(key="issues", task_ids="fill_in_grist")

    text = (
        ":mega: Rapport hebdo sur l'état des moissonneurs en attente"
        " ([lien grist](https://grist.numerique.gouv.fr/o/datagouv/6xrGmKARsDFR/Suivi-moissonneurs/p/2)):\n"
        f"- {len(pending_harvesters)} moissonneurs en attente\n"
    )
    if new:
        text += f"- {len(new)} nouveaux moissonneurs :new:\n"
        for harvester in new:
            text += (
                f"   * [{harvester['owner_name']}](https://www.data.gouv.fr/fr/{harvester['owner_type']}s/{harvester['owner_id']}/)"
                f" - moissonneur {harvester['backend'].upper()}"
                f" - [admin](https://www.data.gouv.fr/fr/beta/admin/harvesters/{harvester['id']})\n"
            )
    if issues:
        text += "\n\n"
        for harvester in issues:
            text += f":warning: plusieurs lignes dans la table grist pour {harvester['name']} ({harvester['id']})\n"
    send_message(text, MATTERMOST_DATAGOUV_MOISSONNAGE)


with DAG(
    dag_id=DAG_NAME,
    schedule_interval="0 9 * * WED",
    start_date=datetime(2024, 8, 10),
    dagrun_timeout=timedelta(minutes=60),
    tags=["weekly", "harvester", "mattermost", "notification"],
    default_args=default_args,
    catchup=False,
) as dag:
    _get_pending_harvesters = PythonOperator(
        task_id="get_pending_harvesters",
        python_callable=get_pending_harvesters,
    )

    _get_preview_state = PythonOperator(
        task_id="get_preview_state",
        python_callable=get_preview_state,
    )

    _fill_in_grist = PythonOperator(
        task_id="fill_in_grist",
        python_callable=fill_in_grist,
    )

    _publish_mattermost = PythonOperator(
        task_id="publish_mattermost",
        python_callable=publish_mattermost,
    )

    _get_preview_state.set_upstream(_get_pending_harvesters)
    _fill_in_grist.set_upstream(_get_preview_state)
    _publish_mattermost.set_upstream(_fill_in_grist)
