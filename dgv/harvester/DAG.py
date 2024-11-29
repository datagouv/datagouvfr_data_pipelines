from datetime import timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from datagouvfr_data_pipelines.utils.datagouv import (
    get_all_from_api_query,
    datagouv_session,
)
from datagouvfr_data_pipelines.utils.mattermost import send_message
from datagouvfr_data_pipelines.config import (
    MATTERMOST_DATAGOUV_MOISSONNAGE,
)

DAG_NAME = "dgv_harvester_notification"
PAD_AWAITING_VALIDATION = "https://pad.incubateur.net/173bEiKKTi2laBNyHwIPlQ"

default_args = {"email": ["geoffrey.aldebert@data.gouv.fr"], "email_on_failure": True}


def get_pending_harvester_from_api(ti):
    harvesters = get_all_from_api_query("https://www.data.gouv.fr/api/1/harvest/sources/")
    harvesters = [
        {
            "admin_url": "https://www.data.gouv.fr/fr/admin/harvester/" + harvest["id"],
            "name": harvest["name"],
            "url": harvest["url"],
            "orga": (
                f"Organisation {harvest['organization']['name']}"
                if harvest["organization"]
                else f"Utilisateur {harvest['owner']['first_name']} {harvest['owner']['last_name']}"
                if harvest['owner']
                else None
            ),
            "orga_url": (
                f"https://www.data.gouv.fr/fr/organizations/{harvest['organization']['id']}/"
                if harvest["organization"]
                else f"https://www.data.gouv.fr/fr/users/{harvest['owner']['id']}/"
                if harvest['owner']
                else None
            ),
            "id": harvest["id"]
        }
        for harvest in harvesters
        if harvest["validation"]["state"] == "pending"
    ]
    ti.xcom_push(key="list_pendings", value=harvesters)


def get_preview_state_from_api(ti):
    list_pendings = ti.xcom_pull(key="list_pendings", task_ids="get_pending_harvester")
    for item in list_pendings:
        try:
            r = datagouv_session.get(
                "https://www.data.gouv.fr/api/1/harvest/source/{}/preview".format(
                    item["id"]
                ),
                timeout=60,
            )
            print(r.json())
            item["preview"] = r.json()["status"]
        except:
            print("error on " + item["id"])
            item["preview"] = "timeout"
    ti.xcom_push(key="list_pendings_complete", value=list_pendings)


def publish_mattermost_harvester(ti):
    list_pendings = ti.xcom_pull(
        key="list_pendings_complete", task_ids="get_preview_state"
    )

    list_pendings_done = [lp for lp in list_pendings if lp["preview"] == "done"]
    list_pendings_failed = [lp for lp in list_pendings if lp["preview"] == "failed"]
    list_pendings_timeout = [lp for lp in list_pendings if lp["preview"] == "timeout"]
    list_pendings_other = [
        lp for lp in list_pendings if lp["preview"] not in ["timeout", "done", "failed"]
    ]

    text = (
        ":mega: Rapport hebdo sur l'état des moissonneurs en attente : \n "
        f"- {len(list_pendings)} moissonneurs en attente \n "
        f"- {len(list_pendings_done)} moissonneurs en attente dont la preview fonctionne \n "
        f"- {len(list_pendings_timeout)} moissonneurs en attente dont la preview n'aboutit pas "
        "(timeout de 60 secondes) \n "
        f"- {len(list_pendings_failed)} moissonneurs en attente dont la preview failed \n "
        f"- {len(list_pendings_other)} moissonneurs dont la preview est dans un autre statut "
        "\n \n\nListe des moissonneurs en pending dont la preview fonctionne : \n"
    )

    for lp in list_pendings_done:
        print(lp)
        text = (
            text
            + " - [{}]({}) - Moissonneur [{}]({}) - Lien vers [l'espace Admin]({}) \n".format(
                lp["orga"], lp["orga_url"], lp["name"], lp["url"], lp["admin_url"]
            )
        )

    text = (
        text
        + "\nListe des moissonneurs en attente dont la preview n'aboutit pas (timeout de 60 secondes) : \n"
    )

    for lp in list_pendings_timeout:
        print(lp)
        text = (
            text
            + " - [{}]({}) - Moissonneur [{}]({}) - Lien vers [l'espace Admin]({}) \n".format(
                lp["orga"], lp["orga_url"], lp["name"], lp["url"], lp["admin_url"]
            )
        )

    text = text + "\nListe des moissonneurs en attente dont la preview failed : \n"

    for lp in list_pendings_failed:
        text = (
            text
            + " - [{}]({}) - Moissonneur [{}]({}) - Lien vers [l'espace Admin]({}) \n".format(
                lp["orga"], lp["orga_url"], lp["name"], lp["url"], lp["admin_url"]
            )
        )

    text = text + "\nListe des moissonneurs en pending avec un autre statut : \n"

    for lp in list_pendings_other:
        text = (
            text
            + " - [{}]({}) - Moissonneur [{}]({}) - Lien vers [l'espace Admin]({}) \n".format(
                lp["orga"], lp["orga_url"], lp["name"], lp["url"], lp["admin_url"]
            )
        )

    text = (
        f"{text}Avant validation, pensez à consulter [le pad des moissonneurs à laisser "
        f"en attente de validation]({PAD_AWAITING_VALIDATION}) \n"
    )
    send_message(text, MATTERMOST_DATAGOUV_MOISSONNAGE)
    print(text)


with DAG(
    dag_id=DAG_NAME,
    schedule_interval="0 9 * * WED",
    start_date=days_ago(8),
    dagrun_timeout=timedelta(minutes=60),
    tags=["weekly", "harvester", "mattermost", "notification"],
    default_args=default_args,
    catchup=False,
) as dag:
    get_pending_harvester = PythonOperator(
        task_id="get_pending_harvester",
        python_callable=get_pending_harvester_from_api,
    )

    get_preview_state = PythonOperator(
        task_id="get_preview_state",
        python_callable=get_preview_state_from_api,
    )

    publish_mattermost = PythonOperator(
        task_id="publish_mattermost",
        python_callable=publish_mattermost_harvester,
    )

    get_preview_state.set_upstream(get_pending_harvester)
    publish_mattermost.set_upstream(get_preview_state)
