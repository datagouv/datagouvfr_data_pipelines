from datetime import timedelta

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datagouvfr_data_pipelines.config import (
    MATTERMOST_DATAGOUV_TEAM
)
from datagouvfr_data_pipelines.utils.mattermost import send_message
from datagouvfr_data_pipelines.utils.datagouv import get_all_from_api_query, DATAGOUV_URL

DAG_NAME = "dgv_administrateur"


def list_current_admins(ti):
    # We want the list of all current users with an admin role
    print(f"Fetching admins from {DATAGOUV_URL}/api/1/users/")
    users = get_all_from_api_query(
        f"{DATAGOUV_URL}/api/1/users/?page_size=100",
        auth=True,
    )
    admins = [user for user in users if "admin" in user["roles"]]
    ti.xcom_push(key="admins", value=admins)


def publish_mattermost(ti):
    print("Publishing on mattermost")
    admins = ti.xcom_pull(key="admins", task_ids="list_current_admins")
    message = (
        f":superhero: Voici la liste des {len(admins)} administrateurs actuels de data.gouv.fr"
    )
    for admin in admins:
        message += f"\n* [{admin['first_name']} {admin['last_name']}]({admin['page']})"
    print(message)
    send_message(message, MATTERMOST_DATAGOUV_TEAM)


default_args = {
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_NAME,
    schedule_interval="0 0 1 1/3 *",
    start_date=datetime(2023, 10, 15),
    dagrun_timeout=timedelta(minutes=60),
    tags=["curation", "datagouv"],
    default_args=default_args,
    catchup=False,
) as dag:
    list_current_admins = PythonOperator(
        task_id="list_current_admins",
        python_callable=list_current_admins
    )

    publish_mattermost = PythonOperator(
        task_id="publish_mattermost",
        python_callable=publish_mattermost,
    )

    list_current_admins >> publish_mattermost
