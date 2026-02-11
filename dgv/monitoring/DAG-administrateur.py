from datetime import datetime, timedelta

from airflow.decorators import task
from airflow.models import DAG
from datagouvfr_data_pipelines.config import MATTERMOST_MODERATION_NOUVEAUTES
from datagouvfr_data_pipelines.utils.mattermost import send_message
from datagouvfr_data_pipelines.utils.datagouv import local_client

DAG_NAME = "dgv_administrateur"


@task()
def list_current_admins(**context):
    # We want the list of all current users with an admin role
    print(f"Fetching admins from {local_client.base_url}/api/1/users/")
    users = local_client.get_all_from_api_query("api/1/users/?page_size=100")
    admins = [user for user in users if "admin" in user["roles"]]
    context["ti"].xcom_push(key="admins", value=admins)


@task()
def publish_mattermost(**context):
    print("Publishing on mattermost")
    admins = context["ti"].xcom_pull(key="admins", task_ids="list_current_admins")
    message = f":superhero: Voici la liste des {len(admins)} administrateurs actuels de data.gouv.fr"
    for admin in admins:
        message += f"\n* [{admin['first_name']} {admin['last_name']}]({admin['page']})"
    print(message)
    send_message(message, MATTERMOST_MODERATION_NOUVEAUTES)


default_args = {
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_NAME,
    schedule="0 0 1 1/3 *",
    start_date=datetime(2023, 10, 15),
    dagrun_timeout=timedelta(minutes=60),
    tags=["curation", "datagouv"],
    default_args=default_args,
    catchup=False,
):

    list_current_admins() >> publish_mattermost()
