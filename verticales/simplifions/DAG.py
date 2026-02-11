from datetime import datetime, timedelta
from airflow.models import DAG
from datagouv import Client

# In local, demo_client and local_client are both plugged to demo.datagouv.fr
# So we need to fill both DATAGOUV_SECRET_API_KEY and DEMO_DATAGOUV_SECRET_API_KEY
# with a demo api key to avoid hitting the production api.
# In production, local_client is plugged to www.datagouv.fr
from datagouvfr_data_pipelines.utils.datagouv import (
    local_client,
    demo_client,
)

from datagouvfr_data_pipelines.verticales.simplifions.task_functions import (
    get_and_format_grist_v2_data,
    update_topics_v2,
    watch_grist_data,
    clone_grist_document,
)

default_args = {
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}


def create_simplifions_v2_dag(dag_id: str, schedule_interval: str, client: Client):

    with DAG(
        dag_id=dag_id,
        schedule=schedule_interval,
        start_date=datetime(2024, 10, 1),
        dagrun_timeout=timedelta(minutes=60),
        tags=["verticale", "simplifions"],
        default_args=default_args,
        catchup=False,
    ) as dag:
        get_and_format_grist_v2_data() >> update_topics_v2(client)

    return dag


v2_dags_params = [
    {
        "dag_id": "verticale_simplifions_v2_production",
        "schedule_interval": "0 2 * * *",  # every day at 2am
        "client": local_client,
    },
    {
        "dag_id": "verticale_simplifions_v2_demo",
        "schedule_interval": "*/30 8-20 * * *",  # every 30 minutes from 8 AM to 8 PM
        "client": demo_client,
    },
]

for dag_params in v2_dags_params:
    create_simplifions_v2_dag(**dag_params)

# Grist watcher DAG - runs independently to monitor Grist data changes
with DAG(
    dag_id="verticale_simplifions_grist_watcher",
    schedule="0 4 * * *",  # every day at 4am
    start_date=datetime(2024, 10, 1),
    dagrun_timeout=timedelta(minutes=30),
    tags=["verticale", "simplifions"],
    default_args=default_args,
    catchup=False,
):
    watch_grist_data()


# Document cloner DAG - runs independently to clone the grist document
with DAG(
    dag_id="verticale_simplifions_grist_document_cloner",
    schedule="0 5 * * *",  # every day at 5 am
    start_date=datetime(2024, 10, 1),
    dagrun_timeout=timedelta(minutes=30),
    tags=["verticale", "simplifions"],
    default_args=default_args,
    catchup=False,
):
    clone_grist_document()
