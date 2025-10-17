from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator
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
    op_kwargs = {
        "client": client,
    }

    with DAG(
        dag_id=dag_id,
        schedule_interval=schedule_interval,
        start_date=datetime(2024, 10, 1),
        dagrun_timeout=timedelta(minutes=60),
        tags=["verticale", "simplifions"],
        default_args=default_args,
        catchup=False,
    ) as dag:
        get_and_format_grist_v2_data_task = PythonOperator(
            task_id="get_and_format_grist_v2_data",
            python_callable=get_and_format_grist_v2_data,
            op_kwargs=op_kwargs,
        )

        update_topics_v2_task = PythonOperator(
            task_id="update_topics_v2",
            python_callable=update_topics_v2,
            op_kwargs=op_kwargs,
        )

        update_topics_v2_task.set_upstream(get_and_format_grist_v2_data_task)

    return dag


v2_dags_params = [
    {
        "dag_id": "verticale_simplifions_v2_production",
        "schedule_interval": "0 2 * * *",  # every day at 2am
        "client": local_client,
    },
    {
        "dag_id": "verticale_simplifions_v2_demo",
        "schedule_interval": "*/30 * * * *",  # every 30 minutes
        "client": demo_client,
    },
]

for dag_params in v2_dags_params:
    create_simplifions_v2_dag(**dag_params)

# Grist watcher DAG - runs independently to monitor Grist data changes
verticale_simplifions_grist_watcher = DAG(
    dag_id="verticale_simplifions_grist_watcher",
    schedule_interval="0 4 * * *",  # every day at 4am
    start_date=datetime(2024, 10, 1),
    dagrun_timeout=timedelta(minutes=30),
    tags=["verticale", "simplifions"],
    default_args=default_args,
    catchup=False,
)

watch_grist_data_task = PythonOperator(
    task_id="watch_grist_data",
    python_callable=watch_grist_data,
    dag=verticale_simplifions_grist_watcher,
)

# Document cloner DAG - runs independently to clone the grist document
verticale_simplifions_grist_document_cloner = DAG(
    dag_id="verticale_simplifions_grist_document_cloner",
    schedule_interval="0 5 * * *",  # every day at 5 am
    start_date=datetime(2024, 10, 1),
    dagrun_timeout=timedelta(minutes=30),
    tags=["verticale", "simplifions"],
    default_args=default_args,
    catchup=False,
)

clone_grist_document_task = PythonOperator(
    task_id="clone_grist_document",
    python_callable=clone_grist_document,
    dag=verticale_simplifions_grist_document_cloner,
)
