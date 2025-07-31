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

from datagouvfr_data_pipelines.dgv.simplifions.task_functions import (
    get_and_format_grist_data,
    update_topics,
    update_topics_references,
)

default_args = {
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

def create_simplifions_dag(dag_id: str, schedule_interval: str, client: Client):
    op_kwargs = {
        "client": client,
    }

    with DAG(
        dag_id=dag_id,
        schedule_interval=schedule_interval,
        start_date=datetime(2024, 10, 1),
        dagrun_timeout=timedelta(minutes=60),
        tags=["simplifions"],
        default_args=default_args,
        catchup=False,
    ) as dag:

        get_and_format_grist_data_task = PythonOperator(
            task_id="get_and_format_grist_data",
            python_callable=get_and_format_grist_data,
            op_kwargs=op_kwargs,
        )

        update_topics_task = PythonOperator(
            task_id="update_topics",
            python_callable=update_topics,
            op_kwargs=op_kwargs,
        )

        update_topics_references_task = PythonOperator(
            task_id="update_topics_references",
            python_callable=update_topics_references,
            op_kwargs=op_kwargs,
        )

        update_topics_task.set_upstream(get_and_format_grist_data_task)
        update_topics_references_task.set_upstream(update_topics_task)
    
    return dag

dags_params = [
    {
        "dag_id": "dgv_simplifions_production",
        "schedule_interval": "0 1 * * *", # every day at 1am
        "client": local_client,
    },
    {
        "dag_id": "dgv_simplifions_demo",
        "schedule_interval": "*/30 * * * *", # every 30 minutes
        "client": demo_client,
    },
]

dags = [
    create_simplifions_dag(**dag_params) for dag_params in dags_params
]
