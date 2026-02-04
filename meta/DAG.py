from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator

from datagouvfr_data_pipelines.meta.task_functions import (
    monitor_dags,
    notification_mattermost,
)

DAG_NAME = "meta_dag"

default_args = {
    "email": ["pierlou.ramade@data.gouv.fr", "geoffrey.aldebert@data.gouv.fr"],
    "email_on_failure": False,
}


with DAG(
    dag_id=DAG_NAME,
    schedule="0 12 * * *",
    start_date=datetime(2024, 8, 10),
    dagrun_timeout=timedelta(minutes=240),
    tags=["monitoring"],
    catchup=False,
    default_args=default_args,
) as dag:
    monitor_dags = PythonOperator(
        task_id="monitor_dags",
        python_callable=monitor_dags,
    )

    notification_mattermost = PythonOperator(
        task_id="notification_mattermost",
        python_callable=notification_mattermost,
    )

    notification_mattermost.set_upstream(monitor_dags)
