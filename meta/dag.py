from datetime import datetime, timedelta

from airflow import DAG
from datagouvfr_data_pipelines.meta.task_functions import (
    monitor_dags,
    notification,
)


with DAG(
    dag_id="meta_dag",
    schedule="0 12 * * *",
    start_date=datetime(2024, 8, 10),
    dagrun_timeout=timedelta(minutes=240),
    tags=["monitoring"],
    catchup=False,
):
    monitor_dags() >> notification()
