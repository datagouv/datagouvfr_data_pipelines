from datetime import timedelta, datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator

from datagouvfr_data_pipelines.dgv.monitoring.previz.task_functions import (
    process_catalog,
)

DAG_NAME = "dgv_previz_monitoring"

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_NAME,
    schedule_interval="5 4 * * 1",
    start_date=datetime(2025, 6, 1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=30),
    tags=["data_processing", "hydra"],
    default_args=default_args,
) as dag:

    process_catalog = PythonOperator(
        task_id="process_catalog",
        python_callable=process_catalog,
    )
