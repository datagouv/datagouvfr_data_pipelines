from datetime import timedelta, datetime
from airflow import DAG

from datagouvfr_data_pipelines.dgv.tabular_metrics.task_functions import (
    TMP_FOLDER,
    create_tabular_metrics_tables,
    process_logs,
)
from datagouvfr_data_pipelines.utils.tasks import clean_up_folder

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dgv_tabular_metrics",
    schedule=None,  # triggered by dgv_metrics
    start_date=datetime(2025, 11, 1),
    catchup=False,
    dagrun_timeout=None,  # the first run will catch up and run for a while, then we'll set this properly
    tags=["datagouv", "stats", "metrics", "tabular"],
    default_args=default_args,
):
    (
        clean_up_folder(TMP_FOLDER, recreate=True)
        >> create_tabular_metrics_tables()
        >> process_logs()
        >> clean_up_folder(TMP_FOLDER)
    )
