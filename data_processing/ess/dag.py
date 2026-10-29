from datetime import datetime, timedelta

from airflow.sdk import DAG
from datagouvfr_data_pipelines.data_processing.ess.task_functions import (
    TMP_FOLDER,
    gather_data,
    publish_on_datagouv,
)
from datagouvfr_data_pipelines.utils.tasks import clean_up_folder

with DAG(
    dag_id="data_processing_ess_consolidation",
    schedule="0 6 * * 0",
    start_date=datetime(2026, 10, 29),
    dagrun_timeout=timedelta(minutes=30),
    tags=["data_processing", "consolidation", "entreprises"],
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
):
    (
        clean_up_folder(TMP_FOLDER, recreate=True)
        >> gather_data()
        >> publish_on_datagouv()
        >> clean_up_folder(TMP_FOLDER)
    )
