from datetime import datetime, timedelta

from airflow import DAG
from datagouvfr_data_pipelines.data_processing.meteo.stats_meteo.task_functions import (
    TMP_FOLDER,
    gather_meteo_stats,
    send_notification,
    send_to_s3,
)
from datagouvfr_data_pipelines.utils.tasks import clean_up_folder

default_args = {
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="data_processing_stats_meteo",
    schedule="0 4 1 * *",
    start_date=datetime(2024, 6, 1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=20),
    tags=["data_processing", "meteo"],
    default_args=default_args,
):
    (
        clean_up_folder(TMP_FOLDER, recreate=True)
        >> gather_meteo_stats()
        >> send_to_s3()
        >> send_notification()
        >> clean_up_folder(TMP_FOLDER)
    )
