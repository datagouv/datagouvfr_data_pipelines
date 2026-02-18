from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import ShortCircuitOperator
from datagouvfr_data_pipelines.data_processing.formation.task_functions import (
    TMP_FOLDER,
    compare_files_s3,
    download_latest_data,
    process_organismes_formation,
    send_file_to_s3,
    send_notification,
)
from datagouvfr_data_pipelines.utils.tasks import clean_up_folder

with DAG(
    dag_id="data_processing_formation_qualiopi",
    schedule="0 3 * * MON",
    start_date=datetime(2024, 8, 10),
    dagrun_timeout=timedelta(minutes=15),
    tags=["formation", "qualiopi", "certification"],
    catchup=False,
):
    (
        clean_up_folder(TMP_FOLDER, recreate=True)
        >> download_latest_data()
        >> process_organismes_formation()
        >> send_file_to_s3()
        >> ShortCircuitOperator(
            task_id="compare_files_s3", python_callable=compare_files_s3
        )
        >> send_notification()
        >> clean_up_folder(TMP_FOLDER)
    )
