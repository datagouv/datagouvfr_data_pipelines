from datetime import datetime, timedelta

from airflow.sdk import DAG
from datagouvfr_data_pipelines.data_processing.meteo.ftp_processing.task_functions import (
    TMP_FOLDER,
    delete_replaced_s3_files,
    get_and_upload_file_diff_ftp_s3,
    get_current_files_on_ftp,
    get_current_files_on_s3,
    handle_updated_files_new_name,
    handle_updated_files_same_name,
    log_modified_files,
    notification,
    update_temporal_coverages,
    upload_new_files,
)
from datagouvfr_data_pipelines.utils.tasks import clean_up_folder

default_args = {
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="data_processing_meteo",
    schedule="30 7,10 * * *",
    start_date=datetime(2024, 8, 10),
    catchup=False,
    dagrun_timeout=timedelta(minutes=1200),
    tags=["data_processing", "meteo"],
    max_active_runs=1,
    default_args=default_args,
):
    _same_name = handle_updated_files_same_name()

    (
        clean_up_folder(TMP_FOLDER, recreate=True)
        >> [
            get_current_files_on_ftp(),
            get_current_files_on_s3(),
        ]
        >> get_and_upload_file_diff_ftp_s3()
        >> [
            _same_name,
            handle_updated_files_new_name(),
        ]
        >> upload_new_files()
        >> [
            update_temporal_coverages(),
            log_modified_files(),
        ]
        >> notification()
    )

    _same_name >> delete_replaced_s3_files()
