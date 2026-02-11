from datetime import datetime, timedelta
import ftplib
from airflow.models import DAG

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
    SECRET_FTP_METEO_USER,
    SECRET_FTP_METEO_PASSWORD,
    SECRET_FTP_METEO_ADDRESS,
)
from datagouvfr_data_pipelines.data_processing.meteo.ftp_processing.task_functions import (
    get_current_files_on_ftp,
    get_current_files_on_s3,
    get_and_upload_file_diff_ftp_s3,
    upload_new_files,
    handle_updated_files_same_name,
    handle_updated_files_new_name,
    update_temporal_coverages,
    log_modified_files,
    delete_replaced_s3_files,
    notification_mattermost,
)
from datagouvfr_data_pipelines.utils.tasks import clean_up_folder

TMP_FOLDER = f"{AIRFLOW_DAG_TMP}meteo/"
DAG_NAME = "data_processing_meteo"
DATADIR = f"{AIRFLOW_DAG_TMP}meteo/data"

ftp = ftplib.FTP(SECRET_FTP_METEO_ADDRESS)
ftp.login(SECRET_FTP_METEO_USER, SECRET_FTP_METEO_PASSWORD)

default_args = {
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_NAME,
    schedule="30 7,10 * * *",
    start_date=datetime(2024, 8, 10),
    catchup=False,
    dagrun_timeout=timedelta(minutes=900),
    tags=["data_processing", "meteo"],
    max_active_runs=1,
    default_args=default_args,
):

    _same_name = handle_updated_files_same_name()

    (
        clean_up_folder(DATADIR, recreate=True)
        >> [
            get_current_files_on_ftp(ftp),
            get_current_files_on_s3(),
        ]
        >> get_and_upload_file_diff_ftp_s3(ftp)
        >> [
            _same_name,
            handle_updated_files_new_name(),
        ]
        >> upload_new_files()
        >> [
            update_temporal_coverages(),
            log_modified_files(),
        ]
        >> notification_mattermost()
    )

    _same_name >> delete_replaced_s3_files()
