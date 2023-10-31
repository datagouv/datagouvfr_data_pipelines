from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import ftplib
from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
    SECRET_FTP_METEO_USER,
    SECRET_FTP_METEO_PASSWORD,
    SECRET_FTP_METEO_ADDRESS
)
from datagouvfr_data_pipelines.data_processing.meteo.task_functions import (
    get_current_files_on_ftp,
    get_current_files_on_minio,
    get_and_upload_file_diff_ftp_minio,
    upload_files_datagouv,
    notification_mattermost,
)

TMP_FOLDER = f"{AIRFLOW_DAG_TMP}meteo/"
DAG_FOLDER = 'datagouvfr_data_pipelines/data_processing/'
DAG_NAME = 'data_processing_meteo'
DATADIR = f"{AIRFLOW_DAG_TMP}meteo/data"
minio_folder = "data/synchro_ftp/"

ftp = ftplib.FTP(SECRET_FTP_METEO_ADDRESS)
ftp.login(SECRET_FTP_METEO_USER, SECRET_FTP_METEO_PASSWORD)

default_args = {
    'email': [
        'pierlou.ramade@data.gouv.fr',
        'geoffrey.aldebert@data.gouv.fr'
    ],
    'email_on_failure': False
}

with DAG(
    dag_id=DAG_NAME,
    schedule_interval='15 7 1 * *',
    start_date=days_ago(1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=240),
    tags=["data_processing", "meteo"],
    default_args=default_args,
) as dag:

    clean_previous_outputs = BashOperator(
        task_id="clean_previous_outputs",
        bash_command=f"rm -rf {TMP_FOLDER} && mkdir -p {DATADIR}",
    )

    get_current_files_on_ftp = PythonOperator(
        task_id='get_current_files_on_ftp',
        python_callable=get_current_files_on_ftp,
        op_kwargs={
            "ftp": ftp,
        },
    )

    get_current_files_on_minio = PythonOperator(
        task_id='get_current_files_on_minio',
        python_callable=get_current_files_on_minio,
        op_kwargs={
            "minio_folder": minio_folder,
        },
    )

    get_and_upload_file_diff_ftp_minio = PythonOperator(
        task_id='get_and_upload_file_diff_ftp_minio',
        python_callable=get_and_upload_file_diff_ftp_minio,
        op_kwargs={
            "minio_folder": minio_folder,
            "ftp": ftp,
        },
    )

    upload_files_datagouv = PythonOperator(
        task_id='upload_files_datagouv',
        python_callable=upload_files_datagouv,
        op_kwargs={
            "minio_folder": minio_folder,
        },
    )

    notification_mattermost = PythonOperator(
        task_id='notification_mattermost',
        python_callable=notification_mattermost,
    )

    get_current_files_on_ftp.set_upstream(clean_previous_outputs)
    get_current_files_on_minio.set_upstream(clean_previous_outputs)

    get_and_upload_file_diff_ftp_minio.set_upstream(get_current_files_on_ftp)
    get_and_upload_file_diff_ftp_minio.set_upstream(get_current_files_on_minio)

    upload_files_datagouv.set_upstream(get_and_upload_file_diff_ftp_minio)
    notification_mattermost.set_upstream(upload_files_datagouv)
