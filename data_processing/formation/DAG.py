from airflow.models import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.bash import BashOperator

from airflow.utils.dates import days_ago
from datetime import timedelta
from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
)
from datagouvfr_data_pipelines.data_processing.formation.task_functions import (
    compare_files_minio,
    download_latest_data,
    process_organismes_formation,
    send_file_to_minio,
    send_notification,
)

TMP_FOLDER = f"{AIRFLOW_DAG_TMP}formation/"

with DAG(
    dag_id="data_processing_formation_qualiopi",
    schedule_interval="0 3 * * MON",
    start_date=days_ago(8),
    dagrun_timeout=timedelta(minutes=15),
    tags=["formation", "qualiopi", "certification"],
    params={},
    catchup=False,
) as dag:
    clean_previous_outputs = BashOperator(
        task_id="clean_previous_outputs",
        bash_command=f"rm -rf {TMP_FOLDER} && mkdir -p {TMP_FOLDER}",
    )

    download_latest_data = PythonOperator(
        task_id="download_latest_data", python_callable=download_latest_data
    )

    process_organismes_formation = PythonOperator(
        task_id="process_organismes_formation", python_callable=process_organismes_formation
    )

    send_file_to_minio = PythonOperator(
        task_id="send_file_to_minio", python_callable=send_file_to_minio
    )

    compare_files_minio = ShortCircuitOperator(
        task_id="compare_files_minio", python_callable=compare_files_minio
    )

    send_notification = PythonOperator(
        task_id="send_notification", python_callable=send_notification
    )

    download_latest_data.set_upstream(clean_previous_outputs)
    process_organismes_formation.set_upstream(download_latest_data)
    send_file_to_minio.set_upstream(process_organismes_formation)
    compare_files_minio.set_upstream(send_file_to_minio)
    send_notification.set_upstream(compare_files_minio)
