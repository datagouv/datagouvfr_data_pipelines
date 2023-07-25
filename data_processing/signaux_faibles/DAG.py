from airflow.models import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.bash import BashOperator

from airflow.utils.dates import days_ago
from datetime import timedelta
from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
)
from datagouvfr_data_pipelines.data_processing.signaux_faibles.task_functions import (
    download_signaux_faibles,
    process_signaux_faibles,
    send_file_to_minio,
    compare_files_minio,
    send_notification,
)

TMP_FOLDER = f"{AIRFLOW_DAG_TMP}signaux_faibles_ratio_financiers/"

with DAG(
    dag_id="data_processing_signaux_faibles_ratio_financiers",
    schedule_interval="0 6 * * MON",
    start_date=days_ago(8),
    dagrun_timeout=timedelta(minutes=60),
    tags=["signaux faibles", "bilans", "entreprises"],
    params={},
    catchup=False,
) as dag:
    clean_previous_outputs = BashOperator(
        task_id="clean_previous_outputs",
        bash_command=f"rm -rf {TMP_FOLDER} && mkdir -p {TMP_FOLDER}",
    )

    download_signaux_faibles = PythonOperator(
        task_id="download_signaux_faibles", python_callable=download_signaux_faibles
    )

    process_signaux_faibles = PythonOperator(
        task_id="process_signaux_faibles", python_callable=process_signaux_faibles
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
    download_signaux_faibles.set_upstream(clean_previous_outputs)
    process_signaux_faibles.set_upstream(download_signaux_faibles)
    send_file_to_minio.set_upstream(process_signaux_faibles)
    compare_files_minio.set_upstream(send_file_to_minio)
    send_notification.set_upstream(compare_files_minio)
