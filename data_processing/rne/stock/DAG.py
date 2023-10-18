from airflow.models import DAG
from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
)
from datagouvfr_data_pipelines.data_processing.rne.stock.task_functions import (
    TMP_FOLDER,
    get_rne_stock,
    unzip_files,
    process_rne_files,
    send_rne_to_minio,
    send_notification_mattermost,
)


with DAG(
    dag_id="data_processing_rne_dirigeants",
    start_date=datetime(2023, 10, 5),
    # schedule_interval="0 0 * * FRI",  # every Friday at midnight
    catchup=False,
    dagrun_timeout=timedelta(minutes=(60 * 8)),
    tags=["data_processing", "rne", "dirigeants"],
    params={},
) as dag:
    clean_previous_outputs = BashOperator(
        task_id="clean_previous_outputs",
        bash_command=f"rm -rf {TMP_FOLDER} && mkdir -p {TMP_FOLDER}",
    )

    get_rne_latest_stock = PythonOperator(
        task_id="get_latest_stock", python_callable=get_rne_stock
    )

    unzip_files = PythonOperator(task_id="unzip_files", python_callable=unzip_files)

    process_files = PythonOperator(
        task_id="process_files", python_callable=process_rne_files
    )

    upload_rne_to_minio = PythonOperator(
        task_id="upload_rne_to_minio", python_callable=send_rne_to_minio
    )

    send_notification_mattermost = PythonOperator(
        task_id="send_notification_mattermost",
        python_callable=send_notification_mattermost,
    )

    get_rne_latest_stock.set_upstream(clean_previous_outputs)
    unzip_files.set_upstream(get_rne_latest_stock)
    process_files.set_upstream(unzip_files)
    upload_rne_to_minio.set_upstream(process_files)
    send_notification_mattermost.set_upstream(upload_rne_to_minio)
