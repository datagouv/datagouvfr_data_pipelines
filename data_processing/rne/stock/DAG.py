from airflow.models import DAG
from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datagouvfr_data_pipelines.config import AIRFLOW_DAG_HOME, RNE_FTP_URL
from datagouvfr_data_pipelines.data_processing.rne.stock.task_functions import (
    TMP_FOLDER,
    DAG_FOLDER,
    unzip_files,
    send_extracted_files_to_minio,
    send_notification_mattermost,
)


with DAG(
    dag_id="get_stock_rne",
    start_date=datetime(2023, 10, 5),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=(60 * 8)),
    tags=["download", "rne", "stock"],
    params={},
) as dag:
    clean_previous_outputs = BashOperator(
        task_id="clean_previous_outputs",
        bash_command=f"rm -rf {TMP_FOLDER} && mkdir -p {TMP_FOLDER}",
    )

    get_rne_latest_stock = BashOperator(
        task_id="get_latest_stock",
        bash_command=(
            f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}rne/stock/scripts/stock.sh "
            f"{TMP_FOLDER} {RNE_FTP_URL} "
        ),
    )

    unzip_files = PythonOperator(task_id="unzip_files", python_callable=unzip_files)

    send_extracted_files_to_minio = PythonOperator(
        task_id="send_stock_rne", python_callable=send_extracted_files_to_minio
    )

    send_notification_mattermost = PythonOperator(
        task_id="send_notification_mattermost",
        python_callable=send_notification_mattermost,
    )

    get_rne_latest_stock.set_upstream(clean_previous_outputs)
    unzip_files.set_upstream(get_rne_latest_stock)
    send_extracted_files_to_minio.set_upstream(unzip_files)
    send_notification_mattermost.set_upstream(send_extracted_files_to_minio)
