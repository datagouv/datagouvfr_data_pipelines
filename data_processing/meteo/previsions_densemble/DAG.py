from datetime import timedelta, datetime
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
)
from datagouvfr_data_pipelines.data_processing.meteo.previsions_densemble.task_functions import (
    DATADIR,
    get_files_list_on_sftp,
    transfer_files_to_minio,
)

TMP_FOLDER = f"{AIRFLOW_DAG_TMP}meteo_pe/"
DAG_NAME = "data_processing_meteo_previsions_densemble"

default_args = {
    # 'retries': 5,
    # 'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_NAME,
    schedule_interval=None,
    start_date=datetime(2024, 6, 1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=120),
    tags=["data_processing", "meteo", "sftp"],
    default_args=default_args,
) as dag:

    clean_previous_outputs = BashOperator(
        task_id="clean_previous_outputs",
        bash_command=f"rm -rf {TMP_FOLDER} && mkdir -p {DATADIR}",
    )

    get_files_list_on_sftp = PythonOperator(
        task_id='get_files_list_on_sftp',
        python_callable=get_files_list_on_sftp,
    )

    transfer_files_to_minio = PythonOperator(
        task_id='transfer_files_to_minio',
        python_callable=transfer_files_to_minio,
    )

    get_files_list_on_sftp.set_upstream(clean_previous_outputs)
    transfer_files_to_minio.set_upstream(get_files_list_on_sftp)
