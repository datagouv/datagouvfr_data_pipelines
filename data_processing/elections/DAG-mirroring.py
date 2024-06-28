from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_HOME,
    AIRFLOW_DAG_TMP,
)

from datagouvfr_data_pipelines.data_processing.elections.task_functions import (
    get_files_minio_mirroring,
    get_all_files_miom,
    compare_minio_miom,
    download_local_files,
    send_to_minio,
)

TMP_FOLDER = f"{AIRFLOW_DAG_TMP}elections-mirroring/"
DAG_FOLDER = 'datagouvfr_data_pipelines/data_processing/'
DAG_NAME = 'data_mirroring_elections'
DATADIR = f"{AIRFLOW_DAG_TMP}elections-mirroring/data"

default_args = {
    'email': [
        'pierlou.ramade@data.gouv.fr',
        'geoffrey.aldebert@data.gouv.fr'
    ],
    'email_on_failure': False
}

with DAG(
    dag_id=DAG_NAME,
    schedule_interval='15 7 1 1 *',
    start_date=days_ago(1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=240),
    tags=["data_processing", "election", "miroir", "miom"],
    default_args=default_args,
) as dag:

    clean_previous_outputs = BashOperator(
        task_id="clean_previous_outputs",
        bash_command=f"rm -rf {TMP_FOLDER} && mkdir -p {TMP_FOLDER}",
    )

    get_files_minio_mirroring = PythonOperator(
        task_id='get_files_minio_mirroring',
        python_callable=get_files_minio_mirroring,
    )

    get_all_files_miom = PythonOperator(
        task_id='get_all_files_miom',
        python_callable=get_all_files_miom,
    )

    compare_minio_miom = PythonOperator(
        task_id='compare_minio_miom',
        python_callable=compare_minio_miom,
    )

    download_local_files = PythonOperator(
        task_id='download_local_files',
        python_callable=download_local_files,
    )

    send_to_minio = PythonOperator(
        task_id='send_to_minio',
        python_callable=send_to_minio,
    )

    get_files_minio_mirroring.set_upstream(clean_previous_outputs)
    get_all_files_miom.set_upstream(get_files_minio_mirroring)
    compare_minio_miom.set_upstream(get_all_files_miom)
    download_local_files.set_upstream(compare_minio_miom)
    send_to_minio.set_upstream(download_local_files)
