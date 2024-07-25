from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_HOME,
    AIRFLOW_DAG_TMP,
)

from datagouvfr_data_pipelines.data_processing.elections.miom_mirroring.task_functions import (
    get_files_updated_miom,
    download_local_files,
    send_to_minio,
    send_exports_to_minio,
    download_from_minio,
    check_if_continue,
    create_candidats_files,
    publish_results_elections,
    create_resultats_files,
)

TMP_FOLDER = f"{AIRFLOW_DAG_TMP}elections-mirroring/"
DAG_FOLDER = 'datagouvfr_data_pipelines/data_processing/'
DAG_NAME = 'data_mirroring_elections'
DATADIR = f"{AIRFLOW_DAG_TMP}elections-mirroring/data"
ID_CURRENT_ELECTION = "LG2024"

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

    get_files_updated_miom = PythonOperator(
        task_id='get_files_updated_miom',
        python_callable=get_files_updated_miom,
    )

    check_if_continue = ShortCircuitOperator(
        task_id="check_if_continue", python_callable=check_if_continue,
    )

    download_local_files = PythonOperator(
        task_id='download_local_files',
        python_callable=download_local_files,
    )

    send_to_minio = PythonOperator(
        task_id='send_to_minio',
        python_callable=send_to_minio,
    )

    download_from_minio = PythonOperator(
        task_id='download_from_minio',
        python_callable=download_from_minio,
    )

    zip_folder = BashOperator(
        task_id="zip_folder",
        bash_command=f"cd {AIRFLOW_DAG_TMP}elections-mirroring/ && zip -r {ID_CURRENT_ELECTION}.zip ./export/ ",
    )

    create_candidats_files = PythonOperator(
        task_id='create_candidats_files',
        python_callable=create_candidats_files,
    )

    create_resultats_files = PythonOperator(
        task_id='create_resultats_files',
        python_callable=create_resultats_files,
    )

    send_exports_to_minio = PythonOperator(
        task_id='send_exports_to_minio',
        python_callable=send_exports_to_minio,
    )

    publish_results_elections = PythonOperator(
        task_id='publish_results_elections',
        python_callable=publish_results_elections,
    )

    get_files_updated_miom.set_upstream(clean_previous_outputs)
    check_if_continue.set_upstream(get_files_updated_miom)
    download_local_files.set_upstream(check_if_continue)
    send_to_minio.set_upstream(download_local_files)
    download_from_minio.set_upstream(send_to_minio)
    zip_folder.set_upstream(download_from_minio)
    create_candidats_files.set_upstream(zip_folder)
    create_resultats_files.set_upstream(create_candidats_files)
    send_exports_to_minio.set_upstream(create_resultats_files)
    publish_results_elections.set_upstream(send_exports_to_minio)

