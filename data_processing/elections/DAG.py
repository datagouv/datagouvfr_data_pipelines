from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from operators.clean_folder import CleanFolderOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_HOME,
    AIRFLOW_DAG_TMP,
)
from datagouvfr_data_pipelines.data_processing.elections.task_functions import (
    format_election_files_func,
    process_election_data_func,
    send_stats_to_minio_func,
    publish_stats_elections_func
)

TMP_FOLDER = f"{AIRFLOW_DAG_TMP}elections/"
DAG_FOLDER = 'datagouvfr_data_pipelines/data_processing/'
DAG_NAME = 'data_processing_elections'
DATADIR = f"{AIRFLOW_DAG_TMP}elections/data"

default_args = {
    'email': [
        'pierlou.ramade@data.gouv.fr',
        'geoffrey.aldebert@data.gouv.fr'
    ],
    'email_on_failure': True
}

with DAG(
    dag_id=DAG_NAME,
    schedule_interval='15 7 1 * *',
    start_date=days_ago(1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=240),
    tags=["data_processing", "election", "presidentielle", "legislative"],
    default_args=default_args,
) as dag:

    clean_previous_outputs = CleanFolderOperator(
        task_id="clean_previous_outputs",
        folder_path=TMP_FOLDER
    )

    download_elections_data = BashOperator(
        task_id='download_elections_data',
        bash_command=(
            f"sh {AIRFLOW_DAG_HOME}{DAG_FOLDER}"
            f"elections/scripts/script_dl_elections.sh {DATADIR}"
        )
    )

    format_election_files = PythonOperator(
        task_id='format_election_files',
        python_callable=format_election_files_func,
    )

    process_election_data = PythonOperator(
        task_id='process_election_data',
        python_callable=process_election_data_func,
    )

    send_stats_to_minio = PythonOperator(
        task_id='send_stats_to_minio',
        python_callable=send_stats_to_minio_func,
    )

    publish_stats_elections = PythonOperator(
        task_id='publish_stats_elections',
        python_callable=publish_stats_elections_func,
    )

    download_elections_data.set_upstream(clean_previous_outputs)
    format_election_files.set_upstream(download_elections_data)
    process_election_data.set_upstream(format_election_files)
    send_stats_to_minio.set_upstream(process_election_data)
    publish_stats_elections.set_upstream(process_election_data)
