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
    format_election_files,
    process_election_data,
    send_results_to_minio,
    publish_results_elections,
    send_notification
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
    'email_on_failure': False
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

    clean_previous_outputs = BashOperator(
        task_id="clean_previous_outputs",
        bash_command=f"rm -rf {TMP_FOLDER} && mkdir -p {TMP_FOLDER}",
    )

    download_elections_data = BashOperator(
        task_id='download_elections_data',
        bash_command=(
            f"bash {AIRFLOW_DAG_HOME}{DAG_FOLDER}"
            f"elections/scripts/script_dl_elections.sh {DATADIR} "
        )
    )

    format_election_files = PythonOperator(
        task_id='format_election_files',
        python_callable=format_election_files,
    )

    process_election_data = PythonOperator(
        task_id='process_election_data',
        python_callable=process_election_data,
    )

    send_results_to_minio = PythonOperator(
        task_id='send_results_to_minio',
        python_callable=send_results_to_minio,
    )

    publish_results_elections = PythonOperator(
        task_id='publish_results_elections',
        python_callable=publish_results_elections,
    )

    send_notification = PythonOperator(
        task_id='send_notification',
        python_callable=send_notification,
    )

    download_elections_data.set_upstream(clean_previous_outputs)
    format_election_files.set_upstream(download_elections_data)
    process_election_data.set_upstream(format_election_files)
    send_results_to_minio.set_upstream(process_election_data)
    publish_results_elections.set_upstream(send_results_to_minio)
    send_notification.set_upstream(publish_results_elections)
