from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from operators.clean_folder import CleanFolderOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from dag_datagouv_data_pipelines.config import (
    AIRFLOW_DAG_HOME,
    AIRFLOW_DAG_TMP,
)
from dag_datagouv_data_pipelines.data_processing.dvf.task_functions import (
    
)

TMP_FOLDER = f"{AIRFLOW_DAG_TMP}elections/"
DAG_FOLDER = 'dag_datagouv_data_pipelines/data_processing/'
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
    dagrun_timeout=timedelta(minutes=60),
    tags=["data_processing", "election", "presidentielle"],
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

    download_elections_data.set_upstream(clean_previous_outputs)
