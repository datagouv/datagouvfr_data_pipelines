from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_HOME,
    AIRFLOW_DAG_TMP,
)
from datagouvfr_data_pipelines.data_processing.geozones.task_functions import (
    process_geozones,
    post_geozones
)

topic = "geozones"
TMP_FOLDER = f"{AIRFLOW_DAG_TMP}{topic}/"
DAG_FOLDER = 'datagouvfr_data_pipelines/data_processing/'
DAG_NAME = f'data_processing_{topic}'
DATADIR = f"{AIRFLOW_DAG_TMP}{topic}/data"

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
    dagrun_timeout=timedelta(minutes=60),
    tags=["geozones", "insee", "datagouv"],
    default_args=default_args,
) as dag:

    clean_previous_outputs = BashOperator(
        task_id="clean_previous_outputs",
        bash_command=f"rm -rf {TMP_FOLDER} && mkdir -p {TMP_FOLDER}",
    )

    download_geozones_data = BashOperator(
        task_id='download_geozones_data',
        bash_command=(
            f"bash {AIRFLOW_DAG_HOME}{DAG_FOLDER}"
            f"geozones/scripts/script_dl_geozones.sh {DATADIR} "
        )
    )

    process_geozones = PythonOperator(
        task_id='process_geozones',
        python_callable=process_geozones,
    )

    post_geozones = PythonOperator(
        task_id='post_geozones',
        python_callable=post_geozones,
    )

    download_geozones_data.set_upstream(clean_previous_outputs)
    process_geozones.set_upstream(download_geozones_data)
    post_geozones.set_upstream(process_geozones)
