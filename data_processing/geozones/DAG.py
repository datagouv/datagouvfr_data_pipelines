from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
)
from datagouvfr_data_pipelines.data_processing.geozones.task_functions import (
    download_and_process_geozones,
    post_geozones,
    notification_mattermost,
)

topic = "geozones"
TMP_FOLDER = f"{AIRFLOW_DAG_TMP}{topic}/"
DAG_FOLDER = "datagouvfr_data_pipelines/data_processing/"
DAG_NAME = f"data_processing_{topic}"
DATADIR = f"{AIRFLOW_DAG_TMP}{topic}/data"

default_args = {
    "email": ["pierlou.ramade@data.gouv.fr", "geoffrey.aldebert@data.gouv.fr"],
    "email_on_failure": False,
}

with DAG(
    dag_id=DAG_NAME,
    schedule_interval=None,
    start_date=datetime(2024, 8, 10),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    tags=["geozones", "insee", "datagouv"],
    default_args=default_args,
) as dag:
    clean_previous_outputs = BashOperator(
        task_id="clean_previous_outputs",
        bash_command=f"rm -rf {TMP_FOLDER} && mkdir -p {TMP_FOLDER}",
    )

    download_and_process_geozones = PythonOperator(
        task_id="download_and_process_geozones",
        python_callable=download_and_process_geozones,
    )

    post_geozones = PythonOperator(
        task_id="post_geozones",
        python_callable=post_geozones,
    )

    notification_mattermost = PythonOperator(
        task_id="notification_mattermost",
        python_callable=notification_mattermost,
    )

    download_and_process_geozones.set_upstream(clean_previous_outputs)
    post_geozones.set_upstream(download_and_process_geozones)
    notification_mattermost.set_upstream(post_geozones)
