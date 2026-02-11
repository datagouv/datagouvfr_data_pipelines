from datetime import datetime, timedelta
from airflow.models import DAG

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
)
from datagouvfr_data_pipelines.data_processing.geozones.task_functions import (
    download_and_process_geozones,
    post_geozones,
    notification_mattermost,
)
from datagouvfr_data_pipelines.utils.tasks import clean_up_folder

topic = "geozones"
TMP_FOLDER = f"{AIRFLOW_DAG_TMP}{topic}/"
DAG_FOLDER = "datagouvfr_data_pipelines/data_processing/"
DAG_NAME = f"data_processing_{topic}"
DATADIR = f"{AIRFLOW_DAG_TMP}{topic}/data"

with DAG(
    dag_id=DAG_NAME,
    schedule=None,
    start_date=datetime(2024, 8, 10),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    tags=["geozones", "insee", "datagouv"],
):
    
    (
        clean_up_folder(DATADIR, recreate=True)
        >> download_and_process_geozones()
        >> post_geozones()
        >> notification_mattermost()
    )
