from datetime import datetime, timedelta
from airflow.models import DAG

from datagouvfr_data_pipelines.data_processing.geozones.task_functions import (
    TMP_FOLDER,
    download_and_process_geozones,
    post_geozones,
    notification_mattermost,
)
from datagouvfr_data_pipelines.utils.tasks import clean_up_folder

with DAG(
    dag_id="data_processing_geozones",
    schedule=None,
    start_date=datetime(2024, 8, 10),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    tags=["geozones", "insee", "datagouv"],
):
    
    (
        clean_up_folder(TMP_FOLDER, recreate=True)
        >> download_and_process_geozones()
        >> post_geozones()
        >> notification_mattermost()
    )
