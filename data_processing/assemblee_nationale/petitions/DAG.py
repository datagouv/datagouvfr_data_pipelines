from datetime import datetime, timedelta

from airflow import DAG

from datagouvfr_data_pipelines.data_processing.assemblee_nationale.petitions.task_functions import (
    TMP_FOLDER,
    gather_petitions,
    send_petitions_to_s3,
    publish_on_datagouv,
    send_notification_mattermost,
)
from datagouvfr_data_pipelines.utils.tasks import clean_up_folder

with DAG(
    dag_id="data_processing_an_petitions",
    # every monday morning
    schedule="0 2 * * 1",
    start_date=datetime(2025, 7, 29),
    catchup=False,
    dagrun_timeout=timedelta(minutes=240),
    tags=["data_processing", "assemble_nationale", "petitions"],
):
    (
        clean_up_folder(TMP_FOLDER, recreate=True)
        >> gather_petitions()
        >> send_petitions_to_s3()
        >> publish_on_datagouv()
        >> clean_up_folder(TMP_FOLDER)
        >> send_notification_mattermost()
    )
