from datetime import datetime, timedelta

from airflow import DAG
from datagouvfr_data_pipelines.data_processing.elections.aggregation.task_functions import (
    TMP_FOLDER,
    process_election_data,
    publish_results_elections,
    send_notification,
    send_results_to_s3,
)
from datagouvfr_data_pipelines.utils.tasks import clean_up_folder

with DAG(
    dag_id="data_processing_elections",
    schedule="15 7 1 1 *",
    start_date=datetime(2024, 8, 10),
    catchup=False,
    dagrun_timeout=timedelta(minutes=240),
    tags=["data_processing", "election", "presidentielle", "legislative"],
):
    (
        clean_up_folder(TMP_FOLDER, recreate=True)
        >> process_election_data()
        >> send_results_to_s3()
        >> publish_results_elections()
        >> clean_up_folder(TMP_FOLDER)
        >> send_notification()
    )
