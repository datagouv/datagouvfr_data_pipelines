from datetime import datetime, timedelta
from airflow.models import DAG

from datagouvfr_data_pipelines.data_processing.elections.aggregation.task_functions import (
    DATADIR,
    process_election_data,
    send_results_to_s3,
    publish_results_elections,
    send_notification,
)
from datagouvfr_data_pipelines.utils.tasks import clean_up_folder

DAG_FOLDER = "datagouvfr_data_pipelines/data_processing/"
DAG_NAME = "data_processing_elections"

with DAG(
    dag_id=DAG_NAME,
    schedule="15 7 1 1 *",
    start_date=datetime(2024, 8, 10),
    catchup=False,
    dagrun_timeout=timedelta(minutes=240),
    tags=["data_processing", "election", "presidentielle", "legislative"],
):
    
    (
        clean_up_folder(DATADIR, recreate=True)
        >> process_election_data()
        >> send_results_to_s3()
        >> publish_results_elections()
        >> clean_up_folder(DATADIR)
        >> send_notification()
    )
