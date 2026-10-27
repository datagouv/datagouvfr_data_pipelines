from datetime import datetime, timedelta

from airflow.sdk import DAG, Param
from datagouvfr_data_pipelines.data_processing.elections.aggregation.task_functions import (
    STEPS,
    TMP_FOLDER,
    check_outputs,
    check_sources_updates,
    notification,
    process_communes,
    process_election_data,
    process_nuances,
    publish_description,
    publish_results_elections,
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
    params={
        "steps": Param(
            STEPS,
            type="array",
            items={"type": "string", "enum": STEPS},
            description="Étapes à exécuter (toutes par défaut), pour ne relancer qu'une partie du DAG",
        )
    },
):
    (
        clean_up_folder(TMP_FOLDER, recreate=True)
        >> check_sources_updates()
        >> process_election_data()
        >> process_nuances()
        >> process_communes()
        >> check_outputs()
        >> send_results_to_s3()
        >> publish_results_elections()
        >> publish_description()
        # skipped steps must not skip the clean-up nor the following steps
        >> clean_up_folder(TMP_FOLDER, trigger_rule="none_failed")
        >> notification()
    )
