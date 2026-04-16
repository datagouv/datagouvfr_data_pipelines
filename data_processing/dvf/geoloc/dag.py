from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datagouvfr_data_pipelines.data_processing.dvf.geoloc.task_functions import (
    TMP_FOLDER,
    check_if_modif,
    download_source_data,
    enrich_years,
    publish_datagouv,
    notification,
)
from datagouvfr_data_pipelines.utils.tasks import clean_up_folder

with DAG(
    dag_id="data_processing_dvf_geoloc",
    schedule=None,
    start_date=datetime(2026, 4, 15),
    catchup=False,
    dagrun_timeout=timedelta(minutes=300),
    tags=["data_processing", "dvf"],
):
    (
        enrich_years(
            files=(
                ShortCircuitOperator(
                    task_id="check_if_modif",
                    python_callable=check_if_modif,
                )
                >> clean_up_folder(TMP_FOLDER, recreate=True)
                >> download_source_data()
            )
        )
        >> publish_datagouv()
        # >> notification()
        # >> TriggerDagRunOperator(
        #     task_id="trigger_dvf_explore",
        #     trigger_dag_id="data_processing_dvf_explore",
        # )
    )
