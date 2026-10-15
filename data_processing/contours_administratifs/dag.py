from datetime import datetime, timedelta
from airflow.models import DAG

from datagouvfr_data_pipelines.data_processing.contours_administratifs.task_functions import (
    update_temporal_coverage,
    update_create_resources,
    specific_sort_ressources,
    notification,
)

DAG_NAME = "data_processing_update_contours_administratifs_ressources_list_datagouv"

default_args = {
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
    "provide_context": True,
}

with DAG(
    dag_id=DAG_NAME,
    schedule="0 0 1 * *",
    start_date=datetime(2026, 5, 11),
    dagrun_timeout=timedelta(minutes=240),
    tags=["contours-administratifs", "resources", "datagouv"],
    catchup=False,
    default_args=default_args,
) as dag:
    (
        update_temporal_coverage()
        >> update_create_resources()
        >> specific_sort_ressources()
        >> notification()
    )
