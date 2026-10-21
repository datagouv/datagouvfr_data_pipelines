from datetime import datetime, timedelta
from airflow.models import DAG

from datagouvfr_data_pipelines.data_processing.decoupage_administratif.task_functions import (
    update_create_resources,
    specific_sort_ressources,
    update_temporal_coverage,
    notification,
)

DAG_NAME = "data_processing_update_decoupage_administratif_ressources_list_datagouv"

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
    tags=["decoupage-administratif", "resources", "datagouv"],
    catchup=False,
    default_args=default_args,
) as dag:
    (
        update_temporal_coverage()
        >> update_create_resources()
        >> specific_sort_ressources()
        >> notification()
    )
