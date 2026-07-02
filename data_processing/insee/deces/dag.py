from datetime import datetime, timedelta

from airflow.providers.standard.operators.python import ShortCircuitOperator
from airflow.sdk import DAG
from datagouvfr_data_pipelines.data_processing.insee.deces.task_functions import (
    TMP_FOLDER,
    check_if_modif,
    gather_data,
    notification,
    publish_on_datagouv,
    send_to_s3,
)
from datagouvfr_data_pipelines.utils.tasks import (
    clean_up_folder,
    force_rebuild_params,
)

default_args = {
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
    "provide_context": True,
}


with DAG(
    dag_id="data_processing_deces_consolidation",
    schedule="0 * * * *",
    start_date=datetime(2024, 8, 10),
    dagrun_timeout=timedelta(minutes=240),
    max_active_runs=1,
    tags=["deces", "consolidation", "datagouv"],
    catchup=False,
    default_args=default_args,
    params=force_rebuild_params(),
):
    (
        ShortCircuitOperator(
            task_id="check_if_modif",
            python_callable=check_if_modif,
        )
        >> clean_up_folder(TMP_FOLDER, recreate=True)
        >> gather_data()
        >> send_to_s3()
        >> publish_on_datagouv()
        >> clean_up_folder(TMP_FOLDER)
        >> notification()
    )
