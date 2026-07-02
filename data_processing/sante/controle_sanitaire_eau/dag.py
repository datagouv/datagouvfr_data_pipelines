from datetime import datetime, timedelta

from airflow.providers.standard.operators.python import ShortCircuitOperator
from airflow.sdk import DAG
from datagouvfr_data_pipelines.data_processing.sante.controle_sanitaire_eau.task_functions import (
    TMP_FOLDER,
    config,
    check_if_modif,
    notification,
    process_data,
    publish_on_datagouv,
    send_to_s3,
)
from datagouvfr_data_pipelines.utils.tasks import (
    clean_up_folder,
    force_rebuild_params,
)

DAG_FOLDER = "datagouvfr_data_pipelines/data_processing/"

default_args = {
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="data_processing_controle_sanitaire_eau",
    schedule="0 7 * * *",
    start_date=datetime(2024, 8, 10),
    catchup=False,
    dagrun_timeout=timedelta(minutes=240),
    tags=["data_processing", "sante", "eau"],
    default_args=default_args,
    params=force_rebuild_params(),
):
    _process_data = process_data()
    clean_up = clean_up_folder(TMP_FOLDER)

    (
        ShortCircuitOperator(
            task_id="check_if_modif",
            python_callable=check_if_modif,
        )
        >> clean_up_folder(TMP_FOLDER, recreate=True)
        >> _process_data
    )
    for scope in config.keys():
        (
            _process_data
            >> send_to_s3.override(task_id=f"send_to_s3_{scope}")(scope)
            >> publish_on_datagouv.override(task_id=f"publish_on_datagouv_{scope}")(
                scope
            )
            >> clean_up
        )
    clean_up >> notification()
