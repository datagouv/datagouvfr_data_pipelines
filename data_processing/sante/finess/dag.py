from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import ShortCircuitOperator
from datagouvfr_data_pipelines.data_processing.sante.finess.task_functions import (
    TMP_FOLDER,
    build_and_save,
    build_finess_table_etablissements,
    check_if_modif,
    config,
    publish_on_datagouv,
    send_notification_mattermost,
    send_to_s3,
)
from datagouvfr_data_pipelines.utils.tasks import clean_up_folder

default_args = {
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="data_processing_finess",
    schedule="0 7 * * *",
    start_date=datetime(2024, 8, 10),
    catchup=False,
    dagrun_timeout=timedelta(minutes=240),
    tags=["data_processing", "sante", "finess"],
    default_args=default_args,
):
    clean_previous_outputs = clean_up_folder(TMP_FOLDER, recreate=True)
    clean_up = clean_up_folder(
        TMP_FOLDER,
        trigger_rule="none_failed_or_skipped",
    )

    for scope in config:
        if scope == "geoloc":
            continue
        (
            clean_previous_outputs
            >> ShortCircuitOperator(
                task_id=f"check_if_modif_{scope}",
                python_callable=check_if_modif,
                op_kwargs={"scope": scope},
            )
            >> (
                build_finess_table_etablissements.override(
                    task_id=f"build_finess_table_{scope}"
                )()
                if scope == "etablissements"
                else build_and_save.override(task_id=f"build_finess_table_{scope}")(
                    scope
                )
            )
            >> send_to_s3.override(task_id=f"send_to_s3_{scope}")(scope)
            >> publish_on_datagouv.override(task_id=f"publish_on_datagouv_{scope}")(
                scope
            )
            >> clean_up
        )

    clean_up >> send_notification_mattermost()
