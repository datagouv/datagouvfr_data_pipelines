from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from datagouvfr_data_pipelines.data_processing.sante.finess.task_functions import (
    config,
    DAG_NAME,
    TMP_FOLDER,
    check_if_modif,
    build_finess_table_etablissements,
    build_and_save,
    send_to_s3,
    publish_on_datagouv,
    send_notification_mattermost,
)
from datagouvfr_data_pipelines.utils.tasks import clean_up_folder

DAG_FOLDER = "datagouvfr_data_pipelines/data_processing/"

default_args = {
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_NAME,
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
            >> PythonOperator(
                task_id=f"build_finess_table_{scope}",
                python_callable=(
                    build_finess_table_etablissements
                    if scope == "etablissements"
                    else build_and_save
                ),
                op_kwargs=({} if scope == "etablissements" else {"scope": scope}),
            )
            >> PythonOperator(
                task_id=f"send_to_s3_{scope}",
                python_callable=send_to_s3,
                op_kwargs={"scope": scope},
            )
            >> PythonOperator(
                task_id=f"publish_on_datagouv_{scope}",
                python_callable=publish_on_datagouv,
                op_kwargs={"scope": scope},
            )
            >> clean_up
        )

    clean_up >> send_notification_mattermost()
