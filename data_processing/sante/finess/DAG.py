from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from datagouvfr_data_pipelines.data_processing.sante.finess.task_functions import (
    config,
    DAG_NAME,
    TMP_FOLDER,
    DATADIR,
    check_if_modif,
    build_finess_table_etablissements,
    build_and_save,
    send_to_minio,
    publish_on_datagouv,
    send_notification_mattermost,
)

DAG_FOLDER = "datagouvfr_data_pipelines/data_processing/"

default_args = {
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_NAME,
    schedule_interval="0 7 * * *",
    start_date=datetime(2024, 8, 10),
    catchup=False,
    dagrun_timeout=timedelta(minutes=240),
    tags=["data_processing", "sante", "finess"],
    default_args=default_args,
) as dag:
    clean_previous_outputs = BashOperator(
        task_id="clean_previous_outputs",
        bash_command=f"rm -rf {TMP_FOLDER} && mkdir -p {DATADIR}",
    )

    scope_tasks = {}
    for scope in config:
        if scope == "geoloc":
            continue
        scope_tasks[scope] = [
            ShortCircuitOperator(
                task_id=f"check_if_modif_{scope}",
                python_callable=lambda: True,
                # python_callable=check_if_modif,
                # op_kwargs={"scope": scope},
            ),
            PythonOperator(
                task_id=f"build_finess_table_{scope}",
                python_callable=(
                    build_finess_table_etablissements
                    if scope == "etablissements"
                    else build_and_save
                ),
                op_kwargs=(
                    {}
                    if scope == "etablissements"
                    else {"scope": scope}
                ),
            ),
            PythonOperator(
                task_id=f"send_to_minio_{scope}",
                python_callable=send_to_minio,
                op_kwargs={"scope": scope},
            ),
            PythonOperator(
                task_id=f"publish_on_datagouv_{scope}",
                python_callable=publish_on_datagouv,
                op_kwargs={"scope": scope},
            )
        ]

    # final steps
    clean_up = BashOperator(
        task_id="clean_up",
        bash_command=f"rm -rf {TMP_FOLDER}",
        trigger_rule="none_failed_or_skipped",
    )

    send_notification_mattermost = PythonOperator(
        task_id="send_notification_mattermost",
        python_callable=send_notification_mattermost,
        trigger_rule="none_failed_or_skipped",
    )

    for scope in config:
        if scope == "geoloc":
            continue
        scope_tasks[scope][0].set_upstream(clean_previous_outputs)
        for idx, task in enumerate(scope_tasks[scope][:-1]):
            scope_tasks[scope][idx + 1].set_upstream(task)
        clean_up.set_upstream(scope_tasks[scope][-1])
    send_notification_mattermost.set_upstream(clean_up)
