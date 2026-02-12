from datetime import datetime, timedelta
import json
from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator

from datagouvfr_data_pipelines.config import AIRFLOW_DAG_HOME
from datagouvfr_data_pipelines.data_processing.sante.controle_sanitaire_eau.task_functions import (
    TMP_FOLDER,
    check_if_modif,
    process_data,
    send_to_s3,
    publish_on_datagouv,
    send_notification_mattermost,
)
from datagouvfr_data_pipelines.utils.tasks import clean_up_folder

DAG_FOLDER = "datagouvfr_data_pipelines/data_processing/"

with open(
    f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}sante/controle_sanitaire_eau/config/dgv.json"
) as fp:
    config = json.load(fp)

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
):
    _process_data = process_data()

    type_tasks = {}
    for file_type in config.keys():
        type_tasks[file_type] = [
            PythonOperator(
                task_id=f"send_to_s3_{file_type}",
                python_callable=send_to_s3,
                op_kwargs={
                    "file_type": file_type,
                },
            ),
            PythonOperator(
                task_id=f"publish_on_datagouv_{file_type}",
                python_callable=publish_on_datagouv,
                op_kwargs={
                    "file_type": file_type,
                },
            ),
        ]

    clean_up = clean_up_folder(TMP_FOLDER)

    (
        ShortCircuitOperator(
            task_id="check_if_modif",
            python_callable=check_if_modif,
        )
        >> clean_up_folder(TMP_FOLDER, recreate=True)
        >> _process_data
    )
    for file_type in config.keys():
        (
            _process_data
            >> type_tasks[file_type][0]
            >> type_tasks[file_type][1]
            >> clean_up
        )
    clean_up >> send_notification_mattermost()
