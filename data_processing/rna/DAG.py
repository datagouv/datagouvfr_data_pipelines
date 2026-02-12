from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator

from datagouvfr_data_pipelines.data_processing.rna.task_functions import (
    TMP_FOLDER,
    check_if_modif,
    process_rna,
    send_rna_to_s3,
    publish_on_datagouv,
    send_notification_mattermost,
)
from datagouvfr_data_pipelines.utils.tasks import clean_up_folder

DAG_FOLDER = "datagouvfr_data_pipelines/data_processing/"

with DAG(
    dag_id="data_processing_rna",
    # source files are usually uploaded in the morning of the 1st of the month
    schedule="0 12 1,2,15 * *",
    start_date=datetime(2024, 8, 10),
    catchup=False,
    dagrun_timeout=timedelta(minutes=240),
    tags=["data_processing", "rna", "association"],
):
    clean_previous_outputs = clean_up_folder(TMP_FOLDER, recreate=True)

    type_tasks = {}
    for file_type in ["import", "waldec"]:
        type_tasks[file_type] = [
            PythonOperator(
                task_id=f"process_rna_{file_type}",
                python_callable=process_rna,
                op_kwargs={
                    "file_type": file_type,
                },
            ),
            PythonOperator(
                task_id=f"send_rna_to_s3_{file_type}",
                python_callable=send_rna_to_s3,
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
        >> clean_previous_outputs
    )

    for file_type in ["import", "waldec"]:
        (
            clean_previous_outputs
            >> type_tasks[file_type][0]
            >> type_tasks[file_type][1]
            >> type_tasks[file_type][2]
            >> clean_up
        )

    clean_up >> send_notification_mattermost()
