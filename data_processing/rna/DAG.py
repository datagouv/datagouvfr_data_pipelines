from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from datetime import datetime, timedelta
from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
)
from datagouvfr_data_pipelines.data_processing.rna.task_functions import (
    check_if_modif,
    process_rna,
    send_rna_to_minio,
    publish_on_datagouv,
    send_notification_mattermost,
)

TMP_FOLDER = f"{AIRFLOW_DAG_TMP}rna/"
DAG_FOLDER = 'datagouvfr_data_pipelines/data_processing/'
DAG_NAME = 'data_processing_rna'
DATADIR = f"{TMP_FOLDER}data"

default_args = {
    'email': [
        'pierlou.ramade@data.gouv.fr',
        'geoffrey.aldebert@data.gouv.fr'
    ],
    'email_on_failure': False
}

with DAG(
    dag_id=DAG_NAME,
    # source files are usually uploaded in the morning of the 1st of the month
    schedule_interval='0 12 1,2,15 * *',
    start_date=datetime(2024, 8, 10),
    catchup=False,
    dagrun_timeout=timedelta(minutes=240),
    tags=["data_processing", "rna", "association"],
    default_args=default_args,
) as dag:

    check_if_modif = ShortCircuitOperator(
        task_id='check_if_modif',
        python_callable=check_if_modif,
    )

    clean_previous_outputs = BashOperator(
        task_id="clean_previous_outputs",
        bash_command=f"rm -rf {TMP_FOLDER} && mkdir -p {TMP_FOLDER}",
    )

    type_tasks = {}
    for file_type in ["import", "waldec"]:
        type_tasks[file_type] = [
            PythonOperator(
                task_id=f'process_rna_{file_type}',
                python_callable=process_rna,
                op_kwargs={
                    "file_type": file_type,
                },
            ),
            PythonOperator(
                task_id=f'send_rna_to_minio_{file_type}',
                python_callable=send_rna_to_minio,
                op_kwargs={
                    "file_type": file_type,
                },
            ),
            PythonOperator(
                task_id=f'publish_on_datagouv_{file_type}',
                python_callable=publish_on_datagouv,
                op_kwargs={
                    "file_type": file_type,
                },
            ),
        ]

    send_notification_mattermost = PythonOperator(
        task_id='send_notification_mattermost',
        python_callable=send_notification_mattermost,
    )

    clean_previous_outputs.set_upstream(check_if_modif)
    for file_type in ["import", "waldec"]:
        type_tasks[file_type][0].set_upstream(clean_previous_outputs)
        type_tasks[file_type][1].set_upstream(type_tasks[file_type][0])
        type_tasks[file_type][2].set_upstream(type_tasks[file_type][1])
        send_notification_mattermost.set_upstream(type_tasks[file_type][2])
