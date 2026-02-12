from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.models.baseoperator import chain
from airflow.operators.python import PythonOperator
# from airflow.operators.dummy import DummyOperator
# from airflow.sensors.external_task import ExternalTaskSensor

from datagouvfr_data_pipelines.data_processing.meteo.pg_processing.task_functions import (
    TMP_FOLDER,
    create_tables_if_not_exists,
    retrieve_latest_processed_date,
    download_data,
    insert_latest_date_pg,
    send_notification,
)
from datagouvfr_data_pipelines.utils.tasks import clean_up_folder

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

DATASETS_TO_PROCESS = [
    "BASE/MENS",
    "BASE/DECAD",
    "BASE/DECADAGRO",
    "BASE/QUOT",
    "BASE/HOR",
    "BASE/MIN",
]


with DAG(
    dag_id="data_processing_postgres_meteo",
    # TODO: a better scheduling would be "after the second run of ftp_processing is done"
    schedule="0 12 * * *",
    start_date=datetime(2024, 10, 1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=2000),
    tags=["data_processing", "meteo"],
    default_args=default_args,
):
    # ftp_waiting_room = ExternalTaskSensor(
    #     task_id="ftp_waiting_room",
    #     external_dag_id="data_processing_meteo",
    #     external_task_id="notification_mattermost",
    #     execution_date_fn=second_run_execution_date,
    #     timeout=600,
    #     poke_interval=60,
    #     mode='poke',
    # )

    # ftp_waiting_room = DummyOperator(task_id='ftp_waiting_room')

    processes: list[tuple] = []
    for dataset in DATASETS_TO_PROCESS:
        dataset_comp = dataset + "_COMP"
        processes.append(
            (
                PythonOperator(
                    task_id=f"process_data_{dataset.replace('/', '_').lower()}",
                    python_callable=download_data,
                    op_kwargs={"dataset_name": dataset},
                ),
                PythonOperator(
                    task_id=f"process_data_{dataset_comp.replace('/', '_').lower()}",
                    python_callable=download_data,
                    op_kwargs={"dataset_name": dataset_comp},
                ),
            )
        )

    _retrieve_latest_processed_date = retrieve_latest_processed_date()
    _insert_latest_date_pg = insert_latest_date_pg()

    (
        # ftp_waiting_room >>
        clean_up_folder(TMP_FOLDER, recreate=True)
        >> create_tables_if_not_exists()
        >> _retrieve_latest_processed_date
    )

    for process in processes:
        chain(
            _retrieve_latest_processed_date,
            *process,
            _insert_latest_date_pg,
        )

    _insert_latest_date_pg >> send_notification()
