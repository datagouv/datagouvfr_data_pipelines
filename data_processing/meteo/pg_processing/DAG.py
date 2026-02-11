from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator
# from airflow.operators.dummy import DummyOperator
# from airflow.sensors.external_task import ExternalTaskSensor

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
)
from datagouvfr_data_pipelines.data_processing.meteo.pg_processing.task_functions import (
    create_tables_if_not_exists,
    retrieve_latest_processed_date,
    download_data,
    insert_latest_date_pg,
    send_notification,
)
from datagouvfr_data_pipelines.utils.tasks import clean_up_folder

TMP_FOLDER = f"{AIRFLOW_DAG_TMP}meteo_pg"
DAG_NAME = "data_processing_postgres_meteo"
DATADIR = f"{AIRFLOW_DAG_TMP}meteo_pg/data/"


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
    dag_id=DAG_NAME,
    # a better scheduling would be "after the second run of ftp_processing is done", will investigate
    schedule="0 12 * * *",
    start_date=datetime(2024, 10, 1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=2000),
    tags=["data_processing", "meteo"],
    default_args=default_args,
) as dag:
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

    process_data = []
    for dataset in DATASETS_TO_PROCESS:
        process_data.append(
            PythonOperator(
                task_id=f"process_data_{dataset.replace('/', '_').lower()}",
                python_callable=download_data,
                op_kwargs={
                    "dataset_name": dataset,
                },
            )
        )

    process_data_comp = []
    for dataset in DATASETS_TO_PROCESS:
        dataset_comp = dataset + "_COMP"
        process_data_comp.append(
            PythonOperator(
                task_id=f"process_data_{dataset_comp.replace('/', '_').lower()}",
                python_callable=download_data,
                op_kwargs={
                    "dataset_name": dataset_comp,
                },
            )
        )

    _retrieve_latest_processed_date = retrieve_latest_processed_date()
    _insert_latest_date_pg = insert_latest_date_pg()

    # clean_previous_outputs.set_upstream(ftp_waiting_room)

    (
        clean_up_folder(TMP_FOLDER, recreate=True)
        >> create_tables_if_not_exists()
        >> _retrieve_latest_processed_date
    )

    for i in range(0, len(process_data)):
        process_data[i].set_upstream(_retrieve_latest_processed_date)
        process_data_comp[i].set_upstream(process_data[i])
        _insert_latest_date_pg.set_upstream(process_data_comp[i])

    _insert_latest_date_pg >> send_notification()
