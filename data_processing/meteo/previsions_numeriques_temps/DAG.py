from datetime import timedelta, datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
)
from datagouvfr_data_pipelines.data_processing.meteo.previsions_numeriques_temps.task_functions import (
    CONFIG,
)

TMP_FOLDER = f"{AIRFLOW_DAG_TMP}meteo_pnt/"
DAG_NAME = "data_processing_meteo_previsions_numeriques_temps"


def create_dag(pack: str, grid: str):
    dag = DAG(
        dag_id=DAG_NAME + f"_{pack}_{grid}",
        # DAG runs every 3 minutes
        schedule_interval="*/3 * * * *",
        start_date=datetime(2024, 6, 1),
        catchup=False,
        dagrun_timeout=timedelta(minutes=600),
        tags=["data_processing", "meteo", "pnt", pack],
        # runs can run in parallel, safeguards ensure they won't interfere
        max_active_runs=2,
    )
    with dag:

        common_kwargs = {"pack": pack, "grid": grid}

        _get_files_list_on_sftp = PythonOperator(
            task_id="get_files_list_on_sftp",
            python_callable=get_files_list_on_sftp,
            op_kwargs=common_kwargs,
        )

    return dag


dags = []
for pack in CONFIG:
    for grid in CONFIG[pack]:
        dags.append(create_dag(pack, grid))
