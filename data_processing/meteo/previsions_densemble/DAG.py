from datetime import timedelta, datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
)
from datagouvfr_data_pipelines.data_processing.meteo.previsions_densemble.task_functions import (
    CONFIG,
    get_files_list_on_sftp,
    transfer_files_to_minio,
    publish_on_datagouv,
    remove_old_occurrences,
)

TMP_FOLDER = f"{AIRFLOW_DAG_TMP}meteo_pe/"
DAG_NAME = "data_processing_meteo_previsions_densemble"


def create_dag(pack: str, grid: str):
    dag = DAG(
        dag_id=DAG_NAME + f"_{pack}_{grid}",
        # DAG runs every 3 minutes
        schedule_interval="*/3 * * * *",
        start_date=datetime(2024, 6, 1),
        catchup=False,
        dagrun_timeout=timedelta(minutes=600),
        tags=["data_processing", "meteo", "sftp", pack],
        # runs can run in parallel, safeguards ensure they won't interfere
        max_active_runs=4,
    )
    with dag:

        _get_files_list_on_sftp = PythonOperator(
            task_id="get_files_list_on_sftp",
            python_callable=get_files_list_on_sftp,
        )

        _transfer_files_to_minio = ShortCircuitOperator(
            task_id=f"transfer_{pack}_{grid}",
            python_callable=transfer_files_to_minio,
            op_kwargs={
                "pack": pack,
                "grid": grid,
            },
        )

        _publish_on_datagouv = PythonOperator(
            task_id=f"publish_{pack}_{grid}",
            python_callable=publish_on_datagouv,
            op_kwargs={
                "pack": pack,
                "grid": grid,
            },
        )

        _remove_old_occurrences = PythonOperator(
            task_id=f"remove_old_{pack}_{grid}",
            python_callable=remove_old_occurrences,
            op_kwargs={
                "pack": pack,
                "grid": grid,
            },
        )

        _transfer_files_to_minio.set_upstream(_get_files_list_on_sftp)
        _publish_on_datagouv.set_upstream(_transfer_files_to_minio)
        _remove_old_occurrences.set_upstream(_publish_on_datagouv)
    return dag


dags = []
for pack in CONFIG:
    for grid in CONFIG[pack]:
        dags.append(create_dag(pack, grid))
