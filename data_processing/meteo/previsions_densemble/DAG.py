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
    handle_cyclonic_alert,
)

TMP_FOLDER = f"{AIRFLOW_DAG_TMP}meteo_pe/"
DAG_NAME = "data_processing_meteo_previsions_densemble"


def create_dag(pack: str, grid: str):
    dag = DAG(
        dag_id=DAG_NAME + f"_{pack}_{grid}",
        # DAG runs every 3 minutes
        # schedule_interval="*/3 * * * *",
        schedule_interval=None,
        start_date=datetime(2024, 6, 1),
        catchup=False,
        dagrun_timeout=timedelta(minutes=600),
        tags=["data_processing", "meteo", "sftp", pack],
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

        _transfer_files_to_minio = ShortCircuitOperator(
            task_id="transfer_files_to_minio",
            python_callable=transfer_files_to_minio,
            op_kwargs=common_kwargs,
        )

        _publish_on_datagouv = PythonOperator(
            task_id="publish_on_datagouv",
            python_callable=publish_on_datagouv,
            op_kwargs=common_kwargs,
        )

        _remove_old_occurrences = PythonOperator(
            task_id="remove_old_occurrences",
            python_callable=remove_old_occurrences,
            op_kwargs=common_kwargs,
        )

        _transfer_files_to_minio.set_upstream(_get_files_list_on_sftp)
        _publish_on_datagouv.set_upstream(_transfer_files_to_minio)
        _remove_old_occurrences.set_upstream(_publish_on_datagouv)

        if CONFIG[pack][grid].get("alerte_cyclonique"):
            _handle_cyclonic_alert = PythonOperator(
                task_id="handle_cyclonic_alert",
                python_callable=handle_cyclonic_alert,
                op_kwargs=common_kwargs,
            )
            _handle_cyclonic_alert.set_upstream(_remove_old_occurrences)

    return dag


dags = []
for pack in CONFIG:
    for grid in CONFIG[pack]:
        dags.append(create_dag(pack, grid))
