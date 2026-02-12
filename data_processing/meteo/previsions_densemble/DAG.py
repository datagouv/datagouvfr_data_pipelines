from datetime import timedelta, datetime
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator

from datagouvfr_data_pipelines.data_processing.meteo.previsions_densemble.task_functions import (
    CONFIG,
    TMP_FOLDER,
    clean_directory,
    get_files_list_on_sftp,
    transfer_files_to_s3,
    publish_on_datagouv,
    remove_old_occurrences,
    handle_cyclonic_alert,
)

DAG_NAME = "data_processing_meteo_previsions_densemble"


def create_dag(pack: str, grid: str):
    dag = DAG(
        dag_id=DAG_NAME + f"_{pack}_{grid}",
        # DAG runs every 3 minutes
        schedule="*/3 * * * *",
        start_date=datetime(2024, 6, 1),
        catchup=False,
        dagrun_timeout=timedelta(minutes=600),
        tags=["data_processing", "meteo", "sftp", pack],
        # runs can run in parallel, safeguards ensure they won't interfere
        max_active_runs=2,
    )
    with dag:

        shared_kwargs = {"pack": pack, "grid": grid}

        _remove_old_occurrences = remove_old_occurrences(**shared_kwargs)
        
        clean_directory()
        (
            BashOperator(
                task_id="create_working_dir",
                bash_command=f"mkdir -p {TMP_FOLDER}",
            )
            >> get_files_list_on_sftp(**shared_kwargs)
            >> ShortCircuitOperator(
                task_id="transfer_files_to_s3",
                python_callable=transfer_files_to_s3,
                op_kwargs=shared_kwargs,
            )
            >> publish_on_datagouv(**shared_kwargs)
            >> _remove_old_occurrences
        )

        if CONFIG[pack][grid].get("alerte_cyclonique"):
            _remove_old_occurrences >> handle_cyclonic_alert(**shared_kwargs)

    return dag


dags = []
for pack in CONFIG:
    for grid in CONFIG[pack]:
        dags.append(create_dag(pack, grid))
