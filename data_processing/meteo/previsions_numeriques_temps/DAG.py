from datetime import timedelta, datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
)
from datagouvfr_data_pipelines.data_processing.meteo.previsions_numeriques_temps.config import (
    PACKAGES,
)
from datagouvfr_data_pipelines.data_processing.meteo.previsions_numeriques_temps.task_functions import (
    get_latest_theorical_batches,
    clean_old_runs_in_minio,
    construct_all_possible_files,
    send_files_to_minio,
)

TMP_FOLDER = f"{AIRFLOW_DAG_TMP}meteo_pnt/"
DAG_NAME = "data_processing_meteo_pnt"


def create_dag(model: str, pack: str, grid: str, infos: dict):
    _id = f"_{model}_{pack}_{grid}" if not pack.startswith("$") else f"_{model}_{grid}"
    dag = DAG(
        dag_id=DAG_NAME + _id,
        # DAG runs every 3 minutes
        # schedule_interval="*/3 * * * *",
        schedule_interval=None,
        start_date=datetime(2024, 6, 1),
        catchup=False,
        dagrun_timeout=timedelta(minutes=600),
        tags=["data_processing", "meteo", "pnt", model],
        # runs can run in parallel, safeguards ensure they won't interfere
        max_active_runs=1,
    )
    with dag:

        common_kwargs = {"model": model, "pack": pack, "grid": grid, "infos": infos}

        _get_latest_theorical_batches = PythonOperator(
            task_id="get_latest_theorical_batches",
            python_callable=get_latest_theorical_batches,
            op_kwargs=common_kwargs,
        )

        _clean_old_runs_in_minio = PythonOperator(
            task_id="clean_old_runs_in_minio",
            python_callable=clean_old_runs_in_minio,
        )

        _construct_all_possible_files = ShortCircuitOperator(
            task_id="construct_all_possible_files",
            python_callable=construct_all_possible_files,
            op_kwargs=common_kwargs,
        )

        _send_files_to_minio = PythonOperator(
            task_id="send_files_to_minio",
            python_callable=send_files_to_minio,
            op_kwargs=common_kwargs,
        )

        _clean_old_runs_in_minio.set_upstream(_get_latest_theorical_batches)

        _construct_all_possible_files.set_upstream(_get_latest_theorical_batches)
        _send_files_to_minio.set_upstream(_construct_all_possible_files)

    return dag


dags = []
for model in PACKAGES:
    for pack in PACKAGES[model]:
        infos = {k: PACKAGES[model][pack][k] for k in ["base_url", "product", "extension"]}
        for grid in [_ for _ in PACKAGES[model][pack] if _ not in infos]:
            # for pack in PACKAGES[model][pack][grid] ?
            dags.append(create_dag(model, pack, grid, infos))
