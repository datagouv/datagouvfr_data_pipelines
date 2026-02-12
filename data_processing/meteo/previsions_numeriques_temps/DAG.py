from datetime import timedelta, datetime
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import ShortCircuitOperator

from datagouvfr_data_pipelines.data_processing.meteo.previsions_numeriques_temps.config import (
    PACKAGES,
)
from datagouvfr_data_pipelines.data_processing.meteo.previsions_numeriques_temps.task_functions import (
    LOG_PATH,
    get_latest_theorical_batches,
    clean_old_runs_in_s3,
    construct_all_possible_files,
    send_files_to_s3,
    publish_on_datagouv,
    clean_directory,
)


def create_dag(model: str, pack: str, grid: str, infos: dict):
    _id = f"_{model}_{pack}_{grid}" if not pack.startswith("$") else f"_{model}_{grid}"
    dag = DAG(
        dag_id="data_processing_meteo_pnt" + _id,
        # DAG runs every 3 minutes
        schedule="*/3 * * * *",
        start_date=datetime(2024, 6, 1),
        catchup=False,
        dagrun_timeout=timedelta(minutes=600),
        tags=["data_processing", "meteo", "pnt", model],
        # runs can run in parallel, safeguards ensure they won't interfere
        max_active_runs=2,
    )
    with dag:
        shared_kwargs = {"model": model, "pack": pack, "grid": grid, "infos": infos}

        _get_latest_theorical_batches = get_latest_theorical_batches(**shared_kwargs)
        clean_directory(**shared_kwargs)
        (
            BashOperator(
                task_id="create_working_dirs",
                bash_command=f"mkdir -p {LOG_PATH}",
            )
            >> _get_latest_theorical_batches
            >> ShortCircuitOperator(
                task_id="construct_all_possible_files",
                python_callable=construct_all_possible_files,
                op_kwargs=shared_kwargs,
            )
            >> send_files_to_s3(**shared_kwargs)
            >> publish_on_datagouv(**shared_kwargs)
        )

        _get_latest_theorical_batches >> clean_old_runs_in_s3()

    return dag


dags = []
for model in PACKAGES:
    for pack in PACKAGES[model]:
        infos = {
            k: PACKAGES[model][pack][k] for k in ["base_url", "product", "extension"]
        }
        for grid in [_ for _ in PACKAGES[model][pack] if _ not in infos]:
            # for pack in PACKAGES[model][pack][grid] ?
            dags.append(create_dag(model, pack, grid, infos))
