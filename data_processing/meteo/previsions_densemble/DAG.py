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

with DAG(
    dag_id=DAG_NAME,
    # DAG runs every 5 minutes
    schedule_interval="*/5 * * * *",
    start_date=datetime(2024, 6, 1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=600),
    tags=["data_processing", "meteo", "sftp"],
    # runs can run in parallel, safeguards ensure they won't interfere
    max_active_runs=2,
) as dag:

    get_files_list_on_sftp = PythonOperator(
        task_id="get_files_list_on_sftp",
        python_callable=get_files_list_on_sftp,
    )

    transfers, publications, removals = [], [], []
    for pack in CONFIG:
        for grid in CONFIG[pack]:
            transfers.append(
                ShortCircuitOperator(
                    task_id=f"transfer_{pack}_{grid}",
                    python_callable=transfer_files_to_minio,
                    op_kwargs={
                        "pack": pack,
                        "grid": grid,
                    },
                )
            )
            publications.append(
                PythonOperator(
                    task_id=f"publish_{pack}_{grid}",
                    python_callable=publish_on_datagouv,
                    op_kwargs={
                        "pack": pack,
                        "grid": grid,
                    },
                )
            )
            removals.append(
                PythonOperator(
                    task_id=f"remove_old_{pack}_{grid}",
                    python_callable=remove_old_occurrences,
                    op_kwargs={
                        "pack": pack,
                        "grid": grid,
                    },
                )
            )

    for transfer, publication, removal in zip(transfers, publications, removals):
        transfer.set_upstream(get_files_list_on_sftp)
        publication.set_upstream(transfer)
        removal.set_upstream(publication)
