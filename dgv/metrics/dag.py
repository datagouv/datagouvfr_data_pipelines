from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datagouvfr_data_pipelines.dgv.metrics.task import (
    TMP_FOLDER,
    aggregate_log,
    copy_logs_to_processed_folder,
    create_metrics_tables,
    delete_old_log_files,
    download_catalog,
    download_log,
    get_new_logs,
    matomo_postgres_duplication_safety,
    process_log,
    process_matomo,
    refresh_materialized_views,
    save_matomo_to_postgres,
    save_metrics_to_postgres,
    visit_postgres_duplication_safety,
)
from datagouvfr_data_pipelines.utils.tasks import clean_up_folder

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="dgv_metrics",
    schedule="15 6 * * *",
    start_date=datetime(2024, 8, 10),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60 * 24),
    max_active_runs=1,
    tags=["dgv", "metrics"],
    default_args=default_args,
):
    _create_metrics_tables = create_metrics_tables()
    _download_catalog = download_catalog()
    _download_log = download_log()
    _process_log = process_log()
    _save_matomo_to_postgres = save_matomo_to_postgres()
    _save_metrics_to_postgres = save_metrics_to_postgres()

    clean_up_folder(TMP_FOLDER, recreate=True) >> _create_metrics_tables

    (
        _create_metrics_tables
        >> _download_catalog
        >> process_matomo()
        >> matomo_postgres_duplication_safety()
        >> _save_matomo_to_postgres
    )

    (
        _create_metrics_tables
        >> ShortCircuitOperator(
            task_id="get_new_logs",
            python_callable=get_new_logs,
            ignore_downstream_trigger_rules=False,
        )
        >> _download_log
    )

    (
        [_download_catalog, _download_log]
        >> _process_log
        >> aggregate_log()
        >> visit_postgres_duplication_safety()
        >> _save_metrics_to_postgres
        >> copy_logs_to_processed_folder()
        >> delete_old_log_files()
    )

    (
        [_save_metrics_to_postgres, _save_matomo_to_postgres]
        >> refresh_materialized_views()
        >> clean_up_folder(TMP_FOLDER)
        >> TriggerDagRunOperator(
            task_id="trigger_tabular_metrics",
            trigger_dag_id="dgv_tabular_metrics",
        )
    )
