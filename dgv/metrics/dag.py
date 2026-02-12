from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule

from datagouvfr_data_pipelines.dgv.metrics.task import (
    create_metrics_tables,
    get_new_logs,
    download_catalog,
    download_log,
    matomo_postgres_duplication_safety,
    visit_postgres_duplication_safety,
    process_log,
    aggregate_log,
    save_metrics_to_postgres,
    copy_logs_to_processed_folder,
    refresh_materialized_views,
    process_matomo,
    save_matomo_to_postgres,
    TMP_FOLDER,
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
    )

    (
        [_save_metrics_to_postgres, _save_matomo_to_postgres]
        >> PythonOperator(
            task_id="refresh_materialized_views",
            python_callable=refresh_materialized_views,
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        )
        >> clean_up_folder(TMP_FOLDER)
        >> TriggerDagRunOperator(
            task_id="trigger_tabular_metrics",
            trigger_dag_id="dgv_tabular_metrics",
        )
    )
