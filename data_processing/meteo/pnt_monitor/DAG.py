from datetime import timedelta, datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator

from datagouvfr_data_pipelines.data_processing.meteo.pnt_monitor.task_functions import (
    scan_pnt_files,
    notification_mattermost,
    dump_and_send_tree,
    consolidate_logs,
)

DAG_NAME = "data_processing_pnt_monitor"

default_args = {
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_NAME,
    schedule="0 */3 * * *",
    start_date=datetime(2024, 10, 1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=180),
    tags=["data_processing", "meteo"],
    default_args=default_args,
) as dag:
    scan_pnt_files = PythonOperator(
        task_id="scan_pnt_files",
        python_callable=scan_pnt_files,
    )

    notification_mattermost = PythonOperator(
        task_id="notification_mattermost",
        python_callable=notification_mattermost,
    )

    dump_and_send_tree = PythonOperator(
        task_id="dump_and_send_tree",
        python_callable=dump_and_send_tree,
    )

    consolidate_logs = PythonOperator(
        task_id="consolidate_logs",
        python_callable=consolidate_logs,
    )

    notification_mattermost.set_upstream(scan_pnt_files)
    dump_and_send_tree
    consolidate_logs
