from datetime import timedelta, datetime
from airflow.models import DAG

from datagouvfr_data_pipelines.data_processing.meteo.pnt_monitor.task_functions import (
    scan_pnt_files,
    notification_mattermost,
    dump_and_send_tree,
    consolidate_logs,
)

default_args = {
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="data_processing_pnt_monitor",
    schedule="0 */3 * * *",
    start_date=datetime(2024, 10, 1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=180),
    tags=["data_processing", "meteo", "pnt"],
    default_args=default_args,
):
    scan_pnt_files() >> notification_mattermost()
    dump_and_send_tree()
    consolidate_logs()
