from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from datagouvfr_data_pipelines.data_processing.meteo.pnt_monitor.task_functions import (
    scan_pnt_files,
    notification_mattermost,
)

DAG_NAME = 'data_processing_pnt_monitor'

default_args = {
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_NAME,
    schedule_interval='0 9 * * *',
    start_date=days_ago(1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=15),
    tags=["data_processing", "meteo"],
    default_args=default_args,
) as dag:

    scan_pnt_files = PythonOperator(
        task_id='scan_pnt_files',
        python_callable=scan_pnt_files,
    )

    notification_mattermost = PythonOperator(
        task_id='notification_mattermost',
        python_callable=notification_mattermost,
    )

    notification_mattermost.set_upstream(scan_pnt_files)
