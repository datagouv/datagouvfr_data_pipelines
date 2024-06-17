from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
)
from datagouvfr_data_pipelines.data_processing.stats_meteo.task_functions import (
    gather_meteo_stats,
    send_to_minio,
    send_notification,
)

TMP_FOLDER = f"{AIRFLOW_DAG_TMP}stats_meteo/"
DAG_NAME = "data_processing_stats_meteo"
DATADIR = f"{TMP_FOLDER}data"

default_args = {}

with DAG(
    dag_id=DAG_NAME,
    schedule_interval='0 4 1 * *',
    start_date=datetime(2024, 6, 1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=20),
    tags=["data_processing", "meteo"],
    default_args=default_args,
) as dag:

    clean_previous_outputs = BashOperator(
        task_id="clean_previous_outputs",
        bash_command=f"rm -rf {TMP_FOLDER} && mkdir -p {DATADIR}",
    )

    gather_meteo_stats = PythonOperator(
        task_id='gather_meteo_stats',
        python_callable=gather_meteo_stats,
    )

    send_to_minio = PythonOperator(
        task_id='send_to_minio',
        python_callable=send_to_minio,
    )

    send_notification = PythonOperator(
        task_id='send_notification',
        python_callable=send_notification,
    )

    gather_meteo_stats.set_upstream(clean_previous_outputs)
    send_to_minio.set_upstream(gather_meteo_stats)
    send_notification.set_upstream(send_to_minio)
