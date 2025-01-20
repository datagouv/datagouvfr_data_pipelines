from datetime import timedelta, datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from datagouvfr_data_pipelines.dgv.monitoring.hvd.task_functions import (
    DAG_NAME,
    DATADIR,
    get_hvd,
    send_to_minio,
    publish_mattermost,
)

default_args = {
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_NAME,
    schedule_interval="0 4 * * 1",
    start_date=datetime(2024, 6, 1),
    dagrun_timeout=timedelta(minutes=60),
    tags=["hvd", "datagouv"],
    default_args=default_args,
    catchup=False,
) as dag:

    clean_previous_outputs = BashOperator(
        task_id="clean_previous_outputs",
        bash_command=f"rm -rf {DATADIR} && mkdir -p {DATADIR}",
    ),

    get_hvd = PythonOperator(
        task_id="get_hvd",
        python_callable=get_hvd
    )

    send_to_minio = PythonOperator(
        task_id="send_to_minio",
        python_callable=send_to_minio
    )

    publish_mattermost = PythonOperator(
        task_id="publish_mattermost",
        python_callable=publish_mattermost,
    )

    get_hvd.set_upstream(clean_previous_outputs)
    send_to_minio.set_upstream(get_hvd)
    publish_mattermost.set_upstream(send_to_minio)
