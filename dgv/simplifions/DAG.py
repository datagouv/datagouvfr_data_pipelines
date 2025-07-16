from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator

from datagouvfr_data_pipelines.dgv.simplifions.task_functions import (
    get_and_format_grist_data,
    update_topics,
    update_topics_references,
)

DAG_NAME = "dgv_simplifions"

default_args = {
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_NAME,
    schedule_interval="*/30 * * * *",
    start_date=datetime(2024, 10, 1),
    dagrun_timeout=timedelta(minutes=60),
    tags=["simplifions"],
    default_args=default_args,
    catchup=False,
) as dag:

    get_and_format_grist_data = PythonOperator(
        task_id="get_and_format_grist_data",
        python_callable=get_and_format_grist_data,
    )

    update_topics = PythonOperator(
        task_id="update_topics",
        python_callable=update_topics,
    )

    update_topics_references = PythonOperator(
        task_id="update_topics_references",
        python_callable=update_topics_references,
    )

    update_topics.set_upstream(get_and_format_grist_data)
    update_topics_references.set_upstream(update_topics)
