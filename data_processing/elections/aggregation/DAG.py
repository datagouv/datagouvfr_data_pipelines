from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datagouvfr_data_pipelines.data_processing.elections.aggregation.task_functions import (
    DATADIR,
    process_election_data,
    send_results_to_s3,
    publish_results_elections,
    send_notification,
)

DAG_FOLDER = "datagouvfr_data_pipelines/data_processing/"
DAG_NAME = "data_processing_elections"

default_args = {
    "email": ["pierlou.ramade@data.gouv.fr", "geoffrey.aldebert@data.gouv.fr"],
    "email_on_failure": False,
}

with DAG(
    dag_id=DAG_NAME,
    schedule="15 7 1 1 *",
    start_date=datetime(2024, 8, 10),
    catchup=False,
    dagrun_timeout=timedelta(minutes=240),
    tags=["data_processing", "election", "presidentielle", "legislative"],
    default_args=default_args,
) as dag:
    clean_previous_outputs = BashOperator(
        task_id="clean_previous_outputs",
        bash_command=f"rm -rf {DATADIR} && mkdir -p {DATADIR}",
    )

    process_election_data = PythonOperator(
        task_id="process_election_data",
        python_callable=process_election_data,
    )

    send_results_to_s3 = PythonOperator(
        task_id="send_results_to_s3",
        python_callable=send_results_to_s3,
    )

    publish_results_elections = PythonOperator(
        task_id="publish_results_elections",
        python_callable=publish_results_elections,
    )

    clean_up = BashOperator(
        task_id="clean_up",
        bash_command=f"rm -rf {DATADIR}",
    )

    send_notification = PythonOperator(
        task_id="send_notification",
        python_callable=send_notification,
    )

    process_election_data.set_upstream(clean_previous_outputs)
    send_results_to_s3.set_upstream(process_election_data)
    publish_results_elections.set_upstream(send_results_to_s3)
    clean_up.set_upstream(publish_results_elections)
    send_notification.set_upstream(clean_up)
