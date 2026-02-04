from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datagouvfr_data_pipelines.data_processing.assemblee_nationale.petitions.task_functions import (
    DATADIR,
    gather_petitions,
    send_petitions_to_s3,
    publish_on_datagouv,
    send_notification_mattermost,
)

DAG_FOLDER = "datagouvfr_data_pipelines/data_processing/"
DAG_NAME = "data_processing_an_petitions"

with DAG(
    dag_id=DAG_NAME,
    # every monday morning
    schedule="0 2 * * 1",
    start_date=datetime(2025, 7, 29),
    catchup=False,
    dagrun_timeout=timedelta(minutes=240),
    tags=["data_processing", "assemble_nationale", "petitions"],
) as dag:
    clean_previous_outputs = BashOperator(
        task_id="clean_previous_outputs",
        bash_command=f"rm -rf {DATADIR} && mkdir -p {DATADIR}",
    )

    gather_petitions = PythonOperator(
        task_id="gather_petitions",
        python_callable=gather_petitions,
    )

    send_petitions_to_s3 = PythonOperator(
        task_id="send_petitions_to_s3",
        python_callable=send_petitions_to_s3,
    )

    publish_on_datagouv = PythonOperator(
        task_id="publish_on_datagouv",
        python_callable=publish_on_datagouv,
    )

    clean_up = BashOperator(
        task_id="clean_up",
        bash_command=f"rm -rf {DATADIR}",
    )

    send_notification_mattermost = PythonOperator(
        task_id="send_notification_mattermost",
        python_callable=send_notification_mattermost,
    )

    gather_petitions.set_upstream(clean_previous_outputs)
    send_petitions_to_s3.set_upstream(gather_petitions)
    publish_on_datagouv.set_upstream(send_petitions_to_s3)
    clean_up.set_upstream(publish_on_datagouv)
    send_notification_mattermost.set_upstream(clean_up)
