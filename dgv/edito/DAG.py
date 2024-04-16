from datetime import timedelta
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from datagouvfr_data_pipelines.dgv.edito.task_functions import (
    create_edito_post,
    publish_mattermost,
)

DAG_NAME = "dgv_edito_post_and_tweet"

default_args = {"email": ["geoffrey.aldebert@data.gouv.fr"], "email_on_failure": True}

with DAG(
    dag_id=DAG_NAME,
    schedule_interval="0 8 1 * *",
    start_date=days_ago(31),
    dagrun_timeout=timedelta(minutes=60),
    tags=["edito", "mattermost", "post", "twitter"],
    default_args=default_args,
    catchup=False,
) as dag:
    edito = PythonOperator(
        task_id="create_edito_post",
        python_callable=create_edito_post,
    )

    mattermost = PythonOperator(
        task_id="publish_mattermost",
        python_callable=publish_mattermost
    )

    mattermost.set_upstream(edito)
