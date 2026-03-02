from datetime import datetime, timedelta

from airflow import DAG
from datagouvfr_data_pipelines.dgv.edito.task_functions import (
    create_edito_post,
    publish_mattermost,
)

default_args = {
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dgv_edito_post_and_tweet",
    schedule="0 8 1 * *",
    start_date=datetime(2024, 8, 10),
    dagrun_timeout=timedelta(minutes=60),
    tags=["edito", "mattermost", "post", "twitter"],
    default_args=default_args,
    catchup=False,
):
    create_edito_post() >> publish_mattermost()
