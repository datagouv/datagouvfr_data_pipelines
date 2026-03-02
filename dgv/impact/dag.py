from datetime import datetime, timedelta

from airflow import DAG
from datagouvfr_data_pipelines.dgv.impact.task_functions import (
    TMP_FOLDER,
    calculate_quality_score,
    calculate_time_for_legitimate_answer,
    gather_kpis,
    get_discoverability,
    get_quality_reuses,
    publish_datagouv,
    send_notification_mattermost,
    send_stats_to_s3,
)
from datagouvfr_data_pipelines.utils.tasks import clean_up_folder

DAG_FOLDER = "datagouvfr_data_pipelines/dgv/impact/"
DAG_NAME = "dgv_impact"

default_args = {
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_NAME,
    schedule="0 5 1 * *",
    start_date=datetime(2023, 10, 15),
    catchup=False,
    dagrun_timeout=timedelta(minutes=120),
    tags=["datagouv", "impact", "metrics"],
    default_args=default_args,
):
    (
        clean_up_folder(TMP_FOLDER, recreate=True)
        >> [
            calculate_quality_score(),
            calculate_time_for_legitimate_answer(),
            get_quality_reuses(),
            get_discoverability(),
        ]
        >> gather_kpis()
        >> send_stats_to_s3()
        >> publish_datagouv(DAG_FOLDER)
        >> clean_up_folder(TMP_FOLDER)
        >> send_notification_mattermost(DAG_FOLDER)
    )
