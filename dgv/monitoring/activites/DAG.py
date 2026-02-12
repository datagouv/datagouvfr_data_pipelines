from datetime import datetime, timedelta

from airflow import DAG

from datagouvfr_data_pipelines.dgv.monitoring.activites.task_functions import (
    DAG_NAME,
    TIME_PERIOD,
    check_new,
    send_spam_to_grist,
    publish_mattermost,
    check_schema,
    alert_if_awaiting_spam_comments,
    alert_if_new_reports,
)

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id=DAG_NAME,
    schedule=f"*/{TIME_PERIOD['minutes']} * * * *",
    start_date=datetime(2024, 8, 10),
    dagrun_timeout=timedelta(minutes=60),
    tags=["notification", "datagouv", "activite", "schemas"],
    default_args=default_args,
    catchup=False,
):

    alert_if_awaiting_spam_comments()
    alert_if_new_reports()
    # get_inactive_orgas()

    checks = [
        check_new.override(task_id=f"check_new_{object_type}")(object_type)
        for object_type in [
            "datasets",
            "reuses",
            "dataservices",
            "organizations",
        ]
    ]

    checks >> send_spam_to_grist()
    checks >> publish_mattermost() >> check_schema()
