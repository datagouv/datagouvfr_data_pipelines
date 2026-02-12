from datetime import datetime, timedelta

from airflow.decorators import task
from airflow.models import DAG

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
    tags=["notification", "hourly", "datagouv", "activite", "schemas"],
    default_args=default_args,
    catchup=False,
):

    @task()
    def check_new_datasets(**context):
        return check_new("datasets", context["ti"])

    @task()
    def check_new_reuses(**context):
        return check_new("reuses", context["ti"])

    @task()
    def check_new_dataservices(**context):
        return check_new("dataservices", context["ti"])

    @task()
    def check_new_organizations(**context):
        return check_new("organizations", context["ti"])

    alert_if_awaiting_spam_comments()
    alert_if_new_reports()
    # get_inactive_orgas()

    checks = [
        check_new_datasets(),
        check_new_reuses(),
        check_new_dataservices(),
        check_new_organizations(),
    ]

    checks >> send_spam_to_grist()
    checks >> publish_mattermost() >> check_schema()
