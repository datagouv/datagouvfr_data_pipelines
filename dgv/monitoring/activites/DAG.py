from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator

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
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id=DAG_NAME,
    schedule_interval=f"*/{TIME_PERIOD['minutes']} * * * *",
    start_date=datetime(2024, 8, 10),
    dagrun_timeout=timedelta(minutes=60),
    tags=["notification", "hourly", "datagouv", "activite", "schemas"],
    default_args=default_args,
    catchup=False,
) as dag:
    check_new_datasets = PythonOperator(
        task_id="check_new_datasets",
        python_callable=check_new,
        templates_dict={"type": "datasets"},
    )

    check_new_reuses = PythonOperator(
        task_id="check_new_reuses",
        python_callable=check_new,
        templates_dict={"type": "reuses"},
    )

    check_new_organizations = PythonOperator(
        task_id="check_new_organizations",
        python_callable=check_new,
        templates_dict={"type": "organizations"},
    )

    # check_new_dataservices = PythonOperator(
    #     task_id="check_new_dataservices",
    #     python_callable=check_new,
    #     templates_dict={"type": "dataservices"},
    # )

    # check_new_comments = PythonOperator(
    #     task_id="check_new_comments",
    #     python_callable=check_new_comments,
    # )

    send_spam_to_grist = PythonOperator(
        task_id="send_spam_to_grist",
        python_callable=send_spam_to_grist,
    )

    publish_mattermost = PythonOperator(
        task_id="publish_mattermost",
        python_callable=publish_mattermost,
    )

    check_schema = PythonOperator(
        task_id="check_schema",
        python_callable=check_schema,
    )

    # get_inactive_orgas = PythonOperator(
    #     task_id="get_inactive_orgas",
    #     python_callable=get_inactive_orgas,
    # )

    alert_if_awaiting_spam_comments = PythonOperator(
        task_id="alert_if_awaiting_spam_comments",
        python_callable=alert_if_awaiting_spam_comments,
    )

    alert_if_new_reports = PythonOperator(
        task_id="alert_if_new_reports",
        python_callable=alert_if_new_reports,
    )

    publish_mattermost.set_upstream(check_new_datasets)
    publish_mattermost.set_upstream(check_new_reuses)
    publish_mattermost.set_upstream(check_new_organizations)
    # publish_mattermost.set_upstream(check_new_dataservices)

    check_schema.set_upstream(publish_mattermost)

    send_spam_to_grist.set_upstream(check_new_datasets)
    send_spam_to_grist.set_upstream(check_new_reuses)
    send_spam_to_grist.set_upstream(check_new_organizations)
    # publish_mattermost.set_upstream(check_new_dataservices)

    # get_inactive_orgas
    alert_if_awaiting_spam_comments
    alert_if_new_reports
