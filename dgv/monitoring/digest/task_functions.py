from airflow.decorators import task

from datagouvfr_data_pipelines.config import (
    MATTERMOST_DATAGOUV_ACTIVITES,
    MATTERMOST_DATASERVICES_ONLY,
    SECRET_MAIL_DATAGOUV_BOT_USER,
    SECRET_MAIL_DATAGOUV_BOT_PASSWORD,
    SECRET_MAIL_DATAGOUV_BOT_RECIPIENTS_PROD,
)
from datagouvfr_data_pipelines.dgv.monitoring.digest.utils import get_stats_period
from datagouvfr_data_pipelines.utils.mails import send_mail_datagouv
from datagouvfr_data_pipelines.utils.mattermost import send_message


@task()
def publish_mattermost_period(today: str, period: str, scope: str, **context):
    report_url = context["ti"].xcom_pull(
        key="report_url", task_ids=f"run_notebook_and_save_to_s3_{scope}_{period}"
    )
    stats = get_stats_period(today, period, scope)
    if not stats:
        return
    message = f"{period.title()} Digest : {report_url} \n{stats}"
    channel = (
        MATTERMOST_DATAGOUV_ACTIVITES
        if scope == "general"
        else MATTERMOST_DATASERVICES_ONLY
    )
    send_message(message, channel)


@task()
def send_email_report_period(today: str, period: str, scope: str, **context):
    report_url = context["ti"].xcom_pull(
        key="report_url", task_ids=f"run_notebook_and_save_to_s3_{scope}_{period}"
    )
    stats = get_stats_period(today, period, scope)
    if not stats:
        return
    message = stats + "<br/><br/>" + report_url
    send_mail_datagouv(
        email_user=SECRET_MAIL_DATAGOUV_BOT_USER,
        email_password=SECRET_MAIL_DATAGOUV_BOT_PASSWORD,
        email_recipients=SECRET_MAIL_DATAGOUV_BOT_RECIPIENTS_PROD,
        subject=f"{period.title()} digest of " + today,
        message=message,
    )
