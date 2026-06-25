import json

from airflow.sdk import task
from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
    S3_BUCKET_DATA_PIPELINE_OPEN,
    SECRET_MAIL_DATAGOUV_BOT_PASSWORD,
    SECRET_MAIL_DATAGOUV_BOT_RECIPIENTS_PROD,
    SECRET_MAIL_DATAGOUV_BOT_USER,
    TCHAP_ROOM_ACTIVITES,
    TCHAP_ROOM_MODERATION_NOUVEAUTES,
)
from datagouvfr_data_pipelines.utils.mails import send_mail_datagouv
from datagouvfr_data_pipelines.utils.s3 import S3Client
from datagouvfr_data_pipelines.utils.tchap import send_message

DAG_FOLDER = "datagouvfr_data_pipelines/dgv/monitoring/digest/"
DAG_NAME = "dgv_digests"
S3_PATH = "dgv/"
TMP_FOLDER = AIRFLOW_DAG_TMP + DAG_NAME


def get_stats_period(today: str, period: str, scope: str) -> str | None:
    with open(
        TMP_FOLDER + f"/digest_{period}/{today}/output/stats.json", "r"
    ) as json_file:
        res = json.load(json_file)
    if scope == "api":
        if not (
            res["stats"]["nb_dataservices"]
            or res["stats"]["nb_discussions_dataservices"]
        ):
            # no message if no new API and no comment
            return
        return (
            f"- {res['stats']['nb_dataservices']} APIs créées\n"
            f"- {res['stats']['nb_discussions_dataservices']} discussions sur les APIs\n"
        )
    recap = (
        f"- {res['stats']['nb_datasets']} datasets créés\n"
        f"- {res['stats']['nb_reuses']} reuses créées\n"
        f"- {res['stats']['nb_dataservices']} dataservices créés\n"
    )
    if period == "daily":
        recap += (
            f"- {res['stats']['nb_orgas']} orgas créées\n"
            f"- {res['stats']['nb_discussions']} discussions créées\n"
            f"- {res['stats']['nb_users']} utilisateurs créés"
        )
    return recap


@task()
def publish_period(today: str, period: str, scope: str, **context):
    report_url = context["ti"].xcom_pull(
        key="report_url", task_ids=f"run_notebook_and_save_to_s3_{scope}_{period}"
    )
    stats = get_stats_period(today, period, scope)
    if not stats:
        return
    message = f"{period.title()} Digest : {report_url} \n\n{stats}"
    room_id = (
        TCHAP_ROOM_ACTIVITES if scope == "general" else TCHAP_ROOM_MODERATION_NOUVEAUTES
    )
    send_message(message, room_id)


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


@task()
def clean_old_daily(today: str):
    # purging daily files, keeping the rest as history
    client = S3Client(bucket=S3_BUCKET_DATA_PIPELINE_OPEN)
    for folder in client.get_folders_from_prefix(
        S3_PATH + "digest_daily/", ignore_airflow_env=True
    ):
        date = folder.split("/")[-2]  # by construction
        if date < today:
            for file in client.get_files_from_prefix(folder, ignore_airflow_env=True):
                client.delete_file(file)
