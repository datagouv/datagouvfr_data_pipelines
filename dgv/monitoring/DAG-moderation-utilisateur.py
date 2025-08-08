from datetime import timedelta, datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator

from datagouvfr_data_pipelines.config import (
    MATTERMOST_MODERATION_NOUVEAUTES,
    SECRET_MAIL_DATAGOUV_BOT_USER,
    SECRET_MAIL_DATAGOUV_BOT_PASSWORD,
    SECRET_MAIL_DATAGOUV_BOT_RECIPIENTS_PROD,
)
from datagouvfr_data_pipelines.utils.mattermost import send_message
from datagouvfr_data_pipelines.utils.datagouv import get_last_items
from datagouvfr_data_pipelines.utils.mails import send_mail_datagouv

DAG_NAME = "dgv_moderation_utilisateurs"

TIME_PERIOD = {"hours": 1}
NB_USERS_THRESHOLD = 25


def check_user_creation(ti):
    # we want everything that happened since this date
    start_date = datetime.now() - timedelta(**TIME_PERIOD)
    end_date = datetime.now()
    users = get_last_items(
        "users",
        start_date,
        end_date,
        date_key="since",
    )
    ti.xcom_push(key="nb_users", value=str(len(users)))
    if len(users) > NB_USERS_THRESHOLD:
        return True
    else:
        return False


def publish_mattermost(ti):
    nb_users = ti.xcom_pull(key="nb_users", task_ids="check-user-creation")
    message = (
        f":warning: Attention, {nb_users} utilisateurs ont été créés "
        "sur data.gouv.fr dans la dernière heure."
    )
    send_message(message, MATTERMOST_MODERATION_NOUVEAUTES)


def send_email_report(ti):
    nb_users = ti.xcom_pull(key="nb_users", task_ids="check-user-creation")
    message = "Attention, {} utilisateurs ont été créés sur data.gouv.fr dans la dernière heure.".format(
        nb_users
    )
    send_mail_datagouv(
        email_user=SECRET_MAIL_DATAGOUV_BOT_USER,
        email_password=SECRET_MAIL_DATAGOUV_BOT_PASSWORD,
        email_recipients=SECRET_MAIL_DATAGOUV_BOT_RECIPIENTS_PROD,
        subject="Moderation users data.gouv",
        message=message,
    )


default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id=DAG_NAME,
    schedule_interval="45 * * * *",
    start_date=datetime(2024, 8, 10),
    dagrun_timeout=timedelta(minutes=60),
    tags=["moderation", "hourly", "datagouv"],
    default_args=default_args,
    catchup=False,
) as dag:
    check_user_creation = ShortCircuitOperator(
        task_id="check-user-creation", python_callable=check_user_creation
    )

    publish_mattermost = PythonOperator(
        task_id="publish_mattermost",
        python_callable=publish_mattermost,
    )

    send_email_report = PythonOperator(
        task_id="send_email_report",
        python_callable=send_email_report,
    )

    check_user_creation >> [publish_mattermost, send_email_report]
