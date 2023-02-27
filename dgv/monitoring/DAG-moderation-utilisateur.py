from airflow.models import DAG, Variable
from operators.mattermost import MattermostOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from operators.mail_datagouv import MailDatagouvOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
from dag_datagouv_data_pipelines.config import (
    MATTERMOST_DATAGOUV_ACTIVITES,
    SECRET_MAIL_DATAGOUV_BOT_USER,
    SECRET_MAIL_DATAGOUV_BOT_PASSWORD,
    SECRET_MAIL_DATAGOUV_BOT_RECIPIENTS_PROD,
)
from dag_datagouv_data_pipelines.utils.datagouv import get_last_items

DAG_NAME = 'dgv_moderation_utilisateurs'

TIME_PERIOD = {'hours': 1} 
NB_USERS_THRESHOLD = 25

def check_user_creation(ti):
    # we want everything that happened since this date
    start_date = datetime.now() - timedelta(**TIME_PERIOD)
    end_date = datetime.now()
    users = get_last_items('users', start_date, end_date, date_key='since',)
    ti.xcom_push(key='nb_users', value=str(len(users))) 
    if len(users) > NB_USERS_THRESHOLD:
        return True
    else:
        return False


def publish_mattermost(ti):
    nb_users = ti.xcom_pull(key='nb_users', task_ids='check-user-creation')
    print(nb_users)
    publish_mattermost = MattermostOperator(
            task_id="publish_result",
            mattermost_endpoint=MATTERMOST_DATAGOUV_ACTIVITES,
            text=":warning: Attention, {} utilisateurs ont été créés sur data.gouv.fr dans la dernière heure.".format(nb_users)
    )
    publish_mattermost.execute(dict())


def send_email_report(ti):
    nb_users = ti.xcom_pull(key='nb_users', task_ids='check-user-creation')
    message = "Attention, {} utilisateurs ont été créés sur data.gouv.fr dans la dernière heure.".format(nb_users)
    send_email_report = MailDatagouvOperator(
        task_id="send_email_report", 
        email_user=SECRET_MAIL_DATAGOUV_BOT_USER,
        email_password=SECRET_MAIL_DATAGOUV_BOT_PASSWORD,
        email_recipients=SECRET_MAIL_DATAGOUV_BOT_RECIPIENTS_PROD,
        subject='Moderation users data.gouv',
        message=message
    )
    send_email_report.execute(dict())


default_args = {
   'email': ['geoffrey.aldebert@data.gouv.fr'],
   'email_on_failure': True
}

with DAG(
    dag_id=DAG_NAME,
    schedule_interval='45 * * * *',
    start_date=days_ago(0, hour=1),
    dagrun_timeout=timedelta(minutes=60),
    tags=['moderation','hourly','datagouv'],
    default_args=default_args,
    catchup=False,
) as dag:

    check_user_creation = ShortCircuitOperator(
        task_id="check-user-creation", 
        python_callable=check_user_creation
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
