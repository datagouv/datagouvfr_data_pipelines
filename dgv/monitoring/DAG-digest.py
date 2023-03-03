from airflow.models import DAG
from operators.papermill_minio import PapermillMinioOperator
from operators.mail_datagouv import MailDatagouvOperator
from operators.clean_folder import CleanFolderOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import json
from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_HOME,
    AIRFLOW_DAG_TMP,
    MATTERMOST_DATAGOUV_ACTIVITES,
    SECRET_MAIL_DATAGOUV_BOT_USER,
    SECRET_MAIL_DATAGOUV_BOT_PASSWORD,
    SECRET_MAIL_DATAGOUV_BOT_RECIPIENTS_PROD,
    MINIO_URL,
    MINIO_BUCKET_DATA_PIPELINE_OPEN,
    SECRET_MINIO_DATA_PIPELINE_USER,
    SECRET_MINIO_DATA_PIPELINE_PASSWORD,
)
from datagouvfr_data_pipelines.utils.mattermost import send_message
from datagouvfr_data_pipelines.utils.utils import (
    check_if_monday,
    check_if_first_day_of_month,
)

DAG_FOLDER = "datagouvfr_data_pipelines/dgv/monitoring/"
DAG_NAME = "dgv_digests"
MINIO_PATH = "dgv/"


def get_stats_daily(TODAY):
    with open(
        AIRFLOW_DAG_TMP + DAG_FOLDER + "digest_daily/" + TODAY + "/output/stats.json"
    ) as json_file:
        res = json.load(json_file)
    recap = (
        "- "
        + str(res["stats"]["nb_datasets"])
        + " datasets créés\n- "
        + str(res["stats"]["nb_orgas"])
        + " orgas créées\n- "
        + str(res["stats"]["nb_reuses"])
        + " reuses créées\n- "
        + str(res["stats"]["nb_discussions"])
        + " discussions créées\n- "
        + str(res["stats"]["nb_users"])
        + " users créés\n"
    )
    return recap


def publish_mattermost_daily(ti, **kwargs):
    templates_dict = kwargs.get("templates_dict")
    report_url = ti.xcom_pull(
        key="report_url", task_ids="run_notebook_and_save_to_minio_daily"
    )
    stats = get_stats_daily(templates_dict["TODAY"])
    message = f"Daily Digest : {report_url} \n{stats}"
    send_message(message, MATTERMOST_DATAGOUV_ACTIVITES)


def send_email_report_daily(ti, **kwargs):
    templates_dict = kwargs.get("templates_dict")
    report_url = ti.xcom_pull(
        key="report_url", task_ids="run_notebook_and_save_to_minio_daily"
    )
    message = get_stats_daily(templates_dict["TODAY"]) + "<br/><br/>" + report_url
    send_email_report = MailDatagouvOperator(
        task_id="send_email_report",
        email_user=SECRET_MAIL_DATAGOUV_BOT_USER,
        email_password=SECRET_MAIL_DATAGOUV_BOT_PASSWORD,
        email_recipients=SECRET_MAIL_DATAGOUV_BOT_RECIPIENTS_PROD,
        subject="Daily digest of " + templates_dict["TODAY"],
        message=message,
    )
    send_email_report.execute(dict())


def get_stats_weekly(TODAY):
    with open(
        AIRFLOW_DAG_TMP + DAG_FOLDER + "digest_weekly/" + TODAY + "/output/stats.json"
    ) as json_file:
        res = json.load(json_file)
    recap = (
        "- "
        + str(res["stats"]["nb_datasets"])
        + " datasets créés\n- "
        + str(res["stats"]["nb_reuses"])
        + " reuses créées\n"
    )
    return recap


def publish_mattermost_weekly(ti, **kwargs):
    templates_dict = kwargs.get("templates_dict")
    report_url = ti.xcom_pull(
        key="report_url", task_ids="run_notebook_and_save_to_minio_weekly"
    )
    stats = get_stats_weekly(templates_dict["TODAY"])
    message = f"Weekly Digest : {report_url}\n{stats}"
    image_url = (
        f"https://{MINIO_URL}/{MINIO_BUCKET_DATA_PIPELINE_OPEN}/{MINIO_PATH}"
        f"digest_weekly/{templates_dict['TODAY']}/output/weekly-graph.png",
    )
    send_message(message, MATTERMOST_DATAGOUV_ACTIVITES, image_url)


def send_email_report_weekly(ti, **kwargs):
    templates_dict = kwargs.get("templates_dict")
    report_url = ti.xcom_pull(
        key="report_url", task_ids="run_notebook_and_save_to_minio_weekly"
    )
    message = get_stats_weekly(templates_dict["TODAY"]) + "<br/><br/>" + report_url
    send_email_report = MailDatagouvOperator(
        task_id="send_email_report",
        email_user=SECRET_MAIL_DATAGOUV_BOT_USER,
        email_password=SECRET_MAIL_DATAGOUV_BOT_PASSWORD,
        email_recipients=SECRET_MAIL_DATAGOUV_BOT_RECIPIENTS_PROD,
        subject="Weekly digest of " + templates_dict["TODAY"],
        message=message,
    )
    send_email_report.execute(dict())


def get_stats_monthly(TODAY):
    with open(
        AIRFLOW_DAG_TMP + DAG_FOLDER + "digest_monthly/" + TODAY + "/output/stats.json"
    ) as json_file:
        res = json.load(json_file)
    recap = (
        "- "
        + str(res["stats"]["nb_datasets"])
        + " datasets créés\n- "
        + str(res["stats"]["nb_reuses"])
        + " reuses créées\n"
    )
    return recap


def publish_mattermost_monthly(ti, **kwargs):
    templates_dict = kwargs.get("templates_dict")
    report_url = ti.xcom_pull(
        key="report_url", task_ids="run_notebook_and_save_to_minio_monthly"
    )
    stats = get_stats_monthly(templates_dict["TODAY"])
    message = f"Monthly Digest : {report_url} \n{stats}"
    image_url = (
        f"https://{MINIO_URL}/{MINIO_BUCKET_DATA_PIPELINE_OPEN}/{MINIO_PATH}"
        f"digest_monthly/{templates_dict['TODAY']}/output/monthly-graph.png"
    )
    send_message(message, MATTERMOST_DATAGOUV_ACTIVITES, image_url)


def send_email_report_monthly(ti, **kwargs):
    templates_dict = kwargs.get("templates_dict")
    report_url = ti.xcom_pull(
        key="report_url", task_ids="run_notebook_and_save_to_minio_monthly"
    )
    message = get_stats_monthly(templates_dict["TODAY"]) + "<br/><br/>" + report_url
    send_email_report = MailDatagouvOperator(
        task_id="send_email_report",
        email_user=SECRET_MAIL_DATAGOUV_BOT_USER,
        email_password=SECRET_MAIL_DATAGOUV_BOT_PASSWORD,
        email_recipients=SECRET_MAIL_DATAGOUV_BOT_RECIPIENTS_PROD,
        subject="Monthly digest of " + templates_dict["TODAY"][:7],
        message=message,
    )
    send_email_report.execute(dict())


default_args = {"email": ["geoffrey.aldebert@data.gouv.fr"], "email_on_failure": True}

with DAG(
    dag_id=DAG_NAME,
    schedule_interval="0 6 * * *",
    start_date=days_ago(1),
    dagrun_timeout=timedelta(minutes=60),
    tags=["digest", "daily", "weekly", "monthly", "datagouv"],
    default_args=default_args,
    catchup=False,
) as dag:
    clean_previous_outputs = CleanFolderOperator(
        task_id="clean_previous_outputs",
        folder_path=AIRFLOW_DAG_TMP + DAG_FOLDER + DAG_NAME,
    )

    run_nb_daily = PapermillMinioOperator(
        task_id="run_notebook_and_save_to_minio_daily",
        input_nb=AIRFLOW_DAG_HOME + DAG_FOLDER + "digest.ipynb",
        output_nb="{{ ds }}" + ".ipynb",
        tmp_path=AIRFLOW_DAG_TMP + DAG_FOLDER + "digest_daily/" + "{{ ds }}" + "/",
        minio_url=MINIO_URL,
        minio_bucket=MINIO_BUCKET_DATA_PIPELINE_OPEN,
        minio_user=SECRET_MINIO_DATA_PIPELINE_USER,
        minio_password=SECRET_MINIO_DATA_PIPELINE_PASSWORD,
        minio_output_filepath=MINIO_PATH + "digest_daily/" + "{{ ds }}" + "/",
        parameters={
            "msgs": "Ran from Airflow " + "{{ ds }}" + "!",
            "WORKING_DIR": AIRFLOW_DAG_HOME,
            "OUTPUT_DATA_FOLDER": AIRFLOW_DAG_TMP
            + DAG_FOLDER
            + "digest_daily/"
            + "{{ ds }}"
            + "/output/",
            "DATE_AIRFLOW": "{{ ds }}",
            "PERIOD_DIGEST": "daily",
        },
    )

    publish_mattermost_daily = PythonOperator(
        task_id="publish_mattermost_daily",
        python_callable=publish_mattermost_daily,
        templates_dict={"TODAY": "{{ ds }}"},
    )

    send_email_report_daily = PythonOperator(
        task_id="send_email_report_daily",
        python_callable=send_email_report_daily,
        templates_dict={"TODAY": "{{ ds }}"},
    )

    check_if_monday = ShortCircuitOperator(
        task_id="check_if_monday", python_callable=check_if_monday
    )

    run_nb_weekly = PapermillMinioOperator(
        task_id="run_notebook_and_save_to_minio_weekly",
        input_nb=AIRFLOW_DAG_HOME + DAG_FOLDER + "digest.ipynb",
        output_nb="{{ ds }}" + ".ipynb",
        tmp_path=AIRFLOW_DAG_TMP + DAG_FOLDER + "digest_weekly/" + "{{ ds }}" + "/",
        minio_url=MINIO_URL,
        minio_bucket=MINIO_BUCKET_DATA_PIPELINE_OPEN,
        minio_user=SECRET_MINIO_DATA_PIPELINE_USER,
        minio_password=SECRET_MINIO_DATA_PIPELINE_PASSWORD,
        minio_output_filepath=MINIO_PATH + "digest_weekly/" + "{{ ds }}" + "/",
        parameters={
            "msgs": "Ran from Airflow " + "{{ ds }}" + "!",
            "WORKING_DIR": AIRFLOW_DAG_HOME,
            "OUTPUT_DATA_FOLDER": AIRFLOW_DAG_TMP
            + DAG_FOLDER
            + "digest_weekly/"
            + "{{ ds }}"
            + "/output/",
            "DATE_AIRFLOW": "{{ ds }}",
            "PERIOD_DIGEST": "weekly",
        },
    )

    publish_mattermost_weekly = PythonOperator(
        task_id="publish_mattermost_weekly",
        python_callable=publish_mattermost_weekly,
        templates_dict={"TODAY": "{{ ds }}"},
    )

    send_email_report_weekly = PythonOperator(
        task_id="send_email_report_weekly",
        python_callable=send_email_report_weekly,
        templates_dict={"TODAY": "{{ ds }}"},
    )

    check_if_first_day_of_month = ShortCircuitOperator(
        task_id="check_if_first_day_of_month",
        python_callable=check_if_first_day_of_month,
    )

    run_nb_monthly = PapermillMinioOperator(
        task_id="run_notebook_and_save_to_minio_monthly",
        input_nb=AIRFLOW_DAG_HOME + DAG_FOLDER + "digest.ipynb",
        output_nb="{{ ds }}" + ".ipynb",
        tmp_path=AIRFLOW_DAG_TMP + DAG_FOLDER + "digest_monthly/" + "{{ ds }}" + "/",
        minio_url=MINIO_URL,
        minio_bucket=MINIO_BUCKET_DATA_PIPELINE_OPEN,
        minio_user=SECRET_MINIO_DATA_PIPELINE_USER,
        minio_password=SECRET_MINIO_DATA_PIPELINE_PASSWORD,
        minio_output_filepath=MINIO_PATH + "digest_monthly/" + "{{ ds }}" + "/",
        parameters={
            "msgs": "Ran from Airflow " + "{{ ds }}" + "!",
            "WORKING_DIR": AIRFLOW_DAG_HOME,
            "OUTPUT_DATA_FOLDER": AIRFLOW_DAG_TMP
            + DAG_FOLDER
            + "digest_monthly/"
            + "{{ ds }}"
            + "/output/",
            "DATE_AIRFLOW": "{{ ds }}",
            "PERIOD_DIGEST": "monthly",
        },
    )

    publish_mattermost_monthly = PythonOperator(
        task_id="publish_mattermost_monthly",
        python_callable=publish_mattermost_monthly,
        templates_dict={"TODAY": "{{ ds }}"},
    )

    send_email_report_monthly = PythonOperator(
        task_id="send_email_report_monthly",
        python_callable=send_email_report_monthly,
        templates_dict={"TODAY": "{{ ds }}"},
    )

    run_nb_daily.set_upstream(clean_previous_outputs)
    publish_mattermost_daily.set_upstream(run_nb_daily)
    send_email_report_daily.set_upstream(run_nb_daily)

    check_if_monday.set_upstream(send_email_report_daily)
    check_if_monday.set_upstream(publish_mattermost_daily)
    run_nb_weekly.set_upstream(check_if_monday)
    publish_mattermost_weekly.set_upstream(run_nb_weekly)
    send_email_report_weekly.set_upstream(run_nb_weekly)

    check_if_first_day_of_month.set_upstream(send_email_report_daily)
    check_if_first_day_of_month.set_upstream(publish_mattermost_daily)
    run_nb_monthly.set_upstream(check_if_first_day_of_month)
    publish_mattermost_monthly.set_upstream(run_nb_monthly)
    send_email_report_monthly.set_upstream(run_nb_monthly)
