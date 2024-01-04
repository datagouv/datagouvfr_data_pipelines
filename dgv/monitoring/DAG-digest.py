from airflow.models import DAG
from datagouvfr_data_pipelines.utils.notebook import execute_and_upload_notebook
from datagouvfr_data_pipelines.utils.mails import send_mail_datagouv
from airflow.operators.bash import BashOperator
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
    check_if_first_day_of_year,
)

DAG_FOLDER = "datagouvfr_data_pipelines/dgv/monitoring/"
DAG_NAME = "dgv_digests"
TMP_FOLDER = AIRFLOW_DAG_TMP + DAG_FOLDER + DAG_NAME
MINIO_PATH = "dgv/"


def get_stats_period(TODAY, period):
    with open(
        AIRFLOW_DAG_TMP + DAG_FOLDER + f"digest_{period}/" + TODAY + "/output/stats.json"
    ) as json_file:
        res = json.load(json_file)
    recap = (
        "- "
        + str(res["stats"]["nb_datasets"])
        + " datasets créés\n- "
        + str(res["stats"]["nb_reuses"])
        + " reuses créées"
    )
    if period == "daily":
        recap += (
            "\n- "
            + str(res["stats"]["nb_orgas"])
            + " orgas créées\n- "
            + str(res["stats"]["nb_discussions"])
            + " discussions créées\n- "
            + str(res["stats"]["nb_users"])
            + " users créés\n"
        )
    return recap


def publish_mattermost_period(ti, **kwargs):
    templates_dict = kwargs.get("templates_dict")
    period = templates_dict["period"]
    report_url = ti.xcom_pull(
        key="report_url", task_ids=f"run_notebook_and_save_to_minio_{period}"
    )
    stats = get_stats_period(templates_dict["TODAY"], period)
    message = f"{period.title()} Digest : {report_url} \n{stats}"
    send_message(message, MATTERMOST_DATAGOUV_ACTIVITES)


def send_email_report_period(ti, **kwargs):
    templates_dict = kwargs.get("templates_dict")
    period = templates_dict["period"]
    report_url = ti.xcom_pull(
        key="report_url", task_ids=f"run_notebook_and_save_to_minio_{period}"
    )
    message = get_stats_period(templates_dict["TODAY"], period) + "<br/><br/>" + report_url
    send_mail_datagouv(
        email_user=SECRET_MAIL_DATAGOUV_BOT_USER,
        email_password=SECRET_MAIL_DATAGOUV_BOT_PASSWORD,
        email_recipients=SECRET_MAIL_DATAGOUV_BOT_RECIPIENTS_PROD,
        subject=f"{period.title()} digest of " + templates_dict["TODAY"],
        message=message,
    )


default_args = {
    # "email": ["geoffrey.aldebert@data.gouv.fr"],
    "email_on_failure": False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id=DAG_NAME,
    schedule_interval="0 6 * * *",
    start_date=days_ago(1),
    dagrun_timeout=timedelta(minutes=60),
    tags=["digest", "daily", "weekly", "monthly", "datagouv"],
    default_args=default_args,
    catchup=False,
) as dag:
    clean_previous_output = BashOperator(
        task_id="clean_previous_outputs",
        bash_command=f"rm -rf {TMP_FOLDER} && mkdir -p {TMP_FOLDER}",
    )

    run_nb_daily = PythonOperator(
        task_id='run_notebook_and_save_to_minio_daily',
        python_callable=execute_and_upload_notebook,
        op_kwargs={
            "input_nb": AIRFLOW_DAG_HOME + DAG_FOLDER + "digest.ipynb",
            "output_nb": "{{ ds }}" + ".ipynb",
            "tmp_path": AIRFLOW_DAG_TMP + DAG_FOLDER + "digest_daily/" + "{{ ds }}" + "/",
            "minio_url": MINIO_URL,
            "minio_bucket": MINIO_BUCKET_DATA_PIPELINE_OPEN,
            "minio_user": SECRET_MINIO_DATA_PIPELINE_USER,
            "minio_password": SECRET_MINIO_DATA_PIPELINE_PASSWORD,
            "minio_output_filepath": MINIO_PATH + "digest_daily/" + "{{ ds }}" + "/",
            "parameters": {
                "WORKING_DIR": AIRFLOW_DAG_HOME,
                "OUTPUT_DATA_FOLDER": AIRFLOW_DAG_TMP
                + DAG_FOLDER
                + "digest_daily/"
                + "{{ ds }}"
                + "/output/",
                "DATE_AIRFLOW": "{{ ds }}",
                "PERIOD_DIGEST": "daily",
            },
        },
    )

    publish_mattermost_daily = PythonOperator(
        task_id="publish_mattermost_daily",
        python_callable=publish_mattermost_period,
        templates_dict={
            "TODAY": "{{ ds }}",
            "period": "daily",
        },
    )

    send_email_report_daily = PythonOperator(
        task_id="send_email_report_daily",
        python_callable=send_email_report_period,
        templates_dict={
            "TODAY": "{{ ds }}",
            "period": "daily",
        },
    )

    check_if_monday = ShortCircuitOperator(
        task_id="check_if_monday", python_callable=check_if_monday
    )

    run_nb_weekly = PythonOperator(
        task_id='run_notebook_and_save_to_minio_weekly',
        python_callable=execute_and_upload_notebook,
        op_kwargs={
            "input_nb": AIRFLOW_DAG_HOME + DAG_FOLDER + "digest.ipynb",
            "output_nb": "{{ ds }}" + ".ipynb",
            "tmp_path": AIRFLOW_DAG_TMP + DAG_FOLDER + "digest_weekly/" + "{{ ds }}" + "/",
            "minio_url": MINIO_URL,
            "minio_bucket": MINIO_BUCKET_DATA_PIPELINE_OPEN,
            "minio_user": SECRET_MINIO_DATA_PIPELINE_USER,
            "minio_password": SECRET_MINIO_DATA_PIPELINE_PASSWORD,
            "minio_output_filepath": MINIO_PATH + "digest_weekly/" + "{{ ds }}" + "/",
            "parameters": {
                "WORKING_DIR": AIRFLOW_DAG_HOME,
                "OUTPUT_DATA_FOLDER": AIRFLOW_DAG_TMP
                + DAG_FOLDER
                + "digest_weekly/"
                + "{{ ds }}"
                + "/output/",
                "DATE_AIRFLOW": "{{ ds }}",
                "PERIOD_DIGEST": "weekly",
            },
        },
    )

    publish_mattermost_weekly = PythonOperator(
        task_id="publish_mattermost_weekly",
        python_callable=publish_mattermost_period,
        templates_dict={
            "TODAY": "{{ ds }}",
            "period": "weekly",
        },
    )

    send_email_report_weekly = PythonOperator(
        task_id="send_email_report_weekly",
        python_callable=send_email_report_period,
        templates_dict={
            "TODAY": "{{ ds }}",
            "period": "daily",
        },
    )

    check_if_first_day_of_month = ShortCircuitOperator(
        task_id="check_if_first_day_of_month",
        python_callable=check_if_first_day_of_month,
    )

    run_nb_monthly = PythonOperator(
        task_id='run_notebook_and_save_to_minio_monthly',
        python_callable=execute_and_upload_notebook,
        op_kwargs={
            "input_nb": AIRFLOW_DAG_HOME + DAG_FOLDER + "digest.ipynb",
            "output_nb": "{{ ds }}" + ".ipynb",
            "tmp_path": AIRFLOW_DAG_TMP + DAG_FOLDER + "digest_monthly/" + "{{ ds }}" + "/",
            "minio_url": MINIO_URL,
            "minio_bucket": MINIO_BUCKET_DATA_PIPELINE_OPEN,
            "minio_user": SECRET_MINIO_DATA_PIPELINE_USER,
            "minio_password": SECRET_MINIO_DATA_PIPELINE_PASSWORD,
            "minio_output_filepath": MINIO_PATH + "digest_monthly/" + "{{ ds }}" + "/",
            "parameters": {
                "WORKING_DIR": AIRFLOW_DAG_HOME,
                "OUTPUT_DATA_FOLDER": AIRFLOW_DAG_TMP
                + DAG_FOLDER
                + "digest_monthly/"
                + "{{ ds }}"
                + "/output/",
                "DATE_AIRFLOW": "{{ ds }}",
                "PERIOD_DIGEST": "monthly",
            },
        }
    )

    publish_mattermost_monthly = PythonOperator(
        task_id="publish_mattermost_monthly",
        python_callable=publish_mattermost_period,
        templates_dict={
            "TODAY": "{{ ds }}",
            "period": "monthly",
        },
    )

    send_email_report_monthly = PythonOperator(
        task_id="send_email_report_monthly",
        python_callable=send_email_report_period,
        templates_dict={
            "TODAY": "{{ ds }}",
            "period": "daily",
        },
    )

    check_if_first_day_of_year = ShortCircuitOperator(
        task_id="check_if_first_day_of_year",
        python_callable=check_if_first_day_of_year,
    )

    run_nb_yearly = PythonOperator(
        task_id='run_notebook_and_save_to_minio_yearly',
        python_callable=execute_and_upload_notebook,
        op_kwargs={
            "input_nb": AIRFLOW_DAG_HOME + DAG_FOLDER + "digest.ipynb",
            "output_nb": "{{ ds }}" + ".ipynb",
            "tmp_path": AIRFLOW_DAG_TMP + DAG_FOLDER + "digest_yearly/" + "{{ ds }}" + "/",
            "minio_url": MINIO_URL,
            "minio_bucket": MINIO_BUCKET_DATA_PIPELINE_OPEN,
            "minio_user": SECRET_MINIO_DATA_PIPELINE_USER,
            "minio_password": SECRET_MINIO_DATA_PIPELINE_PASSWORD,
            "minio_output_filepath": MINIO_PATH + "digest_yearly/" + "{{ ds }}" + "/",
            "parameters": {
                "WORKING_DIR": AIRFLOW_DAG_HOME,
                "OUTPUT_DATA_FOLDER": AIRFLOW_DAG_TMP
                + DAG_FOLDER
                + "digest_yearly/"
                + "{{ ds }}"
                + "/output/",
                "DATE_AIRFLOW": "{{ ds }}",
                "PERIOD_DIGEST": "yearly",
            },
        }
    )

    publish_mattermost_yearly = PythonOperator(
        task_id="publish_mattermost_yearly",
        python_callable=publish_mattermost_period,
        templates_dict={
            "TODAY": "{{ ds }}",
            "period": "yearly",
        },
    )

    send_email_report_yearly = PythonOperator(
        task_id="send_email_report_yearly",
        python_callable=send_email_report_period,
        templates_dict={
            "TODAY": "{{ ds }}",
            "period": "daily",
        },
    )

    run_nb_daily.set_upstream(clean_previous_output)
    publish_mattermost_daily.set_upstream(run_nb_daily)
    send_email_report_daily.set_upstream(run_nb_daily)

    check_if_monday.set_upstream(clean_previous_output)
    run_nb_weekly.set_upstream(check_if_monday)
    publish_mattermost_weekly.set_upstream(run_nb_weekly)
    send_email_report_weekly.set_upstream(run_nb_weekly)

    check_if_first_day_of_month.set_upstream(clean_previous_output)
    run_nb_monthly.set_upstream(check_if_first_day_of_month)
    publish_mattermost_monthly.set_upstream(run_nb_monthly)
    send_email_report_monthly.set_upstream(run_nb_monthly)

    check_if_first_day_of_year.set_upstream(clean_previous_output)
    run_nb_yearly.set_upstream(check_if_first_day_of_year)
    publish_mattermost_yearly.set_upstream(run_nb_yearly)
    send_email_report_yearly.set_upstream(run_nb_yearly)
