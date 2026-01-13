from datetime import datetime, timedelta
import json
from collections import defaultdict
from airflow.models import DAG
from datagouvfr_data_pipelines.utils.notebook import execute_and_upload_notebook
from datagouvfr_data_pipelines.utils.mails import send_mail_datagouv
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_HOME,
    AIRFLOW_DAG_TMP,
    MATTERMOST_DATAGOUV_ACTIVITES,
    MATTERMOST_DATASERVICES_ONLY,
    SECRET_MAIL_DATAGOUV_BOT_USER,
    SECRET_MAIL_DATAGOUV_BOT_PASSWORD,
    SECRET_MAIL_DATAGOUV_BOT_RECIPIENTS_PROD,
    S3_URL,
    S3_BUCKET_DATA_PIPELINE_OPEN,
    SECRET_S3_DATA_PIPELINE_USER,
    SECRET_S3_DATA_PIPELINE_PASSWORD,
)
from datagouvfr_data_pipelines.utils.mattermost import send_message
from datagouvfr_data_pipelines.utils.utils import (
    check_if_monday,
    check_if_first_day_of_month,
    check_if_first_day_of_year,
)

DAG_FOLDER = "datagouvfr_data_pipelines/dgv/monitoring/digest/"
DAG_NAME = "dgv_digests"
TMP_FOLDER = AIRFLOW_DAG_TMP + DAG_NAME
S3_PATH = "dgv/"
today = datetime.today().strftime("%Y-%m-%d")


def get_stats_period(TODAY, period, scope):
    with open(
        TMP_FOLDER + f"/digest_{period}/{TODAY}/output/stats.json", "r"
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


def publish_mattermost_period(ti, **kwargs):
    templates_dict = kwargs.get("templates_dict")
    period = templates_dict["period"]
    scope = templates_dict["scope"]
    report_url = ti.xcom_pull(
        key="report_url", task_ids=f"run_notebook_and_save_to_s3_{scope}_{period}"
    )
    stats = get_stats_period(templates_dict["TODAY"], period, scope)
    if not stats:
        return
    message = f"{period.title()} Digest : {report_url} \n{stats}"
    channel = (
        MATTERMOST_DATAGOUV_ACTIVITES
        if scope == "general"
        else MATTERMOST_DATASERVICES_ONLY
    )
    send_message(message, channel)


def send_email_report_period(ti, **kwargs):
    templates_dict = kwargs.get("templates_dict")
    period = templates_dict["period"]
    scope = templates_dict["scope"]
    report_url = ti.xcom_pull(
        key="report_url", task_ids=f"run_notebook_and_save_to_s3_{scope}_{period}"
    )
    message = (
        get_stats_period(templates_dict["TODAY"], period, scope)
        + "<br/><br/>"
        + report_url
    )
    send_mail_datagouv(
        email_user=SECRET_MAIL_DATAGOUV_BOT_USER,
        email_password=SECRET_MAIL_DATAGOUV_BOT_PASSWORD,
        email_recipients=SECRET_MAIL_DATAGOUV_BOT_RECIPIENTS_PROD,
        subject=f"{period.title()} digest of " + templates_dict["TODAY"],
        message=message,
    )


default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id=DAG_NAME,
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 8, 10),
    dagrun_timeout=timedelta(minutes=60),
    tags=["digest", "daily", "weekly", "monthly", "yearly", "datagouv"],
    default_args=default_args,
    catchup=False,
) as dag:
    clean_previous_output = BashOperator(
        task_id="clean_previous_outputs",
        bash_command=f"rm -rf {TMP_FOLDER} && mkdir -p {TMP_FOLDER}",
    )

    clean_up = BashOperator(
        task_id="clean_up",
        bash_command=f"rm -rf {TMP_FOLDER}",
        trigger_rule="none_failed",
    )

    tasks = defaultdict(dict)
    scopes = ["general", "api"]
    freqs = ["daily", "weekly", "monthly", "yearly"]
    for scope in scopes:
        for freq in freqs:
            tasks[scope][freq] = [
                PythonOperator(
                    task_id=f"run_notebook_and_save_to_s3_{scope}_{freq}",
                    python_callable=execute_and_upload_notebook,
                    op_kwargs={
                        "input_nb": AIRFLOW_DAG_HOME
                        + DAG_FOLDER
                        + (
                            "digest.ipynb" if scope == "general" else "digest-api.ipynb"
                        ),
                        "output_nb": today
                        + ("" if scope == "general" else "-api")
                        + ".ipynb",
                        "tmp_path": TMP_FOLDER + f"/digest_{freq}/{today}/",
                        "s3_url": S3_URL,
                        "s3_bucket": S3_BUCKET_DATA_PIPELINE_OPEN,
                        "s3_user": SECRET_S3_DATA_PIPELINE_USER,
                        "s3_password": SECRET_S3_DATA_PIPELINE_PASSWORD,
                        "s3_output_filepath": S3_PATH + f"digest_{freq}/{today}/",
                        "parameters": {
                            "WORKING_DIR": AIRFLOW_DAG_HOME,
                            "OUTPUT_DATA_FOLDER": (
                                TMP_FOLDER + f"/digest_{freq}/{today}/output/"
                            ),
                            "DATE_AIRFLOW": today,
                            "PERIOD_DIGEST": freq,
                        },
                    },
                ),
                PythonOperator(
                    task_id=f"publish_mattermost_{scope}_{freq}",
                    python_callable=publish_mattermost_period,
                    templates_dict={
                        "TODAY": today,
                        "period": freq,
                        "scope": scope,
                    },
                ),
                PythonOperator(
                    task_id=f"send_email_report_{scope}_{freq}",
                    python_callable=send_email_report_period,
                    templates_dict={
                        "TODAY": today,
                        "period": freq,
                        "scope": scope,
                    },
                )
                if scope == "general"
                else None,
            ]

    short_circuits = {
        "weekly": ShortCircuitOperator(
            task_id="check_if_monday", python_callable=check_if_monday
        ),
        "monthly": ShortCircuitOperator(
            task_id="check_if_first_day_of_month",
            python_callable=check_if_first_day_of_month,
        ),
        "yearly": ShortCircuitOperator(
            task_id="check_if_first_day_of_year",
            python_callable=check_if_first_day_of_year,
        ),
    }

    for scope in scopes:
        for freq in freqs:
            if freq in short_circuits:
                short_circuits[freq].set_upstream(clean_previous_output)
                tasks[scope][freq][0].set_upstream(short_circuits[freq])
            else:
                tasks[scope][freq][0].set_upstream(clean_previous_output)
            for k in range(2):
                if tasks[scope][freq][k + 1]:
                    tasks[scope][freq][k + 1].set_upstream(tasks[scope][freq][k])
            if scope != "general":
                # other scopes need stats.json too
                tasks[scope][freq][1].set_upstream(tasks["general"][freq][0])
            if tasks[scope][freq][-1]:
                clean_up.set_upstream(tasks[scope][freq][-1])
            else:
                clean_up.set_upstream(tasks[scope][freq][-2])
