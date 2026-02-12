from datetime import datetime, timedelta
from collections import defaultdict
from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.python import PythonOperator, ShortCircuitOperator

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_HOME,
    MINIO_URL,
    S3_BUCKET_DATA_PIPELINE_OPEN,
)
from datagouvfr_data_pipelines.dgv.monitoring.digest.task_functions import (
    DAG_FOLDER,
    DAG_NAME,
    TMP_FOLDER,
    send_email_report_period,
    publish_mattermost_period,
)
from datagouvfr_data_pipelines.utils.notebook import execute_and_upload_notebook
from datagouvfr_data_pipelines.utils.tasks import clean_up_folder
from datagouvfr_data_pipelines.utils.utils import (
    check_if_monday,
    check_if_first_day_of_month,
    check_if_first_day_of_year,
)

S3_PATH = "dgv/"
today = datetime.today().strftime("%Y-%m-%d")


default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id=DAG_NAME,
    schedule="0 6 * * *",
    start_date=datetime(2024, 8, 10),
    dagrun_timeout=timedelta(minutes=60),
    tags=["digest", "daily", "weekly", "monthly", "yearly", "datagouv"],
    default_args=default_args,
    catchup=False,
):
    clean_up_recreate = clean_up_folder(TMP_FOLDER, recreate=True)
    clean_up = clean_up_folder(TMP_FOLDER, trigger_rule="none_failed")

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

    tasks = defaultdict(dict)
    scopes = ["general", "api"]
    freqs = ["daily", "weekly", "monthly", "yearly"]
    for scope in scopes:
        for freq in freqs:
            tasks[scope][freq] = [clean_up_recreate]
            if freq in short_circuits:
                tasks[scope][freq].append(short_circuits[freq])
            tasks[scope][freq] += [
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
                        "s3_url": MINIO_URL,
                        "s3_bucket": S3_BUCKET_DATA_PIPELINE_OPEN,
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
                    op_kwargs={
                        "today": today,
                        "period": freq,
                        "scope": scope,
                    },
                ),
            ]
            if scope == "general":
                tasks[scope][freq].append(
                    PythonOperator(
                        task_id=f"send_email_report_{scope}_{freq}",
                        python_callable=send_email_report_period,
                        op_kwargs={
                            "today": today,
                            "period": freq,
                            "scope": scope,
                        },
                    )
                )
            tasks[scope][freq].append(clean_up)
            chain(*tasks[scope][freq])

for freq in freqs:
    tasks["general"][freq][-4] >> tasks["api"][freq][-2]
