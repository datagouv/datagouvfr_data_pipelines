from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
)
from datagouvfr_data_pipelines.data_processing.rna.task_functions import (
    get_perimeter_orgas,
)

TMP_FOLDER = f"{AIRFLOW_DAG_TMP}rna/"
DAG_FOLDER = "datagouvfr_data_pipelines/data_processing/"
DAG_NAME = "verticale_culture"
DATADIR = f"{TMP_FOLDER}data"

with DAG(
    dag_id=DAG_NAME,
    # source files are usually uploaded in the morning of the 1st of the month
    schedule_interval="0 12 1,2,15 * *",
    start_date=datetime(2024, 8, 10),
    catchup=False,
    dagrun_timeout=timedelta(minutes=240),
    tags=["data_processing", "rna", "association"],
) as dag:

    check_if_modif = PythonOperator(
        task_id=f"process_rna_{file_type}",
        python_callable=process_rna,
        op_kwargs={
            "file_type": file_type,
        },
    )

    clean_previous_outputs = BashOperator(
        task_id="clean_previous_outputs",
        bash_command=f"rm -rf {TMP_FOLDER} && mkdir -p {TMP_FOLDER}",
    )

    send_notification_mattermost.set_upstream(clean_up)
