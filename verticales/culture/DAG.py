from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
)
from datagouvfr_data_pipelines.vert.task_functions import (
    get_perimeter_orgas,
)

TMP_FOLDER = f"{AIRFLOW_DAG_TMP}culture/"
DAG_NAME = "verticale_culture"
DATADIR = f"{TMP_FOLDER}data"

with DAG(
    dag_id=DAG_NAME,
    schedule_interval=None,
    start_date=datetime(2025, 8, 1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=240),
    tags=["verticale", "culture"],
) as dag:

    clean_previous_outputs = BashOperator(
        task_id="clean_previous_outputs",
        bash_command=f"rm -rf {TMP_FOLDER} && mkdir -p {TMP_FOLDER}",
    )

    get_perimeter_orgas = PythonOperator(
        task_id="get_perimeter_orgas",
        python_callable=get_perimeter_orgas,
    )

    get_perimeter_orgas.set_upstream(clean_previous_outputs)
