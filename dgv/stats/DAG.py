from datetime import timedelta, datetime
from airflow.models import DAG

from datagouvfr_data_pipelines.dgv.stats.task_functions import (
    TMP_FOLDER,
    create_year_if_missing,
    update_year,
)
from datagouvfr_data_pipelines.utils.tasks import clean_up_folder

DAG_NAME = "dgv_stats"

default_args = {
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_NAME,
    schedule="55 5 * * *",
    start_date=datetime(2023, 10, 15),
    catchup=False,
    dagrun_timeout=timedelta(minutes=20),
    tags=["datagouv", "stats", "metrics"],
    default_args=default_args,
):
    
    (
        clean_up_folder(TMP_FOLDER, recreate=True)
        >> create_year_if_missing()
        >> update_year()
        >> clean_up_folder(TMP_FOLDER)
    )
