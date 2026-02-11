from datetime import timedelta, datetime
from airflow.models import DAG
from airflow.operators.python import ShortCircuitOperator

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
)
from datagouvfr_data_pipelines.data_processing.insee.deces.task_functions import (
    check_if_modif,
    gather_data,
    send_to_s3,
    publish_on_datagouv,
    notification_mattermost,
)
from datagouvfr_data_pipelines.utils.tasks import clean_up_folder

TMP_FOLDER = f"{AIRFLOW_DAG_TMP}deces/"
DAG_NAME = "data_processing_deces_consolidation"

default_args = {
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
    "provide_context": True,
}


with DAG(
    dag_id=DAG_NAME,
    schedule="0 * * * *",
    start_date=datetime(2024, 8, 10),
    dagrun_timeout=timedelta(minutes=240),
    tags=["deces", "consolidation", "datagouv"],
    catchup=False,
    default_args=default_args,
):
    
    (
        clean_up_folder(TMP_FOLDER, recreate=True)
        >> ShortCircuitOperator(
            task_id="check_if_modif",
            python_callable=check_if_modif,
        )
        >> gather_data()
        >> send_to_s3()
        >> publish_on_datagouv()
        >> clean_up_folder(TMP_FOLDER)
        >> notification_mattermost()
    )
