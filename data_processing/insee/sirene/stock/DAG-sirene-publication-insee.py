from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
)
from datagouvfr_data_pipelines.data_processing.insee.sirene.stock.task_functions import (
    check_if_already_processed,
    get_files,
    publish_file_s3,
    update_dataset_data_gouv,
    publish_mattermost,
)
from datagouvfr_data_pipelines.utils.tasks import clean_up_folder

TMP_FOLDER = f"{AIRFLOW_DAG_TMP}sirene_publication/"
S3_BASE_PATH = "siren/stock/"

with DAG(
    dag_id="data_processing_sirene_publication",
    schedule="5 6,8,10 1-3 * *",
    start_date=datetime(2024, 8, 10),
    dagrun_timeout=timedelta(minutes=120),
    tags=["data_processing", "sirene", "publication"],
    catchup=False,
):

    (
        clean_up_folder(TMP_FOLDER, recreate=True)
        >> ShortCircuitOperator(
            task_id="check_if_already_processed",
            op_kwargs={
                "s3_path": S3_BASE_PATH,
            },
            python_callable=check_if_already_processed,
        )
        >> get_files(
            tmp_dir=TMP_FOLDER,
            resource_file="resources_to_download.json",
        )
        >> publish_file_s3(
            tmp_dir=TMP_FOLDER,
            resource_file="resources_to_download.json",
            s3_path=S3_BASE_PATH,
        )
        >> update_dataset_data_gouv(
            resource_file="resources_to_download.json",
            tmp_dir=TMP_FOLDER,
        )
        >> publish_mattermost(geoloc=True)
        >> clean_up_folder(TMP_FOLDER)
        >> TriggerDagRunOperator(
            task_id="trigger_geocodage",
            trigger_dag_id="data_processing_sirene_geocodage",
        )
    )
