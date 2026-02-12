from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python import ShortCircuitOperator

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

TMP_FOLDER = f"{AIRFLOW_DAG_TMP}sirene_geolocalisation/"
S3_BASE_PATH = "siren/geoloc/"

with DAG(
    dag_id="data_processing_sirene_geolocalisation",
    schedule="5 6,7,8,9,10 21-23 * *",
    start_date=datetime(2024, 8, 10),
    dagrun_timeout=timedelta(minutes=60),
    tags=["data_processing", "sirene", "geolocalisation"],
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
            resource_file="resources_geolocalisation_to_download.json",
        )
        >> publish_file_s3(
            tmp_dir=TMP_FOLDER,
            resource_file="resources_geolocalisation_to_download.json",
            s3_path=S3_BASE_PATH,
        )
        >> update_dataset_data_gouv(
            resource_file="resources_geolocalisation_to_download.json",
            tmp_dir=TMP_FOLDER,
        )
        >> publish_mattermost(geoloc=True)
        >> clean_up_folder(TMP_FOLDER)
    )
