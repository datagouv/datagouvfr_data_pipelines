from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.bash import BashOperator

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

TMP_FOLDER = f"{AIRFLOW_DAG_TMP}sirene_geolocalisation_insee/"
S3_BASE_PATH = "siren/geoloc/"

with DAG(
    dag_id="data_processing_sirene_geolocalisation",
    schedule="5 6,7,8,9,10 21-23 * *",
    start_date=datetime(2024, 8, 10),
    dagrun_timeout=timedelta(minutes=60),
    tags=["data_processing", "sirene", "geolocalisation"],
    params={},
    catchup=False,
) as dag:
    clean_previous_outputs = BashOperator(
        task_id="clean_previous_outputs",
        bash_command=f"rm -rf {TMP_FOLDER} && mkdir -p {TMP_FOLDER}",
    )

    check_if_already_processed = ShortCircuitOperator(
        task_id="check_if_already_processed",
        op_kwargs={
            "s3_path": S3_BASE_PATH,
        },
        python_callable=check_if_already_processed,
    )

    get_files = PythonOperator(
        task_id="get_files",
        op_kwargs={
            "tmp_dir": TMP_FOLDER,
            "resource_file": "resources_geolocalisation_to_download.json",
        },
        python_callable=get_files,
    )

    publish_file_s3 = PythonOperator(
        task_id="publish_file_s3",
        op_kwargs={
            "tmp_dir": TMP_FOLDER,
            "resource_file": "resources_geolocalisation_to_download.json",
            "s3_path": S3_BASE_PATH,
        },
        python_callable=publish_file_s3,
    )

    update_dataset_data_gouv = PythonOperator(
        task_id="update_dataset_data_gouv",
        op_kwargs={
            "resource_file": "resources_geolocalisation_to_download.json",
            "tmp_dir": TMP_FOLDER,
        },
        python_callable=update_dataset_data_gouv,
    )

    publish_mattermost_geoloc = PythonOperator(
        task_id="publish_mattermost",
        python_callable=publish_mattermost,
        op_kwargs={"geoloc": True},
    )

    clean_up = BashOperator(
        task_id="clean_up",
        bash_command=f"rm -rf {TMP_FOLDER}",
        trigger_rule="none_failed",
    )

    check_if_already_processed.set_upstream(clean_previous_outputs)
    get_files.set_upstream(check_if_already_processed)
    publish_file_s3.set_upstream(get_files)
    update_dataset_data_gouv.set_upstream(publish_file_s3)
    publish_mattermost_geoloc.set_upstream(update_dataset_data_gouv)
    clean_up.set_upstream(publish_mattermost_geoloc)
