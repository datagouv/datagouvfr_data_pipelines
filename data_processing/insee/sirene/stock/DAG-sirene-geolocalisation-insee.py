from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.bash import BashOperator

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
)
from datagouvfr_data_pipelines.data_processing.insee.sirene.stock.task_functions import (
    get_files,
    upload_files_minio,
    compare_minio_files,
    move_new_files_to_latest,
    publish_file_minio,
    update_dataset_data_gouv,
    publish_mattermost,
)

TMP_FOLDER = f"{AIRFLOW_DAG_TMP}sirene_geolocalisation_insee/"
MINIO_BASE_PATH = "insee/sirene/sirene_geolocalisation_insee/"

with DAG(
    dag_id="data_processing_sirene_geolocalisation",
    schedule_interval="5 6,7,8,9,10 21-23 * *",
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

    get_files = PythonOperator(
        task_id="get_files",
        templates_dict={
            "tmp_dir": TMP_FOLDER,
            "resource_file": "resources_geolocalisation_to_download.json",
        },
        python_callable=get_files,
    )

    upload_new_files_minio = PythonOperator(
        task_id="upload_new_files_minio",
        templates_dict={
            "tmp_dir": TMP_FOLDER,
            "minio_path": MINIO_BASE_PATH + "new/",
            "resource_file": "resources_geolocalisation_to_download.json",
        },
        python_callable=upload_files_minio,
    )

    compare_minio_files = ShortCircuitOperator(
        task_id="compare_minio_files",
        templates_dict={
            "minio_path_new": MINIO_BASE_PATH + "new/",
            "minio_path_latest": MINIO_BASE_PATH + "latest/",
            "resource_file": "resources_geolocalisation_to_download.json",
        },
        python_callable=compare_minio_files,
    )

    move_new_files_to_latest = PythonOperator(
        task_id="move_new_files_to_latest",
        templates_dict={
            "minio_latest_path": MINIO_BASE_PATH + "latest/",
            "minio_new_path": MINIO_BASE_PATH + "new/",
            "resource_file": "resources_geolocalisation_to_download.json",
        },
        python_callable=move_new_files_to_latest,
    )

    publish_file_minio = PythonOperator(
        task_id="publish_file_minio",
        templates_dict={
            "tmp_dir": TMP_FOLDER,
            "resource_file": "resources_geolocalisation_to_download.json",
            "minio_path": "siren/geoloc/",
        },
        python_callable=publish_file_minio,
    )

    update_dataset_data_gouv = PythonOperator(
        task_id="update_dataset_data_gouv",
        templates_dict={
            "resource_file": "resources_geolocalisation_to_download.json",
            "day_file": "21",
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

    get_files.set_upstream(clean_previous_outputs)
    upload_new_files_minio.set_upstream(get_files)
    compare_minio_files.set_upstream(upload_new_files_minio)
    move_new_files_to_latest.set_upstream(compare_minio_files)
    publish_file_minio.set_upstream(move_new_files_to_latest)
    update_dataset_data_gouv.set_upstream(publish_file_minio)
    publish_mattermost_geoloc.set_upstream(update_dataset_data_gouv)
    clean_up.set_upstream(publish_mattermost_geoloc)
