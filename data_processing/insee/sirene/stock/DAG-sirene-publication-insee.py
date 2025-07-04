from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datagouvfr_data_pipelines.data_processing.insee.sirene.stock.task_functions import (
    get_files,
    upload_files_minio,
    compare_minio_files,
    move_new_files_to_latest,
    publish_file_files_data_gouv,
    update_dataset_data_gouv,
    publish_mattermost,
)
from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
)

TMP_FOLDER = f"{AIRFLOW_DAG_TMP}sirene_publication/"
MINIO_BASE_PATH = "insee/sirene/sirene_publication/"

with DAG(
    dag_id="data_processing_sirene_publication",
    schedule_interval="5 6,8,10 1-3 * *",
    start_date=datetime(2024, 8, 10),
    dagrun_timeout=timedelta(minutes=120),
    tags=["data_processing", "sirene", "publication"],
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
            "resource_file": "resources_to_download.json",
        },
        python_callable=get_files,
    )

    upload_new_files_minio = PythonOperator(
        task_id="upload_new_files_minio",
        templates_dict={
            "tmp_dir": TMP_FOLDER,
            "minio_path": MINIO_BASE_PATH + "new/",
            "resource_file": "resources_to_download.json",
        },
        python_callable=upload_files_minio,
    )

    compare_minio_files = ShortCircuitOperator(
        task_id="compare_minio_files",
        templates_dict={
            "minio_path_new": MINIO_BASE_PATH + "new/",
            "minio_path_latest": MINIO_BASE_PATH + "latest/",
            "resource_file": "resources_to_download.json",
        },
        python_callable=compare_minio_files,
    )

    move_new_files_to_latest = PythonOperator(
        task_id="move_new_files_to_latest",
        templates_dict={
            "minio_latest_path": MINIO_BASE_PATH + "latest/",
            "minio_new_path": MINIO_BASE_PATH + "new/",
            "resource_file": "resources_to_download.json",
        },
        python_callable=move_new_files_to_latest,
    )

    publish_file_files_data_gouv = PythonOperator(
        task_id="publish_file_files_data_gouv",
        templates_dict={
            "tmp_dir": TMP_FOLDER,
            "resource_file": "resources_to_download.json",
            "files_path": "insee-sirene/",
        },
        python_callable=publish_file_files_data_gouv,
    )

    update_dataset_data_gouv = PythonOperator(
        task_id="update_dataset_data_gouv",
        templates_dict={
            "resource_file": "resources_to_download.json",
            "day_file": "01",
        },
        python_callable=update_dataset_data_gouv,
    )

    publish_mattermost = PythonOperator(
        task_id="publish_mattermost",
        python_callable=publish_mattermost,
        op_kwargs={"geoloc": False},
    )

    clean_up = BashOperator(
        task_id="clean_up",
        bash_command=f"rm -rf {TMP_FOLDER}",
        trigger_rule="none_failed",
    )

    trigger_geocodage = TriggerDagRunOperator(
        task_id="trigger_geocodage",
        trigger_dag_id="data_processing_sirene_geocodage",
    )

    get_files.set_upstream(clean_previous_outputs)
    upload_new_files_minio.set_upstream(get_files)
    compare_minio_files.set_upstream(upload_new_files_minio)
    move_new_files_to_latest.set_upstream(compare_minio_files)
    publish_file_files_data_gouv.set_upstream(move_new_files_to_latest)
    update_dataset_data_gouv.set_upstream(publish_file_files_data_gouv)
    publish_mattermost.set_upstream(update_dataset_data_gouv)
    clean_up.set_upstream(publish_mattermost)
    trigger_geocodage.set_upstream(clean_up)
