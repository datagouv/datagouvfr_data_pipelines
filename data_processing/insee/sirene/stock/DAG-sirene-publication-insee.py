from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datagouvfr_data_pipelines.data_processing.insee.sirene.stock.task_functions import (
    check_if_already_processed,
    get_files,
    publish_file_minio,
    update_dataset_data_gouv,
    publish_mattermost,
)
from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
)

TMP_FOLDER = f"{AIRFLOW_DAG_TMP}sirene_publication/"
MINIO_BASE_PATH = "siren/stock/"

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

    check_if_already_processed = ShortCircuitOperator(
        task_id="check_if_already_processed",
        op_kwargs={
            "minio_path": MINIO_BASE_PATH,
        },
        python_callable=check_if_already_processed,
    )

    get_files = PythonOperator(
        task_id="get_files",
        op_kwargs={
            "tmp_dir": TMP_FOLDER,
            "resource_file": "resources_to_download.json",
        },
        python_callable=get_files,
    )

    publish_file_minio = PythonOperator(
        task_id="publish_file_minio",
        op_kwargs={
            "tmp_dir": TMP_FOLDER,
            "resource_file": "resources_to_download.json",
            "minio_path": MINIO_BASE_PATH,
        },
        python_callable=publish_file_minio,
    )

    update_dataset_data_gouv = PythonOperator(
        task_id="update_dataset_data_gouv",
        op_kwargs={
            "resource_file": "resources_to_download.json",
            "tmp_dir": TMP_FOLDER,
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
        bash_command=f"rm -rf {TMP_FOLDER}/*",
        trigger_rule="none_failed",
    )

    trigger_geocodage = TriggerDagRunOperator(
        task_id="trigger_geocodage",
        trigger_dag_id="data_processing_sirene_geocodage",
    )

    check_if_already_processed.set_upstream(clean_previous_outputs)
    get_files.set_upstream(check_if_already_processed)
    publish_file_minio.set_upstream(get_files)
    update_dataset_data_gouv.set_upstream(publish_file_minio)
    publish_mattermost.set_upstream(update_dataset_data_gouv)
    clean_up.set_upstream(publish_mattermost)
    trigger_geocodage.set_upstream(clean_up)
