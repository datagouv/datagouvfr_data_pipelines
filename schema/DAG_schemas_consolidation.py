from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
from pathlib import Path

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_HOME,
    AIRFLOW_DAG_TMP,
    MINIO_URL,
    MINIO_BUCKET_DATA_PIPELINE_OPEN,
    SECRET_MINIO_DATA_PIPELINE_USER,
    SECRET_MINIO_DATA_PIPELINE_PASSWORD,
    MATTERMOST_DATAGOUV_SCHEMA_ACTIVITE,
    DATAGOUV_SECRET_API_KEY,
    DATAGOUV_URL
)
from datagouvfr_data_pipelines.schema.scripts.schemas_consolidation.schemas_consolidation import (
    run_schemas_consolidation,
)
from datagouvfr_data_pipelines.schema.scripts.schemas_consolidation.consolidation_upload import (
    run_consolidation_upload,
)
from datagouvfr_data_pipelines.utils.schema import (
    upload_minio,
    notification_synthese
)

# for local dev in order not to mess up with production
# DATAGOUV_URL = 'https://data.gouv.fr'
# DATAGOUV_SECRET_API_KEY = ''

DAG_NAME = "schema_consolidation"
TMP_FOLDER = Path(f"{AIRFLOW_DAG_TMP}{DAG_NAME}/")
TMP_CONFIG_FILE = TMP_FOLDER / "schema.data.gouv.fr/config_consolidation.yml"
SCHEMA_CATALOG = "https://schema.data.gouv.fr/schemas/schemas.json"
API_URL = f"{DATAGOUV_URL}/api/1/"
GIT_REPO = "git@github.com:etalab/schema.data.gouv.fr.git"
output_data_folder = f"{TMP_FOLDER}/output/"
date_airflow = "{{ ds }}"

default_args = {"email": ["geoffrey.aldebert@data.gouv.fr"], "email_on_failure": True}


with DAG(
    dag_id=DAG_NAME,
    schedule_interval="0 5 * * *",
    start_date=days_ago(1),
    dagrun_timeout=timedelta(minutes=240),
    tags=["schemas", "irve", "consolidation", "datagouv"],
    default_args=default_args,
) as dag:
    clean_previous_outputs = BashOperator(
        task_id="clean_previous_outputs",
        bash_command=f"rm -rf {TMP_FOLDER} && mkdir -p {TMP_FOLDER}",
    )

    clone_dag_schema_repo = BashOperator(
        task_id="clone_dag_schema_repo",
        bash_command=f"cd {TMP_FOLDER} && git clone {GIT_REPO} --depth 1 ",
        # for local dev without SSH enabled
        # bash_command=f"cd {TMP_FOLDER} && git clone https://github.com/etalab/schema.data.gouv.fr.git --depth 1 ",
    )

    working_dir = f"{AIRFLOW_DAG_HOME}datagouvfr_data_pipelines/schema/scripts/"
    date_airflow = "{{ ds }}"

    run_consolidation = PythonOperator(
        task_id="run_schemas_consolidation",
        python_callable=run_schemas_consolidation,
        op_kwargs={
            "tmp_path": TMP_FOLDER,
            "date_airflow": date_airflow,
            "schema_catalog_url": SCHEMA_CATALOG,
            "config_path": TMP_CONFIG_FILE,
        },
    )

    upload_consolidation = PythonOperator(
        task_id="upload_consolidated_datasets",
        python_callable=run_consolidation_upload,
        op_kwargs={
            "api_url": API_URL,
            "api_key": DATAGOUV_SECRET_API_KEY,
            "tmp_path": TMP_FOLDER,
            "date_airflow": date_airflow,
            "schema_catalog": SCHEMA_CATALOG,
            "output_data_folder": output_data_folder,
            "config_path": TMP_CONFIG_FILE,
        },
    )

    upload_minio = PythonOperator(
        task_id="upload_minio",
        python_callable=upload_minio,
        op_kwargs={
            "TMP_FOLDER": TMP_FOLDER.as_posix(),
            "MINIO_URL": MINIO_URL,
            "MINIO_BUCKET_DATA_PIPELINE_OPEN": MINIO_BUCKET_DATA_PIPELINE_OPEN,
            "SECRET_MINIO_DATA_PIPELINE_USER": SECRET_MINIO_DATA_PIPELINE_USER,
            "SECRET_MINIO_DATA_PIPELINE_PASSWORD": SECRET_MINIO_DATA_PIPELINE_PASSWORD,
            "minio_output_filepath": "/schema/schemas_consolidation/{{ ds }}/",
        },
    )

    commit_changes = BashOperator(
        task_id="commit_changes",
        bash_command=(
            f"cd {TMP_FOLDER.as_posix()}/schema.data.gouv.fr/ && git add config_consolidation.yml "
            ' && git commit -m "Update config consolidation file - '
            f'{ datetime.today().strftime("%Y-%m-%d")}'
            '" || echo "No changes to commit"'
            " && git push origin main"
        )
    )

    notification_synthese = PythonOperator(
        task_id="notification_synthese",
        python_callable=notification_synthese,
        op_kwargs={
            "MINIO_URL": MINIO_URL,
            "MINIO_BUCKET_DATA_PIPELINE_OPEN": MINIO_BUCKET_DATA_PIPELINE_OPEN,
            "TMP_FOLDER": TMP_FOLDER,
            "SECRET_MINIO_DATA_PIPELINE_USER": SECRET_MINIO_DATA_PIPELINE_USER,
            "SECRET_MINIO_DATA_PIPELINE_PASSWORD": SECRET_MINIO_DATA_PIPELINE_PASSWORD,
            "MATTERMOST_DATAGOUV_SCHEMA_ACTIVITE": MATTERMOST_DATAGOUV_SCHEMA_ACTIVITE,
            "date_dict": {"TODAY": "{{ ds }}"}
        },
    )

    clone_dag_schema_repo.set_upstream(clean_previous_outputs)
    run_consolidation.set_upstream(clone_dag_schema_repo)
    upload_consolidation.set_upstream(run_consolidation)
    upload_minio.set_upstream(upload_consolidation)
    commit_changes.set_upstream(upload_minio)
    notification_synthese.set_upstream(commit_changes)
