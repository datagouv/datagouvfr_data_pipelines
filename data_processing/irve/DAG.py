from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from operators.mattermost import MattermostOperator
from operators.python_minio import PythonMinioOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime

import os
import requests
import pandas as pd

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
from datagouvfr_data_pipelines.utils.minio import send_files
from datagouvfr_data_pipelines.utils.mattermost import send_message

# for local dev in order not to mess up with production
# DATAGOUV_URL = 'https://data.gouv.fr'
# DATAGOUV_SECRET_API_KEY = 'non'

DAG_NAME = "irve_consolidation"
TMP_FOLDER = f"{AIRFLOW_DAG_TMP}{DAG_NAME}/"
SCHEMA_CATALOG = "https://schema.data.gouv.fr/schemas/schemas.json"
API_URL = f"{DATAGOUV_URL}/api/1/"
GIT_REPO = "git@github.com:etalab/schema.data.gouv.fr.git"
TMP_CONFIG_FILE = (
    f"{TMP_FOLDER}schema.data.gouv.fr/config_consolidation.yml"
)

default_args = {
    'email': [
        'pierlou.ramade@data.gouv.fr',
        'geoffrey.aldebert@data.gouv.fr'
    ],
    'email_on_failure': True
}


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
        op_args=(
            API_URL,
            working_dir,
            TMP_FOLDER,
            date_airflow,
            SCHEMA_CATALOG,
            TMP_CONFIG_FILE,
        ),
    )

    schema_irve_path = os.path.join(
        TMP_FOLDER, "consolidated_data", "etalab_schema-irve-statique"
    )
    schema_irve_cols = {
        "xy_coords": "coordonneesXY",
        "code_insee": "code_insee_commune",
        "adress": "adresse_station",
        "longitude": "consolidated_longitude",
        "latitude": "consolidated_latitude",
    }

    geodata_quality_improvement = PythonOperator(
        task_id="geodata_quality_improvement",
        python_callable=lambda schema_path: improve_geo_data_quality(
            {
                os.path.join(schema_path, filename): schema_irve_cols
                for filename in os.listdir(schema_path)
            }
        )
        if schema_path is not None
        else None,
        op_args=[schema_irve_path],
    )

    output_data_folder = f"{TMP_FOLDER}output/"
    upload_consolidation = PythonMinioOperator(
        task_id="upload_consolidated_datasets",
        tmp_path=TMP_FOLDER,
        minio_url=MINIO_URL,
        minio_bucket=MINIO_BUCKET_DATA_PIPELINE_OPEN,
        minio_user=SECRET_MINIO_DATA_PIPELINE_USER,
        minio_password=SECRET_MINIO_DATA_PIPELINE_PASSWORD,
        minio_output_filepath="schema/schemas_consolidation/{{ ds }}/",
        python_callable=run_consolidation_upload,
        op_args=(
            API_URL,
            DATAGOUV_SECRET_API_KEY,
            TMP_FOLDER,
            working_dir,
            date_airflow,
            SCHEMA_CATALOG,
            output_data_folder,
            TMP_CONFIG_FILE,
        ),
    )

    commit_changes = BashOperator(
        task_id="commit_changes",
        bash_command=(
            f"cd {TMP_FOLDER}schema.data.gouv.fr/ && git add config_consolidation.yml "
            ' && git commit -m "Update config consolidation file - '
            f'{ datetime.today().strftime("%Y-%m-%d")}'
            '" || echo "No changes to commit"'
            " && git push origin main"
        )
    )

    notification_synthese = PythonOperator(
        task_id="notification_synthese",
        python_callable=notification_synthese,
        templates_dict={"TODAY": "{{ ds }}"},
    )

    clone_dag_schema_repo.set_upstream(clean_previous_outputs)
    run_consolidation.set_upstream(clone_dag_schema_repo)
    geodata_quality_improvement.set_upstream(run_consolidation)
    upload_consolidation.set_upstream(geodata_quality_improvement)
    commit_changes.set_upstream(upload_consolidation)
    notification_synthese.set_upstream(commit_changes)
