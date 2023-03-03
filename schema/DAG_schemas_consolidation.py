from airflow.models import DAG, Variable
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from operators.mattermost import MattermostOperator
from operators.python_minio import PythonMinioOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime

import os
import requests
from minio import Minio
import pandas as pd

from dag_datagouv_data_pipelines.config import (
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
from dag_datagouv_data_pipelines.utils.minio import send_files
from dag_datagouv_data_pipelines.utils.mattermost import send_message

from dag_datagouv_data_pipelines.schema.utils.geo import improve_geo_data_quality
from dag_datagouv_data_pipelines.schema.scripts.schemas_consolidation.schemas_consolidation import (
    run_schemas_consolidation,
)
from dag_datagouv_data_pipelines.schema.scripts.schemas_consolidation.consolidation_upload import (
    run_consolidation_upload,
)

DAG_NAME = "schema_consolidation"
TMP_FOLDER = f"{AIRFLOW_DAG_TMP}{DAG_NAME}"
SCHEMA_CATALOG = "https://schema.data.gouv.fr/schemas/schemas.json"
API_URL = f"{DATAGOUV_URL}/api/1/"
GIT_REPO = "git@github.com:etalab/dag_schema_data_gouv_fr.git"

TMP_CLONE_REPO_PATH = f"{TMP_FOLDER}{DAG_NAME}/"
TMP_CONFIG_FILE = (
    f"{TMP_CLONE_REPO_PATH}dag_datagouv_data_pipelines/schema/scripts/config_tableschema.yml"
)

default_args = {"email": ["geoffrey.aldebert@data.gouv.fr"], "email_on_failure": True}


def notification_synthese(**kwargs):
    templates_dict = kwargs.get("templates_dict")
    last_conso = templates_dict["TODAY"]
    r = requests.get("https://schema.data.gouv.fr/schemas/schemas.json")
    schemas = r.json()["schemas"]

    message = (
        ":mega: *Rapport sur la consolidation des données répondant à un schéma.*\n"
    )

    for s in schemas:
        if s["schema_type"] == "tableschema":
            try:
                filename = (
                    f"https://{MINIO_URL}/{MINIO_BUCKET_DATA_PIPELINE_OPEN}/schema/schemas_consolidation/"
                    f"{last_conso}/output/ref_tables/ref_table_{s['name'].replace('/','_').csv}"
                )
                df = pd.read_csv(filename)
                nb_declares = df[df["resource_found_by"] == "1 - schema request"].shape[0]
                nb_suspectes = df[df["resource_found_by"] != "1 - schema request"].shape[0]
                nb_valides = df[df["is_valid_one_version"] == True].shape[0]
                df = df[df["is_valid_one_version"] == False]
                df = df[
                    [
                        "dataset_id",
                        "resource_id",
                        "dataset_title",
                        "resource_title",
                        "dataset_page",
                        "resource_url",
                        "resource_found_by",
                    ]
                ]
                df["schema_name"] = s["title"]
                df["schema_id"] = s["name"]
                df["validata_report"] = (
                    "https://validata.etalab.studio/table-schema?input=url&url="
                    f"{df['resource_url']}&schema_url={s['schema_url']}"
                )
                df.to_csv(
                    f"{TMP_FOLDER}{DAG_NAME}/liste_erreurs-{s['name'].replace('/', '_')}.csv"
                )

                send_files(
                    MINIO_URL=MINIO_URL,
                    MINIO_BUCKET=MINIO_BUCKET_DATA_PIPELINE_OPEN,
                    MINIO_USER=SECRET_MINIO_DATA_PIPELINE_USER,
                    MINIO_PASSWORD=SECRET_MINIO_DATA_PIPELINE_PASSWORD,
                    list_files=[
                        {
                            "source_path": f"{TMP_FOLDER}{DAG_NAME}",
                            "source_name": f"liste_erreurs-{s['name'].replace('/', '_')}.csv",
                            "dest_path": "schema/schemas_consolidation/liste_erreurs/",
                            "dest_name": f"liste_erreurs-{s['name'].replace('/', '_')}.csv",
                        }
                    ],
                )

                message += f"\n- Schéma ***{s['title']}***\n - Ressources déclarées : {nb_declares}"

                if nb_suspectes != 0:
                    message += f"\n - Ressources suspectées : {nb_suspectes}"

                message += (
                    f"\n - Ressources valides : {nb_valides} \n - [Liste des ressources non valides]"
                    f"(https://{MINIO_URL}/{MINIO_BUCKET_DATA_PIPELINE_OPEN}/schema/schemas_consolidation/"
                    f"{last_conso}/liste_erreurs/liste_erreurs-{s['name'].replace('/', '_')}.csv)\n"
                )
            except: # noqa
                print("No report for {}".format(s["name"]))
                pass
    send_message(message, MATTERMOST_DATAGOUV_SCHEMA_ACTIVITE)


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
        bash_command=f"rm -rf {TMP_FOLDER}{DAG_NAME}/ && mkdir -p {TMP_FOLDER}{DAG_NAME}/",
    )

    clone_dag_schema_repo = BashOperator(
        task_id="clone_dag_schema_repo",
        bash_command=f"cd {TMP_CLONE_REPO_PATH} && git clone {GIT_REPO} ",
    )

    shared_params = {
        "msgs": "Ran from Airflow {{ ds }} !",
        "WORKING_DIR": f"{AIRFLOW_DAG_HOME}dag_datagouv_data_pipelines/schema/scripts/",
        "TMP_FOLDER": TMP_FOLDER,
        "API_KEY": DATAGOUV_SECRET_API_KEY,
        "API_URL": API_URL,
        "DATE_AIRFLOW": "{{ ds }}",
        "SCHEMA_CATALOG": SCHEMA_CATALOG,
    }

    working_dir = f"{AIRFLOW_DAG_HOME}}dag_datagouv_data_pipelines/schema/scripts/"
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

    output_data_folder = f"{TMP_FOLDER}{DAG_NAME}/output/"
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
            f"cd {TMP_CLONE_REPO_PATH}/dag_datagouv_data_pipelines/ && git add schema "
            ' && git commit -m "Update config file - '
            f'{ datetime.today().strftime("%Y-%m-%d")}'
            '" || echo "No changes to commit"'
            " && git push origin master"
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
