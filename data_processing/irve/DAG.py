from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
from pathlib import Path
import os

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
    MINIO_URL,
    MINIO_BUCKET_DATA_PIPELINE_OPEN,
    SECRET_MINIO_DATA_PIPELINE_USER,
    SECRET_MINIO_DATA_PIPELINE_PASSWORD,
    MATTERMOST_DATAGOUV_SCHEMA_ACTIVITE,
    DATAGOUV_SECRET_API_KEY
)
from datagouvfr_data_pipelines.data_processing.irve.task_functions import (
    get_all_irve_resources,
    download_irve_resources,
    consolidate_irve,
    custom_filters_irve,
    upload_consolidated_irve,
    update_reference_table_irve,
    update_resource_send_mail_producer_irve,
    add_validata_report_irve,
    update_consolidation_documentation_report_irve,
    create_consolidation_reports_irve,
    create_detailed_report_irve,
    final_directory_clean_up_irve,
    notification_synthese_irve
)
from datagouvfr_data_pipelines.data_processing.irve.geo_utils.geo import (
    improve_geo_data_quality,
)
from datagouvfr_data_pipelines.utils.datagouv import (
    DATAGOUV_URL,
)

# DEV : for local dev in order not to mess up with production
# DATAGOUV_URL = 'https://data.gouv.fr'
# DATAGOUV_SECRET_API_KEY = 'non'

DAG_NAME = "irve_consolidation"
TMP_FOLDER = Path(f"{AIRFLOW_DAG_TMP}{DAG_NAME}/")
TMP_CONFIG_FILE = TMP_FOLDER / "schema.data.gouv.fr/config_consolidation.yml"
SCHEMA_CATALOG = "https://schema.data.gouv.fr/schemas/schemas.json"
API_URL = f"{DATAGOUV_URL}/api/1/"
GIT_REPO = "git@github.com:etalab/schema.data.gouv.fr.git"
output_data_folder = f"{TMP_FOLDER}/output/"
date_airflow = "{{ ds }}"

default_args = {
    'email': [
        'pierlou.ramade@data.gouv.fr',
        'geoffrey.aldebert@data.gouv.fr'
    ],
    'email_on_failure': False,
    'provide_context': True,
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
        # DEV : for local dev without SSH enabled
        # bash_command=f"cd {TMP_FOLDER} && git clone https://github.com/etalab/schema.data.gouv.fr.git --depth 1 ",
    )

    date_airflow = "{{ ds }}"

    get_all_irve_resources = PythonOperator(
        task_id="get_all_irve_resources",
        python_callable=get_all_irve_resources,
        op_kwargs={
            "date_airflow": date_airflow,
            "tmp_path": TMP_FOLDER,
            "schemas_catalogue_url": SCHEMA_CATALOG,
            "config_path": TMP_CONFIG_FILE
        },
    )

    download_irve_resources = PythonOperator(
        task_id="download_irve_resources",
        python_callable=download_irve_resources,
    )

    consolidate_irve = PythonOperator(
        task_id="consolidate_irve",
        python_callable=consolidate_irve,
        op_kwargs={
            "tmp_path": TMP_FOLDER,
            "schemas_catalogue_url": SCHEMA_CATALOG
        },
    )

    custom_filters_irve = PythonOperator(
        task_id="custom_filters_irve",
        python_callable=custom_filters_irve,
    )

    schema_irve_path = TMP_FOLDER / "consolidated_data" / "etalab_schema-irve-statique"
    # it will exist for sure at this point but otherwise airflow throws an error
    if schema_irve_path.exists():
        schema_irve_path = schema_irve_path / os.listdir(schema_irve_path)[0]

    schema_irve_cols = {
        "xy_coords": "coordonneesXY",
        "code_insee": "code_insee_commune",
        "adress": "adresse_station",
        "longitude": "consolidated_longitude",
        "latitude": "consolidated_latitude",
    }

    geodata_quality_improvement = PythonOperator(
        task_id="geodata_quality_improvement",
        python_callable=improve_geo_data_quality,
        op_kwargs={
            "file_cols_mapping": {schema_irve_path.as_posix(): schema_irve_cols}
        },
    )

    upload_consolidated_irve = PythonOperator(
        task_id="upload_consolidated_irve",
        python_callable=upload_consolidated_irve,
        op_kwargs={
            "api_key": DATAGOUV_SECRET_API_KEY,
            "config_path": TMP_CONFIG_FILE
        },
    )

    update_reference_table_irve = PythonOperator(
        task_id="update_reference_table_irve",
        python_callable=update_reference_table_irve,
    )

    update_resource_send_mail_producer_irve = PythonOperator(
        task_id="update_resource_send_mail_producer_irve",
        python_callable=update_resource_send_mail_producer_irve,
        op_kwargs={
            "api_key": DATAGOUV_SECRET_API_KEY
        },
    )

    add_validata_report_irve = PythonOperator(
        task_id="add_validata_report_irve",
        python_callable=add_validata_report_irve,
        op_kwargs={
            "api_key": DATAGOUV_SECRET_API_KEY
        },
    )

    update_consolidation_documentation_report_irve = PythonOperator(
        task_id="update_consolidation_documentation_report_irve",
        python_callable=update_consolidation_documentation_report_irve,
        op_kwargs={
            "api_key": DATAGOUV_SECRET_API_KEY,
            "config_path": TMP_CONFIG_FILE
        },
    )

    create_consolidation_reports_irve = PythonOperator(
        task_id="create_consolidation_reports_irve",
        python_callable=create_consolidation_reports_irve
    )

    create_detailed_report_irve = PythonOperator(
        task_id="create_detailed_report_irve",
        python_callable=create_detailed_report_irve,
    )

    final_directory_clean_up_irve = PythonOperator(
        task_id="final_directory_clean_up_irve",
        python_callable=final_directory_clean_up_irve,
        op_kwargs={
            "tmp_path": TMP_FOLDER,
            "output_data_folder": output_data_folder,
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

    notification_synthese_irve = PythonOperator(
        task_id="notification_synthese_irve",
        python_callable=notification_synthese_irve,
        op_kwargs={
            "MINIO_URL": MINIO_URL,
            "MINIO_BUCKET_DATA_PIPELINE_OPEN": MINIO_BUCKET_DATA_PIPELINE_OPEN,
            "TMP_FOLDER": TMP_FOLDER,
            "SECRET_MINIO_DATA_PIPELINE_USER": SECRET_MINIO_DATA_PIPELINE_USER,
            "SECRET_MINIO_DATA_PIPELINE_PASSWORD": SECRET_MINIO_DATA_PIPELINE_PASSWORD,
            "MATTERMOST_DATAGOUV_SCHEMA_ACTIVITE": MATTERMOST_DATAGOUV_SCHEMA_ACTIVITE,
        },
        templates_dict={"TODAY": "{{ ds }}"},
    )

    clone_dag_schema_repo.set_upstream(clean_previous_outputs)
    get_all_irve_resources.set_upstream(clone_dag_schema_repo)
    download_irve_resources.set_upstream(get_all_irve_resources)
    consolidate_irve.set_upstream(download_irve_resources)
    custom_filters_irve.set_upstream(consolidate_irve)
    geodata_quality_improvement.set_upstream(custom_filters_irve)
    upload_consolidated_irve.set_upstream(geodata_quality_improvement)
    update_reference_table_irve.set_upstream(upload_consolidated_irve)
    update_resource_send_mail_producer_irve.set_upstream(update_reference_table_irve)
    add_validata_report_irve.set_upstream(update_resource_send_mail_producer_irve)
    update_consolidation_documentation_report_irve.set_upstream(add_validata_report_irve)
    create_consolidation_reports_irve.set_upstream(update_consolidation_documentation_report_irve)
    create_detailed_report_irve.set_upstream(create_consolidation_reports_irve)
    final_directory_clean_up_irve.set_upstream(create_detailed_report_irve)
    commit_changes.set_upstream(final_directory_clean_up_irve)
    notification_synthese_irve.set_upstream(commit_changes)
