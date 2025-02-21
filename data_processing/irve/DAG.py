from datetime import datetime, timedelta
from pathlib import Path

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
    AIRFLOW_ENV,
    MATTERMOST_DATAGOUV_SCHEMA_ACTIVITE,
    MINIO_BUCKET_DATA_PIPELINE_OPEN,
    MINIO_URL,
)
from datagouvfr_data_pipelines.data_processing.irve.task_functions import (
    consolidate_irve,
    create_consolidation_reports_irve,
    create_detailed_report_irve,
    custom_filters_irve,
    download_irve_resources,
    final_directory_clean_up_irve,
    get_all_irve_resources,
    improve_irve_geo_data_quality,
    notification_synthese_irve,
    update_consolidation_documentation_report_irve,
    update_reference_table_irve,
    update_resource_send_mail_producer_irve,
    upload_consolidated_irve,
    upload_minio_irve,
)

DAG_NAME = "irve_consolidation"
TMP_FOLDER = Path(f"{AIRFLOW_DAG_TMP}{DAG_NAME}/")
TMP_CONFIG_FILE = TMP_FOLDER / "schema.data.gouv.fr/config_consolidation.yml"
SCHEMA_CATALOG = "https://schema.data.gouv.fr/schemas/schemas.json"
GIT_REPO = "https://github.com/datagouv/schema.data.gouv.fr.git"
output_data_folder = f"{TMP_FOLDER}/output/"

default_args = {
    "retries": 5 if AIRFLOW_ENV == "prod" else 0,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id=DAG_NAME,
    schedule_interval="0 5 * * *",
    start_date=datetime(2024, 8, 10),
    dagrun_timeout=timedelta(minutes=360),
    catchup=False,
    max_active_runs=1,
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
    )

    get_all_irve_resources = PythonOperator(
        task_id="get_all_irve_resources",
        python_callable=get_all_irve_resources,
        op_kwargs={
            "tmp_path": TMP_FOLDER,
            "schemas_catalogue_url": SCHEMA_CATALOG,
            "config_path": TMP_CONFIG_FILE,
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
            "schemas_catalogue_url": SCHEMA_CATALOG,
        },
    )

    custom_filters_irve = PythonOperator(
        task_id="custom_filters_irve",
        python_callable=custom_filters_irve,
    )

    geodata_quality_improvement = PythonOperator(
        task_id="geodata_quality_improvement",
        python_callable=improve_irve_geo_data_quality,
        op_kwargs={
            "tmp_path": TMP_FOLDER,
        },
    )

    upload_consolidated_irve = PythonOperator(
        task_id="upload_consolidated_irve",
        python_callable=upload_consolidated_irve,
        op_kwargs={"config_path": TMP_CONFIG_FILE},
    )

    update_reference_table_irve = PythonOperator(
        task_id="update_reference_table_irve",
        python_callable=update_reference_table_irve,
    )

    update_resource_send_mail_producer_irve = PythonOperator(
        task_id="update_resource_send_mail_producer_irve",
        python_callable=update_resource_send_mail_producer_irve,
    )

    update_consolidation_documentation_report_irve = PythonOperator(
        task_id="update_consolidation_documentation_report_irve",
        python_callable=update_consolidation_documentation_report_irve,
        op_kwargs={"config_path": TMP_CONFIG_FILE},
    )

    create_consolidation_reports_irve = PythonOperator(
        task_id="create_consolidation_reports_irve",
        python_callable=create_consolidation_reports_irve,
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

    upload_minio_irve = PythonOperator(
        task_id="upload_minio_irve",
        python_callable=upload_minio_irve,
        op_kwargs={
            "tmp_folder": TMP_FOLDER,
            "minio_bucket_data_pipeline_open": MINIO_BUCKET_DATA_PIPELINE_OPEN,
            "minio_output_filepath": f"schema/schemas_consolidation/{datetime.today().strftime('%Y-%m-%d')}",
        },
    )

    commit_changes = BashOperator(
        task_id="commit_changes",
        bash_command=(
            f"cd {TMP_FOLDER.as_posix()}/schema.data.gouv.fr/ && git add config_consolidation.yml "
            ' && git commit -m "Update config consolidation file - '
            f"{datetime.today().strftime('%Y-%m-%d')}"
            '" || echo "No changes to commit"'
            " && git push origin main"
        ),
    )

    notification_synthese_irve = PythonOperator(
        task_id="notification_synthese_irve",
        python_callable=notification_synthese_irve,
        op_kwargs={
            "minio_url": MINIO_URL,
            "minio_bucket_data_pipeline_open": MINIO_BUCKET_DATA_PIPELINE_OPEN,
            "tmp_folder": TMP_FOLDER,
            "mattermost_datagouv_schema_activite": MATTERMOST_DATAGOUV_SCHEMA_ACTIVITE,
            "date_dict": {"TODAY": f"{datetime.today().strftime('%Y-%m-%d')}"},
        },
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
    update_consolidation_documentation_report_irve.set_upstream(update_resource_send_mail_producer_irve)
    create_consolidation_reports_irve.set_upstream(update_consolidation_documentation_report_irve)
    create_detailed_report_irve.set_upstream(create_consolidation_reports_irve)
    final_directory_clean_up_irve.set_upstream(create_detailed_report_irve)
    upload_minio_irve.set_upstream(final_directory_clean_up_irve)
    commit_changes.set_upstream(upload_minio_irve)
    notification_synthese_irve.set_upstream(commit_changes)
