from datetime import timedelta, datetime
from pathlib import Path
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
    MINIO_URL,
    MINIO_BUCKET_DATA_PIPELINE_OPEN,
    MATTERMOST_DATAGOUV_SCHEMA_ACTIVITE,
)
from datagouvfr_data_pipelines.data_processing.irve.task_functions import (
    get_all_irve_resources,
    download_irve_resources,
    consolidate_irve,
    custom_filters_irve,
    improve_irve_geo_data_quality,
    upload_consolidated_irve,
    update_reference_table_irve,
    update_resource_send_mail_producer_irve,
    update_consolidation_documentation_report_irve,
    create_consolidation_reports_irve,
    create_detailed_report_irve,
    final_directory_clean_up_irve,
    upload_minio_irve,
    notification_synthese_irve
)

DAG_NAME = "irve_consolidation"
TMP_FOLDER = Path(f"{AIRFLOW_DAG_TMP}{DAG_NAME}/")
TMP_CONFIG_FILE = TMP_FOLDER / "schema.data.gouv.fr/config_consolidation.yml"
SCHEMA_CATALOG = "https://schema.data.gouv.fr/schemas/schemas.json"
GIT_REPO = "git@github.com:etalab/schema.data.gouv.fr.git"
output_data_folder = f"{TMP_FOLDER}/output/"

default_args = {
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
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

    get_all_irve_resources = PythonOperator(
        task_id="get_all_irve_resources",
        python_callable=get_all_irve_resources,
        op_kwargs={
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
        op_kwargs={
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
    )

    update_consolidation_documentation_report_irve = PythonOperator(
        task_id="update_consolidation_documentation_report_irve",
        python_callable=update_consolidation_documentation_report_irve,
        op_kwargs={
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

    upload_minio_irve = PythonOperator(
        task_id="upload_minio_irve",
        python_callable=upload_minio_irve,
        op_kwargs={
            "TMP_FOLDER": TMP_FOLDER,
            "MINIO_BUCKET_DATA_PIPELINE_OPEN": MINIO_BUCKET_DATA_PIPELINE_OPEN,
            "minio_output_filepath": f"schema/schemas_consolidation/{datetime.today().strftime('%Y-%m-%d')}",
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
            "MATTERMOST_DATAGOUV_SCHEMA_ACTIVITE": MATTERMOST_DATAGOUV_SCHEMA_ACTIVITE,
            "date_dict": {"TODAY": f"{datetime.today().strftime('%Y-%m-%d')}"}
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
