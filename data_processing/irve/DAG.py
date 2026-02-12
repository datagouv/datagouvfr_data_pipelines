from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
    AIRFLOW_ENV,
    MATTERMOST_DATAGOUV_SCHEMA_ACTIVITE,
    S3_BUCKET_DATA_PIPELINE_OPEN,
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
    update_consolidation_documentation_report_irve,
    update_reference_table_irve,
    update_resource_send_mail_producer_irve,
    upload_consolidated_irve,
)
from datagouvfr_data_pipelines.utils.schema import upload_s3, notification_synthese
from datagouvfr_data_pipelines.utils.tasks import clean_up_folder

TMP_FOLDER = Path(f"{AIRFLOW_DAG_TMP}irve/")
TMP_CONFIG_FILE = TMP_FOLDER / "schema.data.gouv.fr/config_consolidation.yml"
SCHEMA_CATALOG = "https://schema.data.gouv.fr/schemas/schemas.json"
GIT_REPO = "git@github.com:datagouv/schema.data.gouv.fr.git"
if AIRFLOW_ENV == "dev":
    GIT_REPO = GIT_REPO.replace("git@github.com:", "https://github.com/")
output_data_folder = TMP_FOLDER / "output/"

default_args = {
    "retries": 5 if AIRFLOW_ENV == "prod" else 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="irve_consolidation",
    schedule="20 4 * * *",
    start_date=datetime(2024, 8, 10),
    dagrun_timeout=timedelta(minutes=360),
    catchup=False,
    max_active_runs=1,
    tags=["schemas", "irve", "consolidation", "datagouv"],
    default_args=default_args,
):
    (
        clean_up_folder(TMP_FOLDER.as_posix(), recreate=True)
        >> BashOperator(
            task_id="clone_dag_schema_repo",
            bash_command=f"cd {TMP_FOLDER} && git clone {GIT_REPO} --depth 1 ",
        )
        >> get_all_irve_resources(
            tmp_path=TMP_FOLDER,
            schemas_catalogue_url=SCHEMA_CATALOG,
            config_path=TMP_CONFIG_FILE,
        )
        >> download_irve_resources()
        >> consolidate_irve(
            tmp_path=TMP_FOLDER,
            schemas_catalogue_url=SCHEMA_CATALOG,
        )
        >> custom_filters_irve()
        >> improve_irve_geo_data_quality(tmp_path=TMP_FOLDER)
        >> upload_consolidated_irve(config_path=TMP_CONFIG_FILE)
        >> update_reference_table_irve()
        >> update_resource_send_mail_producer_irve()
        >> update_consolidation_documentation_report_irve(config_path=TMP_CONFIG_FILE)
        >> create_consolidation_reports_irve()
        >> create_detailed_report_irve()
        >> final_directory_clean_up_irve(
            tmp_path=TMP_FOLDER,
            output_data_folder=output_data_folder.as_posix(),
        )
        >> upload_s3.override(task_id="upload_s3_irve")(
            tmp_folder=TMP_FOLDER.as_posix(),
            s3_bucket_data_pipeline_open=S3_BUCKET_DATA_PIPELINE_OPEN,
            s3_output_filepath=f"schema/schemas_consolidation/{datetime.today().strftime('%Y-%m-%d')}",
        )
        >> BashOperator(
            task_id="commit_changes",
            bash_command=(
                f"cd {TMP_FOLDER.as_posix()}/schema.data.gouv.fr/ && git add config_consolidation.yml "
                ' && git commit -m "Update config consolidation file - '
                f"{datetime.today().strftime('%Y-%m-%d')}"
                '" || echo "No changes to commit"'
                " && git push origin main"
            ),
        )
        >> notification_synthese.override(task_id="notification_synthese_irve")(
            s3_url=MINIO_URL,
            s3_bucket_data_pipeline_open=S3_BUCKET_DATA_PIPELINE_OPEN,
            tmp_folder=TMP_FOLDER,
            mattermost_channel=MATTERMOST_DATAGOUV_SCHEMA_ACTIVITE,
            schema_name="etalab/schema-irve-statique",
        )
        >> clean_up_folder(TMP_FOLDER.as_posix())
    )
