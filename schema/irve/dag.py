from datetime import datetime, timedelta
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG
from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
    AIRFLOW_ENV,
    S3_BUCKET_DATA_PIPELINE_OPEN,
    TCHAP_ROOM_DATAENG,
)
from datagouvfr_data_pipelines.schema.irve.task_functions import (
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
from datagouvfr_data_pipelines.schema.utils.consolidation import (
    notification_synthese,
    upload_s3,
)
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
        BashOperator(
            task_id="clone_dag_schema_repo",
            # Recreating the folder before cloning to avoid stale data when a retry occurs
            bash_command=f"rm -rf {TMP_FOLDER} && mkdir -p {TMP_FOLDER} && cd {TMP_FOLDER} && timeout 300 git clone {GIT_REPO} --depth 1 ",
            execution_timeout=timedelta(minutes=10),
            env={
                "GIT_SSH_COMMAND": (
                    "ssh "
                    # # Fail instead of asking interactive questions and hagging indefinitely
                    # "-o BatchMode=yes "
                    # Temporary debug fix
                    "-o StrictHostKeyChecking=accept-new "
                    # Fail fast instead of hanging forever on a stalled SSH git clone
                    # so Airflow retry can actually occur quickly
                    "-o ConnectTimeout=30 -o ServerAliveInterval=15 -o ServerAliveCountMax=3"
                ),
            },
            append_env=True,
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
        )
        >> BashOperator(
            task_id="commit_changes",
            bash_command=(
                f"cd {TMP_FOLDER.as_posix()}/schema.data.gouv.fr/ && git add config_consolidation.yml "
                ' && git commit -m "Update config consolidation file - {{ ds }}"'
                ' || echo "No changes to commit"'
                " && git push origin main"
            ),
        )
        >> notification_synthese.override(task_id="notification_synthese_irve")(
            s3_bucket_data_pipeline_open=S3_BUCKET_DATA_PIPELINE_OPEN,
            tmp_folder=TMP_FOLDER,
            room_id=TCHAP_ROOM_DATAENG,
            schema_name="etalab/schema-irve-statique",
        )
        >> clean_up_folder(TMP_FOLDER.as_posix())
    )
