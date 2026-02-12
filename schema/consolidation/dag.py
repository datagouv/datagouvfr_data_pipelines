from datetime import timedelta, datetime
from pathlib import Path
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
    AIRFLOW_ENV,
    MINIO_URL,
    S3_BUCKET_DATA_PIPELINE_OPEN,
    MATTERMOST_DATAGOUV_SCHEMA_ACTIVITE,
)
from datagouvfr_data_pipelines.schema.consolidation.task_functions import (
    get_resources,
    download_resources,
    consolidate_resources,
    upload_consolidated_data,
    update_reference_tables,
    update_resources,
    update_consolidation_documentation,
    create_consolidation_reports,
    create_detailed_reports,
    final_clean_up,
)
from datagouvfr_data_pipelines.utils.schema import upload_s3, notification_synthese
from datagouvfr_data_pipelines.utils.tasks import clean_up_folder

TMP_FOLDER = Path(f"{AIRFLOW_DAG_TMP}schema_consolidation/")
TMP_CONFIG_FILE = TMP_FOLDER / "schema.data.gouv.fr/config_consolidation.yml"
SCHEMA_CATALOG = "https://schema.data.gouv.fr/schemas/schemas.json"
GIT_REPO = "git@github.com:datagouv/schema.data.gouv.fr.git"
if AIRFLOW_ENV == "dev":
    GIT_REPO = GIT_REPO.replace("git@github.com:", "https://github.com/")
output_data_folder = f"{TMP_FOLDER}/output/"

default_args = {
    "retries": 5,
    "retry_delay": timedelta(minutes=20),
}


with DAG(
    dag_id="schema_consolidation",
    schedule="0 5 * * *",
    start_date=datetime(2024, 8, 10),
    dagrun_timeout=timedelta(minutes=600),
    tags=["schemas", "consolidation", "datagouv"],
    catchup=False,
    default_args=default_args,
):

    (
        clean_up_folder(TMP_FOLDER.as_posix(), recreate=True)
        >> BashOperator(
            task_id="clone_dag_schema_repo",
            bash_command=f"cd {TMP_FOLDER} && git clone {GIT_REPO} --depth 1 ",
        )
        >> get_resources(
            tmp_path=TMP_FOLDER,
            schema_catalog_url=SCHEMA_CATALOG,
            config_path=TMP_CONFIG_FILE,
        )
        >> download_resources()
        >> consolidate_resources(TMP_FOLDER)
        >> upload_consolidated_data(TMP_CONFIG_FILE)
        >> update_reference_tables()
        >> update_resources()
        >> update_consolidation_documentation(TMP_CONFIG_FILE)
        >> create_consolidation_reports()
        >> create_consolidation_reports()
        >> create_detailed_reports()
        >> final_clean_up(
            tmp_path=TMP_FOLDER,
            output_data_folder=output_data_folder,
        )
        >> PythonOperator(
            task_id="upload_s3",
            python_callable=upload_s3,
            op_kwargs={
                "TMP_FOLDER": TMP_FOLDER.as_posix(),
                "S3_BUCKET_DATA_PIPELINE_OPEN": S3_BUCKET_DATA_PIPELINE_OPEN,
                "s3_output_filepath": f"schema/schemas_consolidation/{datetime.today().strftime('%Y-%m-%d')}",
            },
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
        >> PythonOperator(
            task_id="notification_synthese",
            python_callable=notification_synthese,
            op_kwargs={
                "S3_URL": MINIO_URL,
                "S3_BUCKET_DATA_PIPELINE_OPEN": S3_BUCKET_DATA_PIPELINE_OPEN,
                "TMP_FOLDER": TMP_FOLDER,
                "MATTERMOST_DATAGOUV_SCHEMA_ACTIVITE": MATTERMOST_DATAGOUV_SCHEMA_ACTIVITE,
                "list_schema_skip": ["etalab/schema-irve-statique"],
            },
        )
        >> clean_up_folder(TMP_FOLDER.as_posix())
    )
