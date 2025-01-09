from datetime import timedelta, datetime
from pathlib import Path
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_HOME,
    AIRFLOW_DAG_TMP,
    MINIO_URL,
    MINIO_BUCKET_DATA_PIPELINE_OPEN,
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
from datagouvfr_data_pipelines.utils.schema import (
    upload_minio,
    notification_synthese
)
from datagouvfr_data_pipelines.utils.datagouv import DATAGOUV_URL

# for local dev in order not to mess up with production
# DATAGOUV_URL = 'https://data.gouv.fr'

DAG_NAME = "schema_consolidation"
TMP_FOLDER = Path(f"{AIRFLOW_DAG_TMP}{DAG_NAME}/")
TMP_CONFIG_FILE = TMP_FOLDER / "schema.data.gouv.fr/config_consolidation.yml"
SCHEMA_CATALOG = "https://schema.data.gouv.fr/schemas/schemas.json"
API_URL = f"{DATAGOUV_URL}/api/1/"
GIT_REPO = "git@github.com:etalab/schema.data.gouv.fr.git"
# for local dev without SSH enabled
# GIT_REPO = "https://github.com/etalab/schema.data.gouv.fr.git"
output_data_folder = f"{TMP_FOLDER}/output/"

default_args = {
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    dag_id=DAG_NAME,
    schedule_interval="0 5 * * *",
    start_date=datetime(2024, 8, 10),
    dagrun_timeout=timedelta(minutes=240),
    tags=["schemas", "consolidation", "datagouv"],
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

    working_dir = f"{AIRFLOW_DAG_HOME}datagouvfr_data_pipelines/schema/scripts/"

    get_resources = PythonOperator(
        task_id="get_resources",
        python_callable=get_resources,
        op_kwargs={
            "tmp_path": TMP_FOLDER,
            "schema_catalog_url": SCHEMA_CATALOG,
            "config_path": TMP_CONFIG_FILE,
        },
    )

    download_resources = PythonOperator(
        task_id="download_resources",
        python_callable=download_resources,
    )

    consolidate_resources = PythonOperator(
        task_id="consolidate_resources",
        python_callable=consolidate_resources,
        op_kwargs={
            "tmp_path": TMP_FOLDER,
        },
    )

    upload_consolidated_data = PythonOperator(
        task_id="upload_consolidated_data",
        python_callable=upload_consolidated_data,
        op_kwargs={
            "config_path": TMP_CONFIG_FILE,
        },
    )

    update_reference_tables = PythonOperator(
        task_id="update_reference_tables",
        python_callable=update_reference_tables,
    )

    update_resources = PythonOperator(
        task_id="update_resources",
        python_callable=update_resources,
    )

    update_consolidation_documentation = PythonOperator(
        task_id="update_consolidation_documentation",
        python_callable=update_consolidation_documentation,
        op_kwargs={
            "config_path": TMP_CONFIG_FILE,
        },
    )

    create_consolidation_reports = PythonOperator(
        task_id="create_consolidation_reports",
        python_callable=create_consolidation_reports,
    )

    create_detailed_reports = PythonOperator(
        task_id="create_detailed_reports",
        python_callable=create_detailed_reports,
    )

    final_clean_up = PythonOperator(
        task_id="final_clean_up",
        python_callable=final_clean_up,
        op_kwargs={
            "tmp_path": TMP_FOLDER,
            "output_data_folder": output_data_folder,
        },
    )

    upload_minio = PythonOperator(
        task_id="upload_minio",
        python_callable=upload_minio,
        op_kwargs={
            "TMP_FOLDER": TMP_FOLDER.as_posix(),
            "MINIO_BUCKET_DATA_PIPELINE_OPEN": MINIO_BUCKET_DATA_PIPELINE_OPEN,
            "minio_output_filepath": f"schema/schemas_consolidation/{datetime.today().strftime('%Y-%m-%d')}",
        },
    )

    commit_changes = BashOperator(
        task_id="commit_changes",
        bash_command=(
            f"cd {TMP_FOLDER.as_posix()}/schema.data.gouv.fr/ && git add config_consolidation.yml "
            ' && git commit -m "Update config consolidation file - '
            f'{datetime.today().strftime("%Y-%m-%d")}'
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
            "MATTERMOST_DATAGOUV_SCHEMA_ACTIVITE": MATTERMOST_DATAGOUV_SCHEMA_ACTIVITE,
            "list_schema_skip": ['etalab/schema-irve-statique'],
        },
    )

    clone_dag_schema_repo.set_upstream(clean_previous_outputs)
    get_resources.set_upstream(clone_dag_schema_repo)
    download_resources.set_upstream(get_resources)
    consolidate_resources.set_upstream(download_resources)
    upload_consolidated_data.set_upstream(consolidate_resources)
    update_reference_tables.set_upstream(upload_consolidated_data)
    update_resources.set_upstream(update_reference_tables)
    update_consolidation_documentation.set_upstream(update_resources)
    create_consolidation_reports.set_upstream(update_consolidation_documentation)
    create_detailed_reports.set_upstream(create_consolidation_reports)
    final_clean_up.set_upstream(create_detailed_reports)
    upload_minio.set_upstream(final_clean_up)
    commit_changes.set_upstream(upload_minio)
    notification_synthese.set_upstream(commit_changes)
