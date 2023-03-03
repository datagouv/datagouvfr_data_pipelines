from airflow.models import DAG
from operators.papermill_minio import PapermillMinioOperator
from airflow.operators.bash import BashOperator
from datetime import timedelta, datetime
from airflow.utils.dates import days_ago
from dag_datagouv_data_pipelines.config import (
    AIRFLOW_DAG_HOME,
    AIRFLOW_DAG_TMP,
    MINIO_URL,
    MINIO_BUCKET_DATA_PIPELINE_OPEN,
    SECRET_MINIO_DATA_PIPELINE_USER,
    SECRET_MINIO_DATA_PIPELINE_PASSWORD
)

DAG_NAME = "schema_recommendations"
TMP_FOLDER = f"{AIRFLOW_DAG_TMP}{DAG_NAME}/"
GIT_REPO = "git@github.com:etalab/schema.data.gouv.fr.git"

default_args = {"email": ["geoffrey.aldebert@data.gouv.fr"], "email_on_failure": True}

with DAG(
    dag_id=DAG_NAME,
    schedule_interval="0 4 * * *",
    start_date=days_ago(1),
    dagrun_timeout=timedelta(minutes=60),
    tags=["schemas", "recommendation", "schema.data.gouv.fr"],
    default_args=default_args,
) as dag:
    clean_previous_outputs = BashOperator(
        task_id="clean_previous_outputs",
        bash_command=f"rm -rf {TMP_FOLDER} && mkdir -p {TMP_FOLDER}",
    )

    clone_schema_repo = BashOperator(
        task_id="clone_schema_repo",
        bash_command=f"cd {TMP_FOLDER} && git clone {GIT_REPO} ",
    )

    run_nb = PapermillMinioOperator(
        task_id="run_notebook_schemas_recommendations",
        input_nb=(
            f"{AIRFLOW_DAG_HOME}dag_datagouv_data_pipelines/schema/"
            "notebooks/schemas_recommendations.ipynb"
        ),
        output_nb="{{ ds }}.ipynb",
        tmp_path=f"{TMP_FOLDER}",
        minio_url=MINIO_URL,
        minio_bucket=MINIO_BUCKET_DATA_PIPELINE_OPEN,
        minio_user=SECRET_MINIO_DATA_PIPELINE_USER,
        minio_password=SECRET_MINIO_DATA_PIPELINE_PASSWORD,
        minio_output_filepath="schema/schemas_recommendations/{{ ds }}/",
        parameters={
            "msgs": "Ran from Airflow {{ ds }} !",
            "WORKING_DIR": f"{AIRFLOW_DAG_HOME}{DAG_NAME}",
            "TMP_FOLDER": f"{TMP_FOLDER}",
            "OUTPUT_DATA_FOLDER": f"{TMP_FOLDER}output/",
            "DATE_AIRFLOW": "{{ ds }}",
        },
    )

    copy_recommendations = BashOperator(
        task_id="copy_recommendations",
        bash_command=(
            f"cd {TMP_FOLDER} "
            " && rm -rf schema.data.gouv.fr/site/.vuepress/public/api"
            " && mkdir schema.data.gouv.fr/site/.vuepress/public/api"
            " && mv recommendations.json ./schema.data.gouv.fr/site/.vuepress/public/api/"
        ),
    )

    commit_changes = BashOperator(
        task_id="commit_changes",
        bash_command=(
            f"cd {TMP_FOLDER}schema.data.gouv.fr"
            " && git add site/.vuepress/public/api"
            ' && git commit -m "Update Recommendations '
            f'{datetime.today().strftime("%Y-%m-%d")}'
            '" || echo "No changes to commit"'
            " && git push origin main"
        ),
    )

    clone_schema_repo.set_upstream(clean_previous_outputs)
    run_nb.set_upstream(clone_schema_repo)
    copy_recommendations.set_upstream(run_nb)
    commit_changes.set_upstream(copy_recommendations)
