from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from datagouvfr_data_pipelines.config import AIRFLOW_ENV
from datagouvfr_data_pipelines.schema.recommendations.task_functions import (
    TMP_FOLDER,
    create_and_export_recommendations,
)
from datagouvfr_data_pipelines.utils.tasks import clean_up_folder

GIT_REPO = (
    "git@github.com:" if AIRFLOW_ENV == "prod" else "https://github.com/"
) + "datagouv/schema.data.gouv.fr.git"

default_args = {"email": ["geoffrey.aldebert@data.gouv.fr"], "email_on_failure": False}

with DAG(
    dag_id="schema_recommendations",
    schedule="0 4 * * *",
    start_date=datetime(2024, 8, 10),
    dagrun_timeout=timedelta(minutes=20),
    tags=["schemas", "recommendation", "schema.data.gouv.fr"],
    catchup=False,
    default_args=default_args,
):
    (
        clean_up_folder(TMP_FOLDER, recreate=True)
        >> BashOperator(
            task_id="clone_schema_repo",
            bash_command=f"cd {TMP_FOLDER} && git clone {GIT_REPO} ",
        )
        >> create_and_export_recommendations()
        >> BashOperator(
            task_id="copy_recommendations",
            bash_command=(
                f"cd {TMP_FOLDER} "
                " && rm -rf schema.data.gouv.fr/site/.vuepress/public/api"
                " && mkdir schema.data.gouv.fr/site/.vuepress/public/api"
                " && mv recommendations.json ./schema.data.gouv.fr/site/.vuepress/public/api/"
            ),
        )
        >> BashOperator(
            task_id="commit_changes",
            bash_command=(
                f"cd {TMP_FOLDER}schema.data.gouv.fr"
                " && git add site/.vuepress/public/api"
                ' && git commit -m "Update Recommendations '
                f"{datetime.today().strftime('%Y-%m-%d')}"
                '" || echo "No changes to commit"'
                " && git push origin main"
            ),
        )
        >> clean_up_folder(TMP_FOLDER)
    )
