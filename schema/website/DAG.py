from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
    AIRFLOW_ENV,
)
from datagouvfr_data_pipelines.schema.website.task_functions import (
    initialization,
    check_and_save_schemas,
    update_news_feed,
    sort_folders,
    get_issues_and_labels,
    publish_schema_dataset,
    final_clean_up,
)

GIT_REPO = (
    "git@github.com:" if AIRFLOW_ENV == "prod" else "https://github.com/"
) + "datagouv/schema.data.gouv.fr.git"

default_args = {
    "retries": 5,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="schema_website_publication",
    schedule="0 2 * * *",
    start_date=datetime(2024, 8, 10),
    dagrun_timeout=timedelta(minutes=60),
    tags=["schemas", "backend", "schema.data.gouv.fr"],
    catchup=False,
    default_args=default_args,
):
    branches = ["main", "preprod"]
    tasks = {}
    for branch in branches:
        suffix = "_prod" if branch == "main" else f"_{branch}"
        tmp_folder = f"{AIRFLOW_DAG_TMP}schema_website_publication{suffix}/"
        (
            BashOperator(
                task_id="clean_previous_outputs" + suffix,
                bash_command=f"rm -rf {tmp_folder} && mkdir -p {tmp_folder} ",
            )
            >> BashOperator(
                task_id="clone_schema_repo" + suffix,
                bash_command=(
                    f"cd {tmp_folder} && git clone --depth 1 {GIT_REPO} "
                    + (f"-b {branch} " if branch != "main" else "")
                ),
            )
            >> initialization.override(task_id="initialization" + suffix)(
                tmp_folder=tmp_folder,
                branch=branch,
            )
            >> check_and_save_schemas.override(task_id="check_and_save_schemas" + suffix)(
                suffix=suffix,
            )
            >> update_news_feed.override(task_id="update_news_feed" + suffix)(
                tmp_folder=tmp_folder,
                suffix=suffix,
            )
            >> sort_folders.override(task_id="sort_folders" + suffix)(
                suffix=suffix,
            )
            >> get_issues_and_labels.override(task_id="get_issues_and_labels" + suffix)(
                suffix=suffix,
            )
            >> publish_schema_dataset.override(task_id="publish_schema_dataset" + suffix)(
                tmp_folder=tmp_folder,
                AIRFLOW_ENV=AIRFLOW_ENV,
                branch=branch,
                suffix=suffix,
            )
            >> final_clean_up.override(task_id="final_clean_up" + suffix)(
                suffix=suffix,
            )
            >> BashOperator(
                task_id="copy_files" + suffix,
                bash_command=(
                    f"cd {tmp_folder}"
                    " && mkdir site"
                    " && cp -r schema.data.gouv.fr/site/*.md ./site/"
                    " && cp -r schema.data.gouv.fr/site/.vuepress/ ./site/"
                    " && rm -rf ./site/.vuepress/public/schemas"
                    " && mkdir ./site/.vuepress/public/schemas"
                    " && cp -r data/* ./site/ "
                    " && cp -r data2/* ./site/.vuepress/public/schemas"
                    " && cp ./site/.vuepress/public/schemas/*.json ./site/.vuepress/public/"
                    " && rm -rf ./schema.data.gouv.fr/site"
                    " && mv ./site ./schema.data.gouv.fr/"
                ),
            )
            >> BashOperator(
                task_id="commit_changes" + suffix,
                bash_command=(
                    f"cd {tmp_folder}schema.data.gouv.fr"
                    " && git add site/"
                    ' && git commit -m "Update Website '
                    f"{datetime.today().strftime('%Y-%m-%d')}"
                    '" || echo "No changes to commit"'
                    f" && git push origin {branch}"
                ),
            )
            >> BashOperator(
                task_id="clean_up" + suffix,
                bash_command=f"rm -rf {tmp_folder}",
            )
        )
