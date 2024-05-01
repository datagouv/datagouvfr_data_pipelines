from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from airflow.utils.dates import days_ago
from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
)
from datagouvfr_data_pipelines.schema.scripts.schema_website.task_functions import (
    initialization,
    check_and_save_schemas,
    update_news_feed,
    sort_folders,
    get_issues_and_labels,
    final_clean_up,
)

DAG_NAME = "schema_website_publication_preprod"
TMP_FOLDER = f"{AIRFLOW_DAG_TMP}{DAG_NAME}/"
GIT_REPO = "git@github.com:datagouv/schema.data.gouv.fr.git"
# GIT_REPO = "https://github.com/datagouv/schema.data.gouv.fr.git"

default_args = {"email": ["geoffrey.aldebert@data.gouv.fr"], "email_on_failure": False}


with DAG(
    dag_id=DAG_NAME,
    schedule_interval="0 2 * * *",
    start_date=days_ago(1),
    dagrun_timeout=timedelta(minutes=60),
    tags=["schemas", "backend", "prod", "schema.data.gouv.fr"],
    default_args=default_args,
) as dag:
    clean_previous_outputs = BashOperator(
        task_id="clean_previous_outputs",
        bash_command=f"rm -rf {TMP_FOLDER} && mkdir -p {TMP_FOLDER} ",
    )

    clone_schema_repo = BashOperator(
        task_id="clone_schema_repo",
        bash_command=f"cd {TMP_FOLDER} && git clone --depth 1 {GIT_REPO} -b preprod ",
    )

    initialization = PythonOperator(
        task_id="initialization",
        python_callable=initialization,
        op_kwargs={
            "TMP_FOLDER": TMP_FOLDER,
        },
    )

    check_and_save_schemas = PythonOperator(
        task_id="check_and_save_schemas",
        python_callable=check_and_save_schemas,
    )

    update_news_feed = PythonOperator(
        task_id="update_news_feed",
        python_callable=update_news_feed,
        op_kwargs={
            "TMP_FOLDER": TMP_FOLDER,
        },
    )

    sort_folders = PythonOperator(
        task_id="sort_folders",
        python_callable=sort_folders,
    )

    get_issues_and_labels = PythonOperator(
        task_id="get_issues_and_labels",
        python_callable=get_issues_and_labels,
    )

    final_clean_up = PythonOperator(
        task_id="final_clean_up",
        python_callable=final_clean_up,
    )

    copy_etalab_folder = BashOperator(
        task_id="copy_etalab_folder",
        bash_command=(
            f"cp -r {TMP_FOLDER}schema.data.gouv.fr/site/etalab/ "
            f"{TMP_FOLDER}schema.data.gouv.fr/site/datagouv/ "
        ),
    )

    copy_files = BashOperator(
        task_id="copy_files",
        bash_command=(
            f"cd {TMP_FOLDER}"
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

    commit_changes = BashOperator(
        task_id="commit_changes",
        bash_command=(
            f"cd {TMP_FOLDER}schema.data.gouv.fr"
            " && git add site/"
            ' && git commit -m "Update Website '
            f'{datetime.today().strftime("%Y-%m-%d")}'
            '" || echo "No changes to commit"'
            " && git push origin preprod"
        ),
    )

    clone_schema_repo.set_upstream(clean_previous_outputs)
    initialization.set_upstream(clone_schema_repo)
    check_and_save_schemas.set_upstream(initialization)
    update_news_feed.set_upstream(check_and_save_schemas)
    sort_folders.set_upstream(update_news_feed)
    get_issues_and_labels.set_upstream(sort_folders)
    final_clean_up.set_upstream(get_issues_and_labels)
    copy_etalab_folder.set_upstream(final_clean_up)
    copy_files.set_upstream(copy_etalab_folder)
    commit_changes.set_upstream(copy_files)
