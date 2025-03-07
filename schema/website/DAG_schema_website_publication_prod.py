from datetime import timedelta, datetime
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

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

DAG_NAME = "schema_website_publication_prod"
TMP_FOLDER = f"{AIRFLOW_DAG_TMP}{DAG_NAME}/"
GIT_REPO = (
    ("git@github.com:" if AIRFLOW_ENV == "prod" else "https://github.com/")
    + "datagouv/schema.data.gouv.fr.git"
)

default_args = {
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    dag_id=DAG_NAME,
    schedule_interval="0 2 * * *",
    start_date=datetime(2024, 8, 10),
    dagrun_timeout=timedelta(minutes=60),
    tags=["schemas", "backend", "prod", "schema.data.gouv.fr"],
    catchup=False,
    default_args=default_args,
) as dag:
    clean_previous_outputs = BashOperator(
        task_id="clean_previous_outputs",
        bash_command=f"rm -rf {TMP_FOLDER} && mkdir -p {TMP_FOLDER} ",
    )

    clone_schema_repo = BashOperator(
        task_id="clone_schema_repo",
        bash_command=f"cd {TMP_FOLDER} && git clone --depth 1 {GIT_REPO} ",
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

    publish_schema_dataset = PythonOperator(
        task_id="publish_schema_dataset",
        python_callable=publish_schema_dataset,
        op_kwargs={
            "TMP_FOLDER": TMP_FOLDER,
            "AIRFLOW_ENV": AIRFLOW_ENV,
        },
    )

    final_clean_up = PythonOperator(
        task_id="final_clean_up",
        python_callable=final_clean_up,
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
            " && git push origin main"
        ),
    )

    clone_schema_repo.set_upstream(clean_previous_outputs)
    initialization.set_upstream(clone_schema_repo)
    check_and_save_schemas.set_upstream(initialization)
    update_news_feed.set_upstream(check_and_save_schemas)
    sort_folders.set_upstream(update_news_feed)
    get_issues_and_labels.set_upstream(sort_folders)
    publish_schema_dataset.set_upstream(get_issues_and_labels)
    final_clean_up.set_upstream(publish_schema_dataset)
    copy_files.set_upstream(final_clean_up)
    commit_changes.set_upstream(copy_files)
