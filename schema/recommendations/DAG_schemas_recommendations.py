from datetime import timedelta, datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from datagouvfr_data_pipelines.schema.recommendations.task_functions import (
    DAG_NAME,
    TMP_FOLDER,
    create_and_export_recommendations,
)

GIT_REPO = "git@github.com:etalab/schema.data.gouv.fr.git"
# GIT_REPO = "https://github.com/etalab/schema.data.gouv.fr.git"

default_args = {"email": ["geoffrey.aldebert@data.gouv.fr"], "email_on_failure": True}

with DAG(
    dag_id=DAG_NAME,
    schedule_interval="0 4 * * *",
    start_date=datetime(2024, 8, 10),
    dagrun_timeout=timedelta(minutes=20),
    tags=["schemas", "recommendation", "schema.data.gouv.fr"],
    catchup=False,
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

    create_and_export_recommendations = PythonOperator(
        task_id='create_and_export_recommendations',
        python_callable=create_and_export_recommendations,
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
    create_and_export_recommendations.set_upstream(clone_schema_repo)
    copy_recommendations.set_upstream(create_and_export_recommendations)
    commit_changes.set_upstream(copy_recommendations)
