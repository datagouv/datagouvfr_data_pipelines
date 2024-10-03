from datetime import timedelta
from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datagouvfr_data_pipelines.dgv.apigouv.task_functions import (
    import_api_to_grist,
    publish_api_to_datagouv,
    publish_api_to_datagouv,
    publish_mattermost,
)
from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
)

DAG_NAME = "dgv_migration_apigouv"
TMP_FOLDER = f"{AIRFLOW_DAG_TMP}migration_apigouv"

default_args = {
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_NAME,
    schedule_interval="0 18 * * *",
    start_date=days_ago(0),
    dagrun_timeout=timedelta(minutes=60),
    tags=["apigouv"],
    default_args=default_args,
    catchup=False,
) as dag:
    
    clean_previous_outputs = BashOperator(
        task_id="clean_previous_outputs",
        bash_command=f"rm -rf {TMP_FOLDER} && mkdir -p {TMP_FOLDER}",
    )

    clone_dag_apigouv_repo = BashOperator(
        task_id="clone_dag_apigouv_repo",
        bash_command=f"cd {TMP_FOLDER} && git clone https://github.com/betagouv/api.gouv.fr.git --depth 1 ",
    )

    import_api_to_grist = PythonOperator(
        task_id="import_api_to_grist",
        python_callable=import_api_to_grist,
    )

    publish_api_to_datagouv = PythonOperator(
        task_id="publish_api_to_datagouv",
        python_callable=publish_api_to_datagouv
    )

    publish_mattermost = PythonOperator(
        task_id="publish_mattermost",
        python_callable=publish_mattermost
    )

    clone_dag_apigouv_repo.set_upstream(clean_previous_outputs)
    import_api_to_grist.set_upstream(clone_dag_apigouv_repo)
    publish_api_to_datagouv.set_upstream(import_api_to_grist)
    publish_mattermost.set_upstream(publish_api_to_datagouv)
