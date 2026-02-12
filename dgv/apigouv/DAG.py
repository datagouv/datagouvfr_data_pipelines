# from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.operators.bash import BashOperator

# from datagouvfr_data_pipelines.dgv.apigouv.task_functions import (
#     import_api_to_grist,
#     publish_api_to_datagouv,
#     publish_mattermost,
# )
# from datagouvfr_data_pipelines.config import (
#     AIRFLOW_DAG_TMP,
# )
# from datagouvfr_data_pipelines.utils.tasks import clean_up_folder

# DAG_NAME = "dgv_migration_apigouv"
# TMP_FOLDER = f"{AIRFLOW_DAG_TMP}migration_apigouv"

# default_args = {
#     "retries": 0,
#     "retry_delay": timedelta(minutes=5),
# }

# with DAG(
#     dag_id=DAG_NAME,
#     schedule="0 18 * * *",
#     start_date=datetime(2024, 10, 1),
#     dagrun_timeout=timedelta(minutes=60),
#     tags=["apigouv"],
#     default_args=default_args,
#     catchup=False,
# ):
# (
#     clean_up_folder(TMP_FOLDER, recreate=True)
#     >> BashOperator(
#         task_id="clone_dag_apigouv_repo",
#         bash_command=f"cd {TMP_FOLDER} && git clone https://github.com/betagouv/api.gouv.fr.git --depth 1 ",
#     )
#     >> import_api_to_grist()
#     >> publish_api_to_datagouv()
#     >> publish_mattermost()
# )
