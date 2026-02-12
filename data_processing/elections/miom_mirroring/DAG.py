# from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.operators.bash import BashOperator
# from airflow.operators.python import ShortCircuitOperator

# from datagouvfr_data_pipelines.config import AIRFLOW_DAG_TMP
# from datagouvfr_data_pipelines.data_processing.elections.miom_mirroring.task_functions import (
#     TMP_FOLDER,
#     get_files_updated_miom,
#     download_local_files,
#     send_to_s3,
#     send_exports_to_s3,
#     download_from_s3,
#     check_if_continue,
#     create_candidats_files,
#     publish_results_elections,
#     create_resultats_files,
# )
# from datagouvfr_data_pipelines.utils.tasks import clean_up_folder

# DAG_NAME = "data_mirroring_elections"
# ID_CURRENT_ELECTION = "LG2024"

# default_args = {
#     "email": ["pierlou.ramade@data.gouv.fr", "geoffrey.aldebert@data.gouv.fr"],
#     "email_on_failure": False,
# }

# with DAG(
#     dag_id=DAG_NAME,
#     schedule="15 7 1 1 *",
#     start_date=datetime(2024, 8, 10),
#     catchup=False,
#     dagrun_timeout=timedelta(minutes=240),
#     tags=["data_processing", "election"],
#     default_args=default_args,
# ):

# (
#     clean_up_folder(TMP_FOLDER, recreate=True)
#     >> get_files_updated_miom()
#     >> ShortCircuitOperator(
#         task_id="check_if_continue",
#         python_callable=check_if_continue,
#     )
#     >> download_local_files()
#     >> send_to_s3()
#     >> download_from_s3()
#     >> BashOperator(
#         task_id="zip_folder",
#         bash_command=f"cd {AIRFLOW_DAG_TMP}elections-mirroring/ && zip -r {ID_CURRENT_ELECTION}.zip ./export/ ",
#     )
#     >> create_candidats_files()
#     >> create_resultats_files()
#     >> send_exports_to_s3()
#     >> publish_results_elections()
#     >> clean_up_folder(TMP_FOLDER)
# )
