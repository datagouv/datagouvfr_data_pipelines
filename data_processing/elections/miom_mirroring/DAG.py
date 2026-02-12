from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator

from datagouvfr_data_pipelines.config import AIRFLOW_DAG_TMP
from datagouvfr_data_pipelines.data_processing.elections.miom_mirroring.task_functions import (
    TMP_FOLDER,
    get_files_updated_miom,
    download_local_files,
    send_to_s3,
    send_exports_to_s3,
    download_from_s3,
    check_if_continue,
    create_candidats_files,
    publish_results_elections,
    create_resultats_files,
)

DAG_NAME = "data_mirroring_elections"
ID_CURRENT_ELECTION = "LG2024"

default_args = {
    "email": ["pierlou.ramade@data.gouv.fr", "geoffrey.aldebert@data.gouv.fr"],
    "email_on_failure": False,
}

# with DAG(
#     dag_id=DAG_NAME,
#     schedule="15 7 1 1 *",
#     start_date=datetime(2024, 8, 10),
#     catchup=False,
#     dagrun_timeout=timedelta(minutes=240),
#     tags=["data_processing", "election", "miroir", "miom"],
#     default_args=default_args,
# ):
#     clean_previous_outputs = BashOperator(
#         task_id="clean_previous_outputs",
#         bash_command=f"rm -rf {TMP_FOLDER} && mkdir -p {TMP_FOLDER}",
#     )

#     get_files_updated_miom = PythonOperator(
#         task_id="get_files_updated_miom",
#         python_callable=get_files_updated_miom,
#     )

#     check_if_continue = ShortCircuitOperator(
#         task_id="check_if_continue",
#         python_callable=check_if_continue,
#     )

#     download_local_files = PythonOperator(
#         task_id="download_local_files",
#         python_callable=download_local_files,
#     )

#     send_to_s3 = PythonOperator(
#         task_id="send_to_s3",
#         python_callable=send_to_s3,
#     )

#     download_from_s3 = PythonOperator(
#         task_id="download_from_s3",
#         python_callable=download_from_s3,
#     )

#     zip_folder = BashOperator(
#         task_id="zip_folder",
#         bash_command=f"cd {AIRFLOW_DAG_TMP}elections-mirroring/ && zip -r {ID_CURRENT_ELECTION}.zip ./export/ ",
#     )

#     create_candidats_files = PythonOperator(
#         task_id="create_candidats_files",
#         python_callable=create_candidats_files,
#     )

#     create_resultats_files = PythonOperator(
#         task_id="create_resultats_files",
#         python_callable=create_resultats_files,
#     )

#     send_exports_to_s3 = PythonOperator(
#         task_id="send_exports_to_s3",
#         python_callable=send_exports_to_s3,
#     )

#     publish_results_elections = PythonOperator(
#         task_id="publish_results_elections",
#         python_callable=publish_results_elections,
#     )

#     get_files_updated_miom.set_upstream(clean_previous_outputs)
#     check_if_continue.set_upstream(get_files_updated_miom)
#     download_local_files.set_upstream(check_if_continue)
#     send_to_s3.set_upstream(download_local_files)
#     download_from_s3.set_upstream(send_to_s3)
#     zip_folder.set_upstream(download_from_s3)
#     create_candidats_files.set_upstream(zip_folder)
#     create_resultats_files.set_upstream(create_candidats_files)
#     send_exports_to_s3.set_upstream(create_resultats_files)
#     publish_results_elections.set_upstream(send_exports_to_s3)
