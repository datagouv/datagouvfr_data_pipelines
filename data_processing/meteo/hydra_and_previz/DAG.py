# from datetime import timedelta, datetime
# from airflow.models import DAG
# from airflow.operators.python import PythonOperator

# from datagouvfr_data_pipelines.data_processing.meteo.hydra_and_previz.task_functions import (
#     get_and_send_errors,
# )

# DAG_NAME = "data_processing_meteo_hydra_and_previz"

# default_args = {
#     "retries": 5,
#     "retry_delay": timedelta(minutes=5),
# }

# with DAG(
#     dag_id=DAG_NAME,
#     schedule="5 4 * * 1",
#     start_date=datetime(2024, 6, 1),
#     catchup=False,
#     dagrun_timeout=timedelta(minutes=30),
#     tags=["data_processing", "meteo", "hydra"],
#     default_args=default_args,
# ):
#     get_and_send_errors = PythonOperator(
#         task_id="get_and_send_errors",
#         python_callable=get_and_send_errors,
#     )

#     get_and_send_errors
