# from datetime import timedelta, datetime
# from airflow import DAG

# from datagouvfr_data_pipelines.data_processing.meteo.hydra_and_previz.task_functions import (
#     get_and_send_errors,
# )

# default_args = {
#     "retries": 5,
#     "retry_delay": timedelta(minutes=5),
# }

# with DAG(
#     dag_id="data_processing_meteo_hydra_and_previz",
#     schedule="5 4 * * 1",
#     start_date=datetime(2024, 6, 1),
#     catchup=False,
#     dagrun_timeout=timedelta(minutes=30),
#     tags=["data_processing", "meteo", "hydra"],
#     default_args=default_args,
# ):

#     get_and_send_errors()
