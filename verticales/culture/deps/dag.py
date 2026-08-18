from datetime import datetime, timedelta
 
from airflow.sdk import DAG
from datagouvfr_data_pipelines.verticales.culture.deps.task_functions import (
    get_perimeter,
    notification,
    refresh_tops,
)
 
with DAG(
    dag_id="verticale_culture_deps",
    # very day at 2 a.m., offset by one hour from `verticale_culture`
    schedule="0 2 * * *",
    start_date=datetime(2026, 8, 1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=30),
    tags=["verticale", "culture", "deps"],
):
    get_perimeter() >> refresh_tops() >> notification()
 
