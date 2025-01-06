from datetime import timedelta, datetime
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datagouvfr_data_pipelines.dgv.stats.task_functions import (
    TMP_FOLDER,
    DATADIR,
    create_current_year_if_missing,
    update_current_year,
)

DAG_NAME = 'dgv_stats'

default_args = {
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_NAME,
    schedule_interval='55 23 * * *',
    start_date=datetime(2023, 10, 15),
    catchup=False,
    dagrun_timeout=timedelta(minutes=20),
    tags=["datagouv", "stats", "metrics"],
    default_args=default_args,
) as dag:

    clean_previous_outputs = BashOperator(
        task_id="clean_previous_outputs",
        bash_command=(
            f"rm -rf {TMP_FOLDER} && "
            f"mkdir -p {TMP_FOLDER} && "
            f"mkdir -p {DATADIR}"
        ),
    )

    create_current_year_if_missing = PythonOperator(
        task_id='create_current_year_if_missing',
        python_callable=create_current_year_if_missing,
    )

    update_current_year = PythonOperator(
        task_id='update_current_year',
        python_callable=update_current_year,
    )

    clean_up = BashOperator(
        task_id="clean_up",
        bash_command=f"rm -rf {TMP_FOLDER}",
    )

    create_current_year_if_missing.set_upstream(clean_previous_outputs)
    update_current_year.set_upstream(clean_previous_outputs)
    update_current_year.set_upstream(create_current_year_if_missing)
    clean_up.set_upstream(update_current_year)
