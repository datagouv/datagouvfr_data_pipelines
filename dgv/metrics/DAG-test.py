from airflow.models import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from operators.clean_folder import CleanFolderOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
)
from datagouvfr_data_pipelines.dgv.metrics.task_functions import (
    create_metrics_tables,
)

TMP_FOLDER = f"{AIRFLOW_DAG_TMP}metrics/"
DAG_NAME = 'dgv_metrics_test'

default_args = {
    'email': ['geoffrey.aldebert@data.gouv.fr'],
    'email_on_failure': True
}

with DAG(
    dag_id=DAG_NAME,
    schedule_interval='15 7 1 * *',
    start_date=days_ago(1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    tags=["dgv", "metrics"],
    default_args=default_args,
) as dag:

    create_metrics_tables = PythonOperator(
        task_id='create_metrics_tables',
        python_callable=create_metrics_tables,
    )

    create_metrics_tables
