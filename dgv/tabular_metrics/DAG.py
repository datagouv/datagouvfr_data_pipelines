from datetime import timedelta, datetime
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator

from datagouvfr_data_pipelines.dgv.tabular_metrics.task_functions import (
    DATADIR,
    create_tabular_metrics_table,
    find_logs_to_process,
    process_logs,
)

DAG_NAME = "dgv_tabular_metrics"

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_NAME,
    schedule_interval=None,
    start_date=datetime(2025, 11, 1),
    catchup=False,
    dagrun_timeout=None,  # the first run will catch up and run for a while, then we'll set this properly
    tags=["datagouv", "stats", "metrics", "tabular"],
    default_args=default_args,
) as dag:
    clean_previous_outputs = BashOperator(
        task_id="clean_previous_outputs",
        bash_command=(f"rm -rf {DATADIR} && mkdir -p {DATADIR}"),
    )

    create_tabular_metrics_table = PythonOperator(
        task_id="create_tabular_metrics_table",
        python_callable=create_tabular_metrics_table,
    )

    find_logs_to_process = ShortCircuitOperator(
        task_id="find_logs_to_process",
        python_callable=find_logs_to_process,
    )

    process_logs = PythonOperator(
        task_id="process_logs",
        python_callable=process_logs,
    )

    clean_up = BashOperator(
        task_id="clean_up",
        bash_command=f"rm -rf {DATADIR}",
    )

    create_tabular_metrics_table.set_upstream(clean_previous_outputs)
    find_logs_to_process.set_upstream(create_tabular_metrics_table)
    process_logs.set_upstream(find_logs_to_process)
    clean_up.set_upstream(process_logs)
