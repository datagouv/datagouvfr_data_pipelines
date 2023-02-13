from airflow.models import DAG, Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from dag_datagouv_data_pipelines.dvf.task_functions import (
    create_dvf_table,
    create_stats_dvf_table,
    get_epci,
    populate_dvf_table,
    populate_stats_dvf_table,
    process_dvf_stats,
)

AIRFLOW_DAG_HOME = Variable.get("AIRFLOW_DAG_HOME")
TMP_FOLDER = '/tmp/'
DAG_FOLDER = 'dag_datagouv_data_pipelines/'
DAG_NAME = 'process_dvf_data'

default_args = {
    'email': ['geoffrey.aldebert@data.gouv.fr'],
    'email_on_failure': True
}

with DAG(
    dag_id=DAG_NAME,
    schedule_interval='15 7 1 * *',
    start_date=days_ago(1),
    dagrun_timeout=timedelta(minutes=60),
    tags=[],
    default_args=default_args,
) as dag:

    download_dvf_data = BashOperator(
        task_id='download_dvf_data',
        bash_command=f"sh {AIRFLOW_DAG_HOME}{DAG_FOLDER}"
        "dvf/scripts/script_dl_dvf.sh "
    )

    create_dvf_table = PythonOperator(
        task_id='create_dvf_table',
        python_callable=create_dvf_table,
    )

    populate_dvf_table = PythonOperator(
        task_id='populate_dvf_table',
        python_callable=populate_dvf_table,
    )

    get_epci = PythonOperator(
        task_id='get_epci',
        python_callable=get_epci,
    )

    process_dvf_stats = PythonOperator(
        task_id='process_dvf_stats',
        python_callable=process_dvf_stats,
    )

    create_stats_dvf_table = PythonOperator(
        task_id='create_stats_dvf_table',
        python_callable=create_stats_dvf_table,
    )

    populate_stats_dvf_table = PythonOperator(
        task_id='populate_stats_dvf_table',
        python_callable=populate_stats_dvf_table,
    )

    create_dvf_table.set_upstream(download_dvf_data)
    populate_dvf_table.set_upstream(create_dvf_table)
    get_epci.set_upstream(populate_dvf_table)
    process_dvf_stats.set_upstream(get_epci)
    create_stats_dvf_table.set_upstream(process_dvf_stats)
    populate_stats_dvf_table.set_upstream(create_stats_dvf_table)
