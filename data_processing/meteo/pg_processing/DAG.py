from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
)
from datagouvfr_data_pipelines.data_processing.meteo.pg_processing.task_functions import (
    create_tables_if_not_exists,
    retrieve_latest_processed_date,
    get_latest_ftp_processing,
    download_data,
    insert_latest_date_pg,
)

TMP_FOLDER = f"{AIRFLOW_DAG_TMP}meteo_pg"
DAG_NAME = 'postgres_meteo'
DATADIR = f"{AIRFLOW_DAG_TMP}meteo_pg/data/"


default_args = {
    'email': [
        'pierlou.ramade@data.gouv.fr',
        'geoffrey.aldebert@data.gouv.fr'
    ],
    'email_on_failure': False
}

# to be on the safe side, it's actually 100
MAX_CONNECTIONS = 90
DATASETS_TO_PROCESS = [
    "BASE/MENS",
    "BASE/DECAD",
    "BASE/DECADAGRO",
    "BASE/QUOT",
    "BASE/HOR",
    "BASE/MIN",
]


with DAG(
    dag_id=DAG_NAME,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=2000),
    tags=["data_processing", "meteo"],
    default_args=default_args,
) as dag:

    clean_previous_outputs = BashOperator(
        task_id="clean_previous_outputs",
        bash_command=f"rm -rf {TMP_FOLDER} && mkdir -p {DATADIR}",
    )

    create_tables_if_not_exists = PythonOperator(
        task_id='create_tables_if_not_exists',
        python_callable=create_tables_if_not_exists,
    )

    retrieve_latest_processed_date = PythonOperator(
        task_id='retrieve_latest_processed_date',
        python_callable=retrieve_latest_processed_date,
    )

    get_latest_ftp_processing = PythonOperator(
        task_id='get_latest_ftp_processing',
        python_callable=get_latest_ftp_processing,
    )

    process_data = []
    for dataset in DATASETS_TO_PROCESS:
        process_data.append(
            PythonOperator(
                task_id=f'process_data_{dataset.replace("/","_").lower()}',
                python_callable=download_data,
                op_kwargs={
                    "dataset_name": dataset,
                    "max_size": MAX_CONNECTIONS // len(DATASETS_TO_PROCESS)
                },
            )
        )

    insert_latest_date_pg = PythonOperator(
        task_id='insert_latest_date_pg',
        python_callable=insert_latest_date_pg,
    )

    create_tables_if_not_exists.set_upstream(clean_previous_outputs)
    retrieve_latest_processed_date.set_upstream(create_tables_if_not_exists)
    get_latest_ftp_processing.set_upstream(retrieve_latest_processed_date)

    for i in range(0, len(process_data)):
        process_data[i].set_upstream(get_latest_ftp_processing)
        insert_latest_date_pg.set_upstream(process_data[i])
