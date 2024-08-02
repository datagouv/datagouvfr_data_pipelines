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
    process_data,
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

DEPIDS = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35', '36', '37', '38', '39', '40', '41', '42', '43', '44', '45', '46', '47', '48', '49', '50', '51', '52', '53', '54', '55', '56', '57', '58', '59', '60', '61', '62', '63', '64', '65', '66', '67', '68', '69', '70', '71', '72', '73', '74', '75', '76', '77', '78', '79', '80', '81', '82', '83', '84', '85', '86', '87', '88', '89', '90', '91', '92', '93', '94', '95', '971', '972', '973', '974', '975', '984', '985', '986', '987', '988', '99']

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
    for depid in DEPIDS:
        process_data.append(
            PythonOperator(
                task_id=f'process_data_{depid}',
                python_callable=process_data,
                op_kwargs={
                    "depid": depid,
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

    for i in range(0,len(process_data)):
        process_data[i].set_upstream(get_latest_ftp_processing)
        insert_latest_date_pg.set_upstream(process_data[i])
