from airflow.models import DAG
from datetime import timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.dates import days_ago
from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
)
from datagouvfr_data_pipelines.data_processing.rne.database.task_functions import (
    get_start_date_minio,
    get_latest_db,
    create_db,
    process_flux_json_files,
)

TMP_FOLDER = f"{AIRFLOW_DAG_TMP}inpi/"

with DAG(
    dag_id='rne_dirigeants',
    start_date=days_ago(1),
    max_active_runs=1,
    catchup=False,
    dagrun_timeout=timedelta(minutes=(60 * 8)),
    tags=["data_processing", "inpi", "dirigeants"],
    params={},
) as dag:

    clean_previous_outputs = BashOperator(
        task_id='clean_previous_outputs',
        bash_command=f"rm -rf {TMP_FOLDER} && mkdir -p {TMP_FOLDER}"
    )
    
    get_start_date = PythonOperator(
        task_id="get_start_date",
        python_callable=get_start_date_minio
    )
    create_db = PythonOperator(
        task_id="create_db",
        python_callable=create_db
    )
    get_latest_db = PythonOperator(
        task_id="get_latest_db",
        python_callable=get_latest_db
    )
    process_flux_json_files = PythonOperator(
        task_id="process_flux_json_files",
        python_callable=process_flux_json_files
    )
    
    
    

  

    get_start_date.set_upstream(clean_previous_outputs)
    create_db.set_upstream(get_start_date)
    get_latest_db.set_upstream(create_db)
    process_flux_json_files.set_upstream(get_latest_db)

  
