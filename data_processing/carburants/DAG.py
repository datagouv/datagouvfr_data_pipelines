from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
)
from datagouvfr_data_pipelines.data_processing.carburants.task_functions import (
    convert_utf8_files,
    download_latest_data,
    generate_latest_france,
    generate_rupture_france,
    get_daily_prices,
    reformat_file,
    send_files_s3,
    unzip_files,
)

TMP_FOLDER = f"{AIRFLOW_DAG_TMP}carburants/"

with DAG(
    dag_id="data_processing_carburants",
    schedule_interval="5,35 4-20 * * *",
    start_date=datetime(2024, 8, 10),
    dagrun_timeout=timedelta(minutes=15),
    tags=["carburants", "prix", "rupture"],
    params={},
    catchup=False,
) as dag:
    clean_previous_outputs = BashOperator(
        task_id="clean_previous_outputs",
        bash_command=f"rm -rf {TMP_FOLDER} && mkdir -p {TMP_FOLDER}",
    )

    download_latest_data = PythonOperator(
        task_id="download_latest_data", python_callable=download_latest_data
    )

    get_daily_prices = PythonOperator(
        task_id="get_daily_prices", python_callable=get_daily_prices
    )

    unzip_files = PythonOperator(task_id="unzip_files", python_callable=unzip_files)

    convert_utf8_files = PythonOperator(
        task_id="convert_utf8_files", python_callable=convert_utf8_files
    )

    reformat_file = PythonOperator(
        task_id="reformat_file", python_callable=reformat_file
    )

    generate_latest_france = PythonOperator(
        task_id="generate_latest_france", python_callable=generate_latest_france
    )

    generate_rupture_france = PythonOperator(
        task_id="generate_rupture_france", python_callable=generate_rupture_france
    )

    send_files_s3 = PythonOperator(
        task_id="send_files_s3", python_callable=send_files_s3
    )

    download_latest_data.set_upstream(clean_previous_outputs)
    get_daily_prices.set_upstream(clean_previous_outputs)

    unzip_files.set_upstream(download_latest_data)
    convert_utf8_files.set_upstream(unzip_files)
    reformat_file.set_upstream(convert_utf8_files)

    generate_latest_france.set_upstream(reformat_file)
    generate_latest_france.set_upstream(get_daily_prices)

    generate_rupture_france.set_upstream(reformat_file)

    send_files_s3.set_upstream(generate_latest_france)
    send_files_s3.set_upstream(generate_rupture_france)
