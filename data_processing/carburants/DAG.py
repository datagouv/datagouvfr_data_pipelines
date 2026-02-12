from datetime import datetime, timedelta
from airflow import DAG

from datagouvfr_data_pipelines.data_processing.carburants.task_functions import (
    TMP_FOLDER,
    convert_utf8_files,
    download_latest_data,
    generate_latest_france,
    generate_rupture_france,
    get_daily_prices,
    reformat_file,
    send_files_s3,
    unzip_files,
)
from datagouvfr_data_pipelines.utils.tasks import clean_up_folder

with DAG(
    dag_id="data_processing_carburants",
    schedule="5,35 4-20 * * *",
    start_date=datetime(2024, 8, 10),
    dagrun_timeout=timedelta(minutes=15),
    tags=["carburants", "prix", "rupture"],
    catchup=False,
):
    
    _clean_up = clean_up_folder(TMP_FOLDER, recreate=True)
    _generate_latest_france = generate_latest_france()
    _send_files_s3 = send_files_s3()

    (
        _clean_up
        >> download_latest_data()
        >> unzip_files()
        >> convert_utf8_files()
        >> reformat_file()
        >> [generate_rupture_france(), _generate_latest_france]
        >> _send_files_s3
    )

    (
        _clean_up
        >> get_daily_prices()
        >> _generate_latest_france
        >> _send_files_s3
    )
