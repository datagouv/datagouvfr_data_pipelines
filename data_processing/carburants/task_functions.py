from datetime import date
import glob
import os
import zipfile
from airflow.operators.bash import BashOperator

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
    MINIO_BUCKET_DATA_PIPELINE_OPEN,
)
from datagouvfr_data_pipelines.utils.download import download_files
from datagouvfr_data_pipelines.data_processing.carburants.scripts.generate_kpis_and_files import (
    generate_kpis
)
from datagouvfr_data_pipelines.data_processing.carburants.scripts.generate_kpis_rupture import (
    generate_kpis_rupture
)
from datagouvfr_data_pipelines.data_processing.carburants.scripts.reformat_prix import (
    reformat_prix,
)
from datagouvfr_data_pipelines.utils.minio import MinIOClient

minio_open = MinIOClient(bucket=MINIO_BUCKET_DATA_PIPELINE_OPEN)


def download_latest_data():
    download_files(
        [
            {
                "url": "https://donnees.roulez-eco.fr/opendata/jour",
                "dest_path": f"{AIRFLOW_DAG_TMP}carburants/",
                "dest_name": "jour.zip",
            },
            {
                "url": "https://donnees.roulez-eco.fr/opendata/instantane",
                "dest_path": f"{AIRFLOW_DAG_TMP}carburants/",
                "dest_name": "instantane.zip",
            },
        ]
    )


def get_daily_prices():
    minio_open.download_files(
        list_files=[
            {
                "source_path": "carburants/",
                "source_name": "daily_prices.json",
                "dest_path": f"{AIRFLOW_DAG_TMP}carburants/",
                "dest_name": "daily_prices.json",
            }
        ],
    )


def unzip_files(ti):
    with zipfile.ZipFile(f"{AIRFLOW_DAG_TMP}carburants/jour.zip", mode="r") as z:
        z.extractall(f"{AIRFLOW_DAG_TMP}carburants/")
    with zipfile.ZipFile(f"{AIRFLOW_DAG_TMP}carburants/instantane.zip", mode="r") as z:
        z.extractall(f"{AIRFLOW_DAG_TMP}carburants/")

    file_instantane = glob.glob(f"{AIRFLOW_DAG_TMP}carburants/*instantane*.xml")[0]
    file_jour = glob.glob(f"{AIRFLOW_DAG_TMP}carburants/*quotidien*.xml")[0]

    new_file_instantane = f"{AIRFLOW_DAG_TMP}carburants/latest.xml"
    new_file_jour = f"{AIRFLOW_DAG_TMP}carburants/quotidien.xml"

    os.rename(file_instantane, new_file_instantane)
    os.rename(file_jour, new_file_jour)

    ti.xcom_push(key="files", value=[new_file_instantane, new_file_jour])


def convert_utf8_files(ti):
    files = ti.xcom_pull(key="files", task_ids="unzip_files")
    files_converted = []
    for file in files:
        file_name = f"{file.replace('.xml', '')}_utf8.xml"
        files_converted.append(file_name)
        convert_file = BashOperator(
            task_id="convert_file",
            bash_command=(f"iconv -f iso-8859-1 -t utf-8 " f"{file} >| {file_name}"),
        )
        convert_file.execute(dict())

    ti.xcom_push(key="files", value=files_converted)


def reformat_file(ti):
    files = ti.xcom_pull(key="files", task_ids="convert_utf8_files")
    for file in files:
        reformat_prix(
            file,
            f"{AIRFLOW_DAG_TMP}carburants/",
            file.split("/")[-1].replace("_utf8.xml", ""),
        )


def generate_latest_france():
    generate_kpis(f"{AIRFLOW_DAG_TMP}carburants/")


def generate_rupture_france():
    generate_kpis_rupture(f"{AIRFLOW_DAG_TMP}carburants/")


def send_files_minio():
    today = date.today().strftime("%Y-%m-%d")

    minio_open.send_files(
        list_files=[
            {
                "source_path": f"{AIRFLOW_DAG_TMP}carburants/",
                "source_name": name,
                "dest_path": f"carburants/{folder}",
                "dest_name": name,
                "content_type": (
                    "application/json; charset=utf-8" if name.endswith("json")
                    else "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                ),
            } for name in [
                "latest_france.geojson",
                "latest_france_ruptures.geojson",
                "synthese_ruptures_latest.xlsx",
            ] for folder in ["", f"{today}/"]
        ] + [
            {
                "source_path": f"{AIRFLOW_DAG_TMP}carburants/",
                "source_name": "daily_prices.geojson",
                "dest_path": "carburants/",
                "dest_name": "daily_prices.geojson",
                "content_type": "application/json; charset=utf-8",
            },
        ],
    )
