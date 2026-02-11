from datetime import date
import glob
import os
import zipfile

from airflow.decorators import task
from airflow.operators.bash import BashOperator

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
    S3_BUCKET_DATA_PIPELINE_OPEN,
)
from datagouvfr_data_pipelines.data_processing.carburants.scripts import (
    generate_kpis,
    generate_kpis_rupture,
    reformat_prix,
)
from datagouvfr_data_pipelines.utils.filesystem import File
from datagouvfr_data_pipelines.utils.s3 import S3Client

s3_open = S3Client(bucket=S3_BUCKET_DATA_PIPELINE_OPEN)


@task()
def download_latest_data():
    File(
        url="https://donnees.roulez-eco.fr/opendata/jour",
        dest_path=f"{AIRFLOW_DAG_TMP}carburants/",
        dest_name="jour.zip",
    ).download()
    File(
        url="https://donnees.roulez-eco.fr/opendata/instantane",
        dest_path=f"{AIRFLOW_DAG_TMP}carburants/",
        dest_name="instantane.zip",
    ).download()


@task()
def get_daily_prices():
    s3_open.download_files(
        list_files=[
            File(
                source_path="carburants/",
                source_name="daily_prices.json",
                dest_path=f"{AIRFLOW_DAG_TMP}carburants/",
                dest_name="daily_prices.json",
                remote_source=True,
            ),
        ],
        ignore_airflow_env=True,
    )


@task()
def unzip_files(**context):
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

    context["ti"].xcom_push(key="files", value=[new_file_instantane, new_file_jour])


@task()
def convert_utf8_files(**context):
    files = context["ti"].xcom_pull(key="files", task_ids="unzip_files")
    files_converted = []
    for file in files:
        file_name = f"{file.replace('.xml', '')}_utf8.xml"
        files_converted.append(file_name)
        convert_file = BashOperator(
            task_id="convert_file",
            bash_command=(f"iconv -f iso-8859-1 -t utf-8 {file} >| {file_name}"),
        )
        convert_file.execute(dict())

    context["ti"].xcom_push(key="files", value=files_converted)


@task()
def reformat_file(**context):
    files = context["ti"].xcom_pull(key="files", task_ids="convert_utf8_files")
    for file in files:
        reformat_prix(
            file,
            f"{AIRFLOW_DAG_TMP}carburants/",
            file.split("/")[-1].replace("_utf8.xml", ""),
        )


@task()
def generate_latest_france():
    generate_kpis(f"{AIRFLOW_DAG_TMP}carburants/")


@task()
def generate_rupture_france():
    generate_kpis_rupture(f"{AIRFLOW_DAG_TMP}carburants/")


@task()
def send_files_s3():
    today = date.today().strftime("%Y-%m-%d")

    s3_open.send_files(
        list_files=[
            File(
                source_path=f"{AIRFLOW_DAG_TMP}carburants/",
                source_name=name,
                dest_path=f"carburants/{folder}",
                dest_name=name,
                content_type=(
                    "application/json; charset=utf-8"
                    if name.endswith("json")
                    else "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                ),
            )
            for name in [
                "latest_france.geojson",
                "latest_france_ruptures.json",
                "synthese_ruptures_latest.xlsx",
            ]
            for folder in ["", f"{today}/"]
        ]
        + [
            File(
                source_path=f"{AIRFLOW_DAG_TMP}carburants/",
                source_name="daily_prices.json",
                dest_path="carburants/",
                dest_name="daily_prices.json",
                content_type="application/json; charset=utf-8",
            ),
        ],
        ignore_airflow_env=True,
    )
