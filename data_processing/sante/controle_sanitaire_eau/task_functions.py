import logging
import os
import re
from io import BytesIO
from zipfile import ZipFile

import pandas as pd
import requests
from airflow.sdk import task
from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
    S3_BUCKET_DATA_PIPELINE_OPEN,
)
from datagouvfr_data_pipelines.utils.conversions import (
    csv_to_csvgz,
)
from datagouvfr_data_pipelines.utils.datagouv import local_client
from datagouvfr_data_pipelines.utils.filesystem import File
from datagouvfr_data_pipelines.utils.s3 import S3Client
from datagouvfr_data_pipelines.utils.tchap import send_message

DAG_FOLDER = "datagouvfr_data_pipelines/data_processing/"
TMP_FOLDER = f"{AIRFLOW_DAG_TMP}controle_sanitaire_eau/"
dataset_id = "6a19b9c37d14bf8843ad1596"
config = {
    "RESULT": "5a84f909-4019-40c4-816a-f2f2c9372b24",
    "COM_UDI": "3b646d13-a9d9-418c-9060-33a9b9cdde50",
    "PLV": "75a43900-a916-4105-adcf-fe88f88194a8",
}


def check_if_modif():
    return local_client.resource(id=config["RESULT"]).check_if_more_recent_update(
        dataset_id="5cf8d9ed8b4c4110294c841d"
    )


@task()
def process_data():
    # this is done in one task to get the files only once
    resources = requests.get(
        "https://www.data.gouv.fr/api/1/datasets/5cf8d9ed8b4c4110294c841d/",
        headers={"X-fields": "resources{title,url}"},
    ).json()["resources"]
    resources = [r for r in resources if re.search(r"dis-\d{4}.zip", r["title"])]
    columns = {file_type: [] for file_type in config.keys()}
    for idx, resource in enumerate(resources):
        logging.info(resource["title"])
        year = int(resource["title"].split(".")[0].split("-")[1])
        r = requests.get(resource["url"])
        r.raise_for_status()
        with ZipFile(BytesIO(r.content)) as zip_ref:
            for _, file in enumerate(zip_ref.namelist()):
                logging.info("> " + file)
                file_type = "_".join(file.split("_")[1:-1])
                assert file_type in config.keys()
                with zip_ref.open(file) as f:
                    df = pd.read_csv(
                        f,
                        sep=",",
                        dtype=str,
                    )
                if not columns[file_type]:
                    columns[file_type] = list(df.columns)
                elif list(df.columns) != columns[file_type]:
                    logging.info(columns[file_type])
                    logging.info(list(df.columns))
                    raise ValueError("Columns differ between files")
                df["annee"] = year
                df.to_csv(
                    f"{TMP_FOLDER}{file_type}.csv",
                    index=False,
                    encoding="utf8",
                    mode="w" if idx == 0 else "a",
                    header=idx == 0,
                )
                del df
                csv_to_csvgz(f"{TMP_FOLDER}{file_type}.csv")


@task()
def send_to_s3(file_type: str):
    S3Client(bucket=S3_BUCKET_DATA_PIPELINE_OPEN).send_files(
        list_files=[
            File(
                source_path=TMP_FOLDER,
                source_name=f"{file_type}.csv.gz",
                dest_path="controle_sanitaire_eau/",
                dest_name=f"{file_type}.csv.gz",
                content_type="application/gzip",
            )
        ],
        ignore_airflow_env=True,
        is_public=True,
    )


@task()
def publish_on_datagouv(file_type: str):
    local_client.resource(
        dataset_id=config[file_type],
        id=config[file_type],
        is_communautary=True,
        fetch=False,
    ).update(
        payload={
            "filesize": os.path.getsize(TMP_FOLDER + f"{file_type}.csv.gz"),
        },
    )


@task()
def notification():
    dataset_id = local_client.Resource(config["RESULT"]).dataset_id
    send_message(
        text=(
            "📣 Données du contrôle sanitaire de l'eau mises à jour.\n\n"
            f"- Données stockées sur S3 - Bucket {S3_BUCKET_DATA_PIPELINE_OPEN}\n"
            f"- Données publiées [sur data.gouv.fr]({local_client.base_url}/datasets/{dataset_id})"
        )
    )
