import logging
import os
import re
from zipfile import ZipFile

import pandas as pd
from airflow.sdk import task
from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
    S3_BUCKET_DATA_PIPELINE_OPEN,
)
from datagouvfr_data_pipelines.utils.conversions import (
    csv_to_csvgz,
)
from datagouvfr_data_pipelines.utils.datagouv import local_client
from datagouvfr_data_pipelines.utils.download import download_files
from datagouvfr_data_pipelines.utils.filesystem import File
from datagouvfr_data_pipelines.utils.retry import RequestRetry
from datagouvfr_data_pipelines.utils.s3 import S3Client
from datagouvfr_data_pipelines.utils.tasks import force_rebuild_requested
from datagouvfr_data_pipelines.utils.tchap import send_message

DAG_FOLDER = "datagouvfr_data_pipelines/data_processing/"
TMP_FOLDER = f"{AIRFLOW_DAG_TMP}controle_sanitaire_eau/"
CHUNK_SIZE = 100_000
dataset_id = "6a19b9c37d14bf8843ad1596"
config = {
    "RESULT": "5a84f909-4019-40c4-816a-f2f2c9372b24",
    "COM_UDI": "3b646d13-a9d9-418c-9060-33a9b9cdde50",
    "PLV": "75a43900-a916-4105-adcf-fe88f88194a8",
}


def check_if_modif(dag_run=None):
    if force_rebuild_requested(dag_run):
        return True
    return local_client.resource(id=config["RESULT"]).check_if_more_recent_update(
        dataset_id="5cf8d9ed8b4c4110294c841d"
    )


@task()
def process_data():
    # this is done in one task to get the files only once
    resources = RequestRetry.get(
        "https://www.data.gouv.fr/api/1/datasets/5cf8d9ed8b4c4110294c841d/",
        headers={"X-fields": "resources{title,url}"},
    ).json()["resources"]
    resources = [r for r in resources if re.search(r"dis-\d{4}.zip", r["title"])]
    columns = {scope: [] for scope in config.keys()}
    started_scopes = set()
    for resource in resources:
        logging.info(resource["title"])
        year = int(resource["title"].split(".")[0].split("-")[1])
        # Les fichiers font plusieurs centaines de Mo, ils sont dont d'abord
        # téléchargés avant d'être ouverts afin d'éviter des erreurs OOM
        zip_path = f"{TMP_FOLDER}{resource['title']}"
        download_files(
            list_urls=[
                File(
                    url=resource["url"],
                    dest_path=TMP_FOLDER,
                    dest_name=resource["title"],
                )
            ]
        )
        with ZipFile(zip_path) as zip_ref:
            for file in zip_ref.namelist():
                logging.info("> " + file)
                scope = "_".join(file.split("_")[1:-1])
                assert scope in config.keys()
                with zip_ref.open(file) as f:
                    # lecture par chunk pour éviter les erreurs OOM
                    for chunk in pd.read_csv(
                        f,
                        sep=",",
                        dtype=str,
                        chunksize=CHUNK_SIZE,
                    ):
                        if not columns[scope]:
                            columns[scope] = list(chunk.columns)
                        elif list(chunk.columns) != columns[scope]:
                            logging.info(columns[scope])
                            logging.info(list(chunk.columns))
                            raise ValueError("Columns differ between files")
                        is_first = scope not in started_scopes
                        chunk["annee"] = year
                        chunk.to_csv(
                            f"{TMP_FOLDER}{scope}.csv",
                            index=False,
                            encoding="utf8",
                            mode="w" if is_first else "a",
                            header=is_first,
                        )
                        started_scopes.add(scope)
        # Supprime le fichier avant de traiter le suivant
        os.remove(zip_path)


@task()
def send_to_s3(scope: str):
    csv_to_csvgz(f"{TMP_FOLDER}{scope}.csv")
    S3Client(bucket=S3_BUCKET_DATA_PIPELINE_OPEN).send_files(
        list_files=[
            File(
                source_path=TMP_FOLDER,
                source_name=f"{scope}.csv.gz",
                dest_path="controle_sanitaire_eau/",
                dest_name=f"{scope}.csv.gz",
                content_type="application/gzip",
            )
        ],
        ignore_airflow_env=True,
        is_public=True,
    )


@task()
def publish_on_datagouv(scope: str):
    local_client.resource(
        dataset_id=dataset_id,
        id=config[scope],
        fetch=False,
    ).update(
        payload={
            "filesize": os.path.getsize(TMP_FOLDER + f"{scope}.csv.gz"),
        },
    )


@task()
def notification():
    send_message(
        text=(
            "📣 Données du contrôle sanitaire de l'eau mises à jour.\n\n"
            f"- Données stockées sur S3 - Bucket {S3_BUCKET_DATA_PIPELINE_OPEN}\n"
            f"- Données publiées [sur data.gouv.fr]({local_client.base_url}/datasets/{dataset_id})"
        )
    )
