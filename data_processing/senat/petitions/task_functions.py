import logging
import os
import re
from datetime import datetime

import pandas as pd
import requests
from airflow.decorators import task
from bs4 import BeautifulSoup
from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
    AIRFLOW_ENV,
    S3_BUCKET_DATA_PIPELINE_OPEN,
)
from datagouvfr_data_pipelines.utils.datagouv import local_client
from datagouvfr_data_pipelines.utils.filesystem import File
from datagouvfr_data_pipelines.utils.tchap import send_message
from datagouvfr_data_pipelines.utils.retry import simple_connection_retry
from datagouvfr_data_pipelines.utils.s3 import S3Client

DAG_FOLDER = "datagouvfr_data_pipelines/data_processing/"
TMP_FOLDER = f"{AIRFLOW_DAG_TMP}senat/"
S3_FOLDER = "senat_petitions/"
FILE_NAME = "petitions.csv"
DATASET_ID = (
    "69b0175c65c90b2a60c3f8c3" if AIRFLOW_ENV != "prod" else "69b04b33b623541cb04aabde"
)
RESOURCE_ID = (
    "eaa95292-072d-4353-a6b8-1be65be0ab0f"
    if AIRFLOW_ENV != "prod"
    else "5bf276c9-1c5a-4b2a-9f02-954f297118b5"
)


def build_status(page: BeautifulSoup) -> str:
    found = page.find("span", attrs={"class": "initiative-status"})
    if not found:
        return "Ouverte"
    return found.text.strip()


def get_limit_date(page: BeautifulSoup) -> str | None:
    found = page.find("span", attrs={"class": "phase-date"})
    if found is None:
        return
    period = remove_duplicate_blanks(found.text.replace("\n", ""))
    return standardize_date(period.split("-")[1].strip())


def get_votes(page: BeautifulSoup) -> int | None:
    found = page.find("span", attrs={"class": "progress__bar__number"})
    if found is None:
        return
    return int(found.text.replace(" ", ""))


def standardize_date(date: str) -> str:
    return "-".join(reversed(date.split("/")))


def remove_duplicate_blanks(input_str: str) -> str:
    return re.sub(r"\s+", " ", input_str).strip()


@simple_connection_retry
def get_row(_id: int, session) -> dict | None:
    url = f"https://petitions.senat.fr/initiatives/i-{_id}"
    ping = session.head(url)
    if ping.headers.get("Location") == "https://petitions.senat.fr/":
        return
    r = session.get(url)
    page = BeautifulSoup(r.text, "html.parser")
    title = (
        page.find_all("h1", attrs={"class": "heading2"})[0]
        .text.replace("\n", "")
        .strip()
    )
    return {
        "titre": title,
        "description": page.find("meta", attrs={"property": "og:description"})
        .get("content")
        .replace("\r", "")
        .replace("\n", ""),
        "date_publication": standardize_date(
            page.find("div", attrs={"class": "publish-date"})
            .text.replace("\n", "")
            .strip()
        ),
        "nb_votes": get_votes(page),
        "statut": build_status(page),
        "date_limite_vote": get_limit_date(page),
        "type": page.find("h2", attrs={"class": "heading-small"})
        .text.replace("\n", "")
        .strip(),
        "url": url,
        "identifiant": _id,
    }


@task()
def gather_petitions():
    s3_open = S3Client(bucket=S3_BUCKET_DATA_PIPELINE_OPEN)
    # getting current file to ignore unused ids
    ids = (
        pd.read_csv(
            s3_open.get_file_url(S3_FOLDER + FILE_NAME),
            sep=";",
            usecols=["identifiant"],
            dtype={"identifiant": float},
        )["identifiant"]
        .dropna()
        .apply(int)
    )
    session = requests.Session()
    max_id = max(ids)
    unused_ids = {k for k in range(1, max_id + 1) if k not in ids.values}
    # we go through all ids except the ones we know are unused
    # we don't know which id is the last one, so we stop:
    # - after we have more than 10 (arbitrary) unused ids in a row
    # - if we are after the previous max id
    data = []
    _id = min(ids) - 1
    unreach_in_a_row = 0
    while True:
        _id += 1
        if _id > max_id and unreach_in_a_row > 10:
            break
        if _id in unused_ids:
            unreach_in_a_row += 1
            continue
        row = get_row(_id, session)
        if row is None:
            unreach_in_a_row += 1
            continue
        else:
            unreach_in_a_row = 0
            data.append(row)
            if len(data) % 100 == 0:
                logging.info(f"> fetched {len(data)}")

    df = pd.DataFrame(data)
    df.to_csv(
        TMP_FOLDER + FILE_NAME,
        index=False,
        sep=";",
    )
    # no need to convert to parquet, hydra will


@task()
def send_petitions_to_s3():
    s3_open = S3Client(bucket=S3_BUCKET_DATA_PIPELINE_OPEN)
    s3_open.send_files(
        list_files=[
            File(
                source_path=TMP_FOLDER,
                source_name=FILE_NAME,
                dest_path=S3_FOLDER,
                dest_name=FILE_NAME,
                content_type="text/csv",
            ),
            # saving dated file
            File(
                source_path=TMP_FOLDER,
                source_name=FILE_NAME,
                dest_path=S3_FOLDER,
                dest_name=datetime.now().strftime("%Y-%m-%d") + "_" + FILE_NAME,
                content_type="text/csv",
            ),
        ],
        ignore_airflow_env=True,
    )


@task()
def publish_on_datagouv():
    s3_open = S3Client(bucket=S3_BUCKET_DATA_PIPELINE_OPEN)
    local_client.resource(
        id=RESOURCE_ID,
        dataset_id=DATASET_ID,
        fetch=False,
    ).update(
        payload={
            "filesize": os.path.getsize(TMP_FOLDER + FILE_NAME),
            "title": (f"Pétitions au {datetime.now().strftime('%d-%m-%Y')}"),
            "format": "csv",
            "description": (
                "Créé à partir de la plateforme https://petitions.senat.fr/"
            ),
            "url": s3_open.get_file_url(S3_FOLDER + FILE_NAME),
        },
    )


@task()
def notification():
    send_message(
        text=(
            "📣 Données des pétitions du Sénat mises à jour.\n\n"
            f"- Données stockées sur S3 - Bucket {S3_BUCKET_DATA_PIPELINE_OPEN}\n"
            f"- Données publiées [sur data.gouv.fr]({local_client.base_url}/datasets/{DATASET_ID})"
        )
    )
