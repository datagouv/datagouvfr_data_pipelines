from datetime import datetime
import logging
import os
import re

from bs4 import BeautifulSoup
import pandas as pd
import requests

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
    AIRFLOW_ENV,
    MINIO_BUCKET_DATA_PIPELINE_OPEN,
)
from datagouvfr_data_pipelines.utils.datagouv import local_client
from datagouvfr_data_pipelines.utils.filesystem import File
from datagouvfr_data_pipelines.utils.mattermost import send_message
from datagouvfr_data_pipelines.utils.s3 import S3Client
from datagouvfr_data_pipelines.utils.retry import simple_connection_retry

DAG_FOLDER = "datagouvfr_data_pipelines/data_processing/"
DATADIR = f"{AIRFLOW_DAG_TMP}an_petitions/"
s3_folder = "an_petitions/"
file_name = "petitions.csv"
dataset_id = (
    "687e2d07eb1e2ad010d1c1af" if AIRFLOW_ENV != "prod" else "6889cc10e79a25f17ed69acb"
)
resource_id = (
    "ce3d1e17-b6fe-49d4-bbc6-1ea736eaa360"
    if AIRFLOW_ENV != "prod"
    else "c94c9dfe-23eb-45aa-acd1-7438c4e977db"
)
s3_open = S3Client(bucket=MINIO_BUCKET_DATA_PIPELINE_OPEN)

session = requests.Session()


def build_status(page: BeautifulSoup) -> str:
    if page.find("div", attrs={"class": "archive-header"}):
        return "archivee"
    elif page.find("span", attrs={"class": "alert label initiative-status"}):
        return "classee"
    elif page.find("span", attrs={"class": "warning label initiative-status"}):
        return "expiree"
    return "ouverte"


def standardize_date(date: str) -> str:
    # DD/MM/YYYY => YYYY-MM-DD
    return "-".join(reversed(date.split("/")))


def remove_duplicate_blanks(input_str: str) -> str:
    return re.sub(r"\s+", " ", input_str).strip()


def get_labels(page: BeautifulSoup) -> dict:
    # there are two possible labels, but they can coexist
    labels = [
        el
        for el in page.find_all("a")
        if el.get("href", "").startswith("/initiatives?filter")
    ]
    commission = next(
        (lab.text for lab in labels if lab.text.startswith("Commission")), None
    )
    legislature = next(
        (lab.text for lab in labels if lab.text.startswith("Législature")), None
    )
    return {
        "commission": commission if commission else None,
        "legislature": legislature.replace("Législature ", "") if legislature else None,
    }


def get_limit_date(page: BeautifulSoup) -> str:
    found = page.find("span", attrs={"class": "phase-date"})
    if found is None:
        return
    return standardize_date(remove_duplicate_blanks(found.text.replace("\n", "")))


def get_decision(page: BeautifulSoup) -> str | None:
    # three possible decisions, but there are not all found at the same spot
    decision = list(
        set(
            [
                el.text.replace("\n", "")
                for el in page.find_all("p")
                if any(
                    substr in el.text
                    for substr in [
                        "En application de l’article 148",
                        "Conformément à la décision",
                    ]
                )
            ]
        )
    )
    if decision:
        return decision[0]
    decision = list(
        set(
            [
                el.text.replace("\n", "")
                for el in page.find_all("h5")
                if any(
                    substr in el.text
                    for substr in [
                        "Cette pétition a été rejetée",
                    ]
                )
            ]
        )
    )
    return decision[0] if decision else None


def get_votes(page: BeautifulSoup) -> int | None:
    found = page.find("span", attrs={"class": "progress__bar__number"})
    if found is None:
        return
    return int(found.text.replace(" ", ""))


@simple_connection_retry
def get_row(_id: int) -> dict | None:
    url = f"https://petitions.assemblee-nationale.fr/initiatives/i-{_id}"
    # ping the ping, because all ids are not used
    ping = session.head(url)
    # if we get redirected to the home page, early stop
    # and we'll save the id as unused for later processes
    if ping.headers.get("Location") == "https://petitions.assemblee-nationale.fr/":
        return
    r = session.get(url)
    page = BeautifulSoup(r.text, "html.parser")
    labels = get_labels(page)
    title = (
        page.find_all("h2", attrs={"class": "heading2"})[0]
        .text.replace("\n", "")
        .strip()
    )
    logging.info(f"{_id}: {title}")
    # all the fields have been constructed heuristically, hopefully the references won't change
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
        "commission": labels["commission"],
        "legislature": labels["legislature"],
        "decision_commission": get_decision(page),
        "url": url,
        "identifiant": _id,
    }


def gather_petitions():
    # getting current file to ignore unused ids
    ids = (
        pd.read_csv(
            s3_open.get_file_url(s3_folder + file_name),
            sep=";",
            usecols=["identifiant"],
            dtype={"identifiant": float},
        )["identifiant"]
        .dropna()
        .apply(int)
    )
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
        row = get_row(_id)
        if row is None:
            unreach_in_a_row += 1
            continue
        else:
            unreach_in_a_row = 0
            data.append(row)

    df = pd.DataFrame(data)
    df.to_csv(
        DATADIR + file_name,
        index=False,
        sep=";",
    )
    # no need to convert to parquet, hydra will


def send_petitions_to_s3():
    s3_open.send_files(
        list_files=[
            File(
                source_path=DATADIR,
                source_name=file_name,
                dest_path=s3_folder,
                dest_name=file_name,
            ),
            # saving dated file
            File(
                source_path=DATADIR,
                source_name=file_name,
                dest_path=s3_folder,
                dest_name=datetime.now().strftime("%Y-%m-%d") + "_" + file_name,
            ),
        ],
        ignore_airflow_env=True,
    )


def publish_on_datagouv():
    local_client.resource(
        id=resource_id,
        dataset_id=dataset_id,
        fetch=False,
    ).update(
        payload={
            "filesize": os.path.getsize(DATADIR + file_name),
            "title": (f"Pétitions au {datetime.now().strftime('%d-%m-%Y')}"),
            "format": "csv",
            "description": (
                "Créé à partir de la plateforme https://petitions.assemblee-nationale.fr/"
            ),
        },
    )


def send_notification_mattermost():
    send_message(
        text=(
            ":mega: Données des pétitions de l'AN mises à jour.\n"
            f"- Données stockées sur Minio - Bucket {MINIO_BUCKET_DATA_PIPELINE_OPEN}\n"
            f"- Données publiées [sur data.gouv.fr]({local_client.base_url}/datasets/{dataset_id})"
        )
    )
