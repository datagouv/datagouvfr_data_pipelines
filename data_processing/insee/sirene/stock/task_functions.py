import os
import json
from datetime import datetime, timedelta
import logging
import zipfile

import pandas as pd

from datagouvfr_data_pipelines.utils.conversions import csv_to_parquet
from datagouvfr_data_pipelines.utils.filesystem import File, compute_checksum_from_file
from datagouvfr_data_pipelines.utils.download import download_files
from datagouvfr_data_pipelines.utils.minio import MinIOClient
from datagouvfr_data_pipelines.utils.datagouv import local_client
from datagouvfr_data_pipelines.utils.mattermost import send_message
from datagouvfr_data_pipelines.utils.utils import MOIS_FR
from datagouvfr_data_pipelines.config import (
    INSEE_BASE_URL,
    MINIO_BUCKET_DATA_PIPELINE,
    MINIO_BUCKET_DATA_PIPELINE_OPEN,
    SECRET_INSEE_LOGIN,
    SECRET_INSEE_PASSWORD,
)

minio_restricted = MinIOClient(bucket=MINIO_BUCKET_DATA_PIPELINE)
minio_open = MinIOClient(bucket=MINIO_BUCKET_DATA_PIPELINE_OPEN)


def check_if_already_processed(minio_path: str):
    files_in_folder = minio_open.get_files_from_prefix(
        prefix=minio_path,
        ignore_airflow_env=True,
    )
    this_month_prefix = datetime.today().strftime("%Y-%m")
    for file in reversed(sorted(files_in_folder)):
        if file.startswith(minio_path + this_month_prefix):
            # early stop and don't trigger the following tasks
            return False
    return True


def get_files(ti, tmp_dir: str, resource_file: str):
    with open(f"{os.path.dirname(__file__)}/config/{resource_file}") as json_file:
        data = json.load(json_file)

    download_files(
        list_urls=[
            File(
                url=f"{INSEE_BASE_URL}{item['nameFTP']}",
                dest_path=tmp_dir,
                dest_name=item["nameFTP"],
            )
            for item in data
            if item["nameFTP"] not in os.listdir(tmp_dir)
        ],
        auth_user=SECRET_INSEE_LOGIN,
        auth_password=SECRET_INSEE_PASSWORD,
        timeout=1200,
    )

    date_col = "dateDernierTraitementEtablissement"
    # parquet conversion (extract zip, convert, delete intermediary step)
    for item in data:
        csv_name = item["nameFTP"].replace(".zip", ".csv")
        logging.info(f"Unzipping {csv_name}")
        with zipfile.ZipFile(tmp_dir + item["nameFTP"], "r") as zip_ref:
            zip_ref.extract(csv_name, tmp_dir)
        parquet_file = csv_to_parquet(
            tmp_dir + csv_name,
            sep=";" if "Geolocalisation" in csv_name else ",",
            dtype=item["dtype"],
            strict_mode=False,
            quotechar='"',
            # the files may contain things like "\," which make the parquet export crash,
            # so using a very rare character as escape to make it work
            escapechar="\u0001",
        )
        os.remove(tmp_dir + csv_name)
        if date_col in item["dtype"]:
            most_recent_update: str = (
                pd.read_parquet(parquet_file, columns=[date_col])[date_col]
                .max()
                .isoformat()
            )
            # checking that the stock has been updated at the last week of the previous month
            oldest_acceptable_date = (
                datetime.now().replace(day=1) - timedelta(days=1, weeks=1)
            ).strftime("%Y-%m-%d")
            if most_recent_update < oldest_acceptable_date:
                raise ValueError(
                    f"Stock looks too old, latest update on {most_recent_update}"
                )

    hashfiles = {}
    for item in data:
        hashfiles[item["nameFTP"]] = compute_checksum_from_file(
            f"{tmp_dir}{item['nameFTP']}"
        )

    ti.xcom_push(key="hashes", value=hashfiles)


def publish_file_minio(tmp_dir: str, resource_file: str, minio_path: str):
    with open(f"{os.path.dirname(__file__)}/config/{resource_file}") as json_file:
        data = json.load(json_file)
    logging.info(data)

    minio_open.send_files(
        list_files=[
            File(
                source_path=tmp_dir,
                source_name=item["nameFTP"],
                dest_path=minio_path,
                dest_name=item["nameFTP"],
            )
            for item in data
        ],
        ignore_airflow_env=True,
    )

    # sending parquet
    minio_open.send_files(
        list_files=[
            File(
                source_path=tmp_dir,
                source_name=item["nameFTP"].replace(".zip", ".parquet"),
                dest_path=minio_path,
                dest_name=item["nameFTP"].replace(".zip", ".parquet"),
            )
            for item in data
        ],
        ignore_airflow_env=True,
    )

    # sending dated files
    minio_open.send_files(
        list_files=[
            File(
                source_path=tmp_dir,
                source_name=item["nameFTP"],
                dest_path=minio_path,
                dest_name=f"{datetime.today().strftime('%Y-%m')}-01-{item['nameFTP']}",
            )
            for item in data
        ],
        ignore_airflow_env=True,
    )


def update_dataset_data_gouv(ti, tmp_dir: str, resource_file: str):
    hashes = ti.xcom_pull(key="hashes", task_ids="get_files")

    liste_mois = [m.title() for m in MOIS_FR.values()]
    mois = datetime.now().date().month
    day = "0" * (2 - len(str(d := datetime.now().date().day))) + str(d)
    with open(f"{os.path.dirname(__file__)}/config/{resource_file}") as json_file:
        data = json.load(json_file)
    logging.info(data)
    for d in data:
        obj = {
            "title": (
                f"Sirene : Fichier {d['name'].replace('_utf8.zip', '')} du "
                f"{day} {liste_mois[mois - 1]} {datetime.today().strftime('%Y')}"
            ),
            "filesize": os.path.getsize(tmp_dir + d["nameFTP"]),
        }
        if d["nameFTP"] in hashes:
            obj["checksum"] = {"type": "sha256", "value": hashes[d["nameFTP"]]}
        local_client.resource(
            dataset_id=d["dataset_id"],
            id=d["resource_id"],
            fetch=False,
        ).update(payload=obj)

        # updating parquet
        local_client.resource(
            dataset_id=d["dataset_id"],
            id=d["parquet_resource_id"],
            fetch=False,
        ).update(
            payload={
                "title": (
                    f"Sirene : Fichier {d['name'].replace('_utf8.zip', '')} du "
                    f"{day} {liste_mois[mois - 1]} {datetime.today().strftime('%Y')}"
                    " (format parquet)"
                ),
                "filesize": os.path.getsize(
                    tmp_dir + d["nameFTP"].replace(".zip", ".parquet")
                ),
            },
        )


def publish_mattermost(geoloc):
    if geoloc:
        text = (
            "Sirene géolocalisé INSEE mis à jour\n- [Stockage données]"
            "(https://object.infra.data.gouv.fr/browser/data-pipeline-open/siren%2Fgeoloc%2F)"
            "\n- [Jeu de données data.gouv.fr](https://www.data.gouv.fr/datasets/geolocalisation-"
            "des-etablissements-du-repertoire-sirene-pour-les-etudes-statistiques/) "
        )
    else:
        text = (
            "Base Sirene mise à jour\n- [Stockage données]"
            "(https://object.infra.data.gouv.fr/browser/data-pipeline-open/siren%2Fstock%2F)"
            "\n- [Jeu de données data.gouv.fr](https://www.data.gouv.fr/datasets/base-sirene-des-"
            "entreprises-et-de-leurs-etablissements-siren-siret/) "
        )
    send_message(
        text=text,
        image_url="https://static.data.gouv.fr/avatars/db/fbfd745ae543f6856ed59e3d44eb71.jpg",
    )
