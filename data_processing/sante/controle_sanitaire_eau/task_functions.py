import pandas as pd
import os
import requests
import json
from io import BytesIO
from zipfile import ZipFile
import re

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_HOME,
    AIRFLOW_DAG_TMP,
    AIRFLOW_ENV,
    MINIO_BUCKET_DATA_PIPELINE_OPEN,
)
from datagouvfr_data_pipelines.utils.datagouv import (
    post_remote_resource,
    DATAGOUV_URL,
)
from datagouvfr_data_pipelines.utils.mattermost import send_message
from datagouvfr_data_pipelines.utils.minio import MinIOClient
from datagouvfr_data_pipelines.utils.utils import csv_to_parquet, MOIS_FR

DAG_FOLDER = "datagouvfr_data_pipelines/data_processing/"
DATADIR = f"{AIRFLOW_DAG_TMP}controle_sanitaire_eau"
minio_open = MinIOClient(bucket=MINIO_BUCKET_DATA_PIPELINE_OPEN)

with open(f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}sante/controle_sanitaire_eau/config/dgv.json") as fp:
    config = json.load(fp)


def check_if_modif():
    resources = requests.get(
        'https://www.data.gouv.fr/api/1/datasets/5cf8d9ed8b4c4110294c841d/',
        headers={"X-fields": "resources{internal{last_modified_internal}}"}
    ).json()['resources']
    # we consider one arbitrary resource of the target dataset
    lastest_update = requests.get(
        (
            f"{DATAGOUV_URL}/api/1/datasets/community_resources/"
            + config["RESULT"]["csv"][AIRFLOW_ENV]["resource_id"] + "/"
        ),
        headers={"X-fields": "internal{last_modified_internal}"}
    ).json()["internal"]["last_modified_internal"]
    return True
    return any(
        r["internal"]["last_modified_internal"] > lastest_update for r in resources
    )


def process_data():
    # this is done in one task to get the files only once
    resources = requests.get(
        'https://www.data.gouv.fr/api/1/datasets/5cf8d9ed8b4c4110294c841d/',
        headers={"X-fields": "resources{title,url}"}
    ).json()['resources']
    resources = [r for r in resources if re.search(r"dis-\d{4}.zip", r["title"])]
    columns = {file_type: [] for file_type in config.keys()}
    for idx, resource in enumerate(resources):
        if idx > 3:
            break
        print(resource["title"])
        year = int(resource["title"].split(".")[0].split("-")[1])
        r = requests.get(resource["url"])
        r.raise_for_status()
        with ZipFile(BytesIO(r.content)) as zip_ref:
            for _, file in enumerate(zip_ref.namelist()):
                print(">", file)
                file_type = "_".join(file.split("_")[1:-1])
                assert file_type in config.keys()
                with zip_ref.open(file) as f:
                    df = pd.read_csv(
                        f,
                        sep=',',
                        dtype=str,
                    )
                if not columns[file_type]:
                    columns[file_type] = list(df.columns)
                elif list(df.columns) != columns[file_type]:
                    print(columns[file_type])
                    print(list(df.columns))
                    raise ValueError('Columns differ between files')
                df["annee"] = year
                df.to_csv(
                    f"{DATADIR}/{file_type}.csv",
                    index=False,
                    encoding="utf8",
                    mode="w" if idx == 0 else "a",
                    header=idx == 0,
                )
                del df
    for file_type in config.keys():
        csv_to_parquet(
            f"{DATADIR}/{file_type}.csv",
            sep=',',
            columns=columns[file_type],
        )


def send_to_minio(file_type):
    minio_open.send_files(
        list_files=[
            {
                "source_path": f"{DATADIR}/",
                "source_name": f"{file_type}.{ext}",
                "dest_path": "controle_sanitaire_eau/",
                "dest_name": f"{file_type}.{ext}",
            }
            for ext in ["csv", "parquet"]
        ],
        ignore_airflow_env=True,
    )
    print(non)


def publish_on_datagouv(ti, file_type):
    latest = ti.xcom_pull(key="latest", task_ids=f"process_rna_{file_type}")
    y, m, d = latest[:4], latest[4:6], latest[6:]
    date = f"{d} {MOIS_FR[m]} {y}"
    with open(f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}rna/config/dgv.json") as fp:
        config = json.load(fp)
    for ext in ["csv", "parquet"]:
        post_remote_resource(
            dataset_id=config[file_type][ext][AIRFLOW_ENV]["dataset_id"],
            resource_id=config[file_type][ext][AIRFLOW_ENV]["resource_id"],
            payload={
                "url": (
                    f"https://object.files.data.gouv.fr/{MINIO_BUCKET_DATA_PIPELINE_OPEN}"
                    f"/rna/{file_type}.{ext}"
                ),
                "filesize": os.path.getsize(DATADIR + f"/{file_type}.{ext}"),
                "title": (
                    f"Données {file_type.title()} au {date} (format {ext})"
                ),
                "format": ext,
                "description": (
                    f"RNA {file_type.title()} au {date} (format {ext})"
                    " (créé à partir des [fichiers du Ministère de l'intérieur]"
                    "(https://www.data.gouv.fr/fr/datasets/58e53811c751df03df38f42d/))"
                ),
            },
        )


def send_notification_mattermost():
    with open(f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}rna/config/dgv.json") as fp:
        config = json.load(fp)
    dataset_id = config["import"]["csv"][AIRFLOW_ENV]["dataset_id"]
    send_message(
        text=(
            ":mega: Données des associations mises à jour.\n"
            f"- Données stockées sur Minio - Bucket {MINIO_BUCKET_DATA_PIPELINE_OPEN}\n"
            f"- Données publiées [sur data.gouv.fr]({DATAGOUV_URL}/fr/datasets/{dataset_id})"
        )
    )
