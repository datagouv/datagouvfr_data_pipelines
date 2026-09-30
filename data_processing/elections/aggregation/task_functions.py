import json
import logging
import os
from datetime import datetime

import pandas as pd
import requests
from airflow.sdk import task
from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_HOME,
    AIRFLOW_DAG_TMP,
    AIRFLOW_ENV,
    S3_BUCKET_DATA_PIPELINE_OPEN,
)
from datagouvfr_data_pipelines.data_processing.elections.aggregation.schema import (
    SCOPES,
    dtypes,
)
from datagouvfr_data_pipelines.utils.conversions import csv_to_parquet
from datagouvfr_data_pipelines.utils.datagouv import local_client
from datagouvfr_data_pipelines.utils.filesystem import File
from datagouvfr_data_pipelines.utils.s3 import S3Client
from datagouvfr_data_pipelines.utils.tchap import send_message

DAG_FOLDER = "datagouvfr_data_pipelines/data_processing/"
TMP_FOLDER = f"{AIRFLOW_DAG_TMP}elections/"
# prod outputs are at the root of the bucket (published URLs), other envs are prefixed
OUTPUT_FOLDER = "elections/" if AIRFLOW_ENV == "prod" else f"{AIRFLOW_ENV}/elections/"
SOURCE_DATASETS_API_URL = "https://www.data.gouv.fr/api/1/datasets/"


def load_sources() -> dict:
    with open(
        f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}elections/aggregation/sources.json"
    ) as fp:
        return json.load(fp)


@task()
def check_sources_updates():
    # the source datasets are always on prod, whatever the environment
    alerts = []
    for id_election, source in sorted(load_sources().items()):
        dataset_id = source["source_dataset_id"]
        link = f"https://www.data.gouv.fr/datasets/{dataset_id}"
        response = requests.get(f"{SOURCE_DATASETS_API_URL}{dataset_id}/", timeout=60)
        if not response.ok:
            alerts.append(
                f"- [{id_election}]({link}) : jeu inaccessible (HTTP {response.status_code})"
            )
            continue
        dataset = response.json()
        if dataset.get("archived"):
            alerts.append(f"- [{id_election}]({link}) : jeu archivé")
        last_update = datetime.fromisoformat(dataset["last_update"])
        if last_update > datetime.fromisoformat(source["source_last_update"]):
            alerts.append(
                f"- [{id_election}]({link}) : modifié le {last_update:%Y-%m-%d}"
                f" (config : {source['source_last_update'][:10]})"
            )
    if not alerts:
        logging.info("All source datasets are up to date")
        return
    logging.warning("\n".join(alerts))
    # we only warn, our copies on S3 remain the reference for the aggregation
    send_message(
        text=(
            "Élections : jeux sources du Ministère de l'Intérieur à vérifier\n\n"
            + "\n".join(alerts)
            + "\n\nSi une correction est reprise, mettre à jour `source_last_update`"
            " dans `sources.json`."
        )
    )


@task()
def process_election_data():
    # the standardized files are built by the standalone scripts (see scripts/),
    # stored on our S3 and listed in sources.json, here we only concatenate them
    sources = load_sources()
    s3_client = S3Client(bucket=S3_BUCKET_DATA_PIPELINE_OPEN, conn_name="S3_OVH_SBG")
    for scope in SCOPES:
        logging.info(f"Processing {scope} resources")
        for idx, id_election in enumerate(sorted(sources)):
            key = sources[id_election]["files"][scope]
            file = File(
                source_path=os.path.dirname(key),
                source_name=os.path.basename(key),
                dest_path=TMP_FOLDER,
                dest_name=f"{id_election}_{scope}.csv",
                remote_source=True,
            )
            s3_client.download_files([file], ignore_airflow_env=True)
            df = pd.read_csv(file.full_dest_path, sep=";", dtype=str)
            os.remove(file.full_dest_path)
            assert all(col in dtypes[scope].keys() for col in df.columns)
            # add missing columns and reorder for concatenation
            for col in dtypes[scope].keys():
                if col not in df.columns:
                    df[col] = ""
            df = df[dtypes[scope].keys()]
            # concatenating all files (first one has header)
            df.to_csv(
                TMP_FOLDER + f"{scope}_results.csv",
                sep=";",
                index=False,
                mode="w" if idx == 0 else "a",
                header=idx == 0,
            )
            del df
        # hydra is not (yet) able to ingest the big csv, maybe soon? :eyes:
        logging.info("Export en parquet...")
        csv_to_parquet(
            csv_file_path=TMP_FOLDER + f"{scope}_results.csv",
            dtype=dtypes[scope],
            # pandas quotes and escapes with ", don't let duckdb sniff it on the first rows
            quotechar='"',
            escapechar='"',
        )


@task()
def send_results_to_s3():
    S3Client(bucket=S3_BUCKET_DATA_PIPELINE_OPEN, conn_name="S3_OVH_SBG").send_files(
        list_files=[
            File(
                source_path=TMP_FOLDER,
                source_name=f"{scope}_results.{ext}",
                dest_path=OUTPUT_FOLDER,
                dest_name=f"{scope}_results.{ext}",
                content_type=(
                    "application/vnd.apache.parquet" if ext == "parquet" else "text/csv"
                ),
            )
            for scope in SCOPES
            for ext in ["csv", "parquet"]
        ],
        # the environment prefix is already handled by OUTPUT_FOLDER
        ignore_airflow_env=True,
        is_public=True,
    )


@task()
def publish_results_elections():
    s3_client = S3Client(bucket=S3_BUCKET_DATA_PIPELINE_OPEN, conn_name="S3_OVH_SBG")
    with open(f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}elections/aggregation/config.json") as fp:
        config = json.load(fp)
    for ext in ["csv", "parquet"]:
        local_client.resource(
            id=config["general"][ext][AIRFLOW_ENV]["resource_id"],
            dataset_id=config["dataset_id"][AIRFLOW_ENV],
            fetch=False,
        ).update(
            payload={
                "url": s3_client.get_file_url(f"{OUTPUT_FOLDER}general_results.{ext}"),
                "filesize": os.path.getsize(TMP_FOLDER + f"general_results.{ext}"),
                "title": "Résultats généraux",
                "format": ext,
                "description": (
                    f"Résultats généraux des élections agrégés au niveau des bureaux de votes,"
                    " créés à partir des données du Ministère de l'Intérieur"
                    f", au format {ext}"
                    f" (dernière modification : {datetime.today().strftime('%Y-%m-%d')})"
                ),
            },
        )
        logging.info(f"Done with general results {ext}")
        local_client.resource(
            id=config["candidats"][ext][AIRFLOW_ENV]["resource_id"],
            dataset_id=config["dataset_id"][AIRFLOW_ENV],
            fetch=False,
        ).update(
            payload={
                "url": s3_client.get_file_url(
                    f"{OUTPUT_FOLDER}candidats_results.{ext}"
                ),
                "filesize": os.path.getsize(TMP_FOLDER + f"candidats_results.{ext}"),
                "title": "Résultats par candidat",
                "format": ext,
                "description": (
                    f"Résultats des élections par candidat agrégés au niveau des bureaux de votes,"
                    " créés à partir des données du Ministère de l'Intérieur"
                    f", au format {ext}"
                    f" (dernière modification : {datetime.today().strftime('%Y-%m-%d')})"
                ),
            },
        )
        logging.info(f"Done with candidats results {ext}")


@task()
def notification():
    with open(f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}elections/aggregation/config.json") as fp:
        config = json.load(fp)
    send_message(
        text=(
            "📣 Données élections mises à jour.\n\n"
            f"- Données stockées sur S3 - Bucket {S3_BUCKET_DATA_PIPELINE_OPEN}\n"
            f"- Données référencées [sur data.gouv.fr]({local_client.base_url}/datasets/"
            f"{config['dataset_id'][AIRFLOW_ENV]})"
        )
    )
