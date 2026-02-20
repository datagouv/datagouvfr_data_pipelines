import json
import logging
import os
from datetime import datetime

import pandas as pd
from airflow.decorators import task
from datagouv import Client
from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_HOME,
    AIRFLOW_DAG_TMP,
    AIRFLOW_ENV,
    S3_BUCKET_DATA_PIPELINE_OPEN,
)
from datagouvfr_data_pipelines.utils.conversions import csv_to_parquet
from datagouvfr_data_pipelines.utils.datagouv import local_client
from datagouvfr_data_pipelines.utils.filesystem import File
from datagouvfr_data_pipelines.utils.mattermost import send_message
from datagouvfr_data_pipelines.utils.s3 import S3Client

DAG_FOLDER = "datagouvfr_data_pipelines/data_processing/"
TMP_FOLDER = f"{AIRFLOW_DAG_TMP}elections/"
s3_open = S3Client(bucket=S3_BUCKET_DATA_PIPELINE_OPEN)

dtypes: dict[str, dict[str, str]] = {
    "general": {
        "id_election": "VARCHAR",
        "id_brut_miom": "VARCHAR",
        "code_departement": "VARCHAR",
        "libelle_departement": "VARCHAR",
        "code_canton": "VARCHAR",
        "libelle_canton": "VARCHAR",
        "code_commune": "VARCHAR",
        "libelle_commune": "VARCHAR",
        "code_circonscription": "VARCHAR",
        "libelle_circonscription": "VARCHAR",
        "code_bv": "VARCHAR",
        "inscrits": "INT32",
        "abstentions": "INT32",
        "votants": "INT32",
        "blancs": "INT32",
        "nuls": "INT32",
        "exprimes": "INT32",
        "ratio_abstentions_inscrits": "FLOAT",
        "ratio_votants_inscrits": "FLOAT",
        "ratio_blancs_inscrits": "FLOAT",
        "ratio_blancs_votants": "FLOAT",
        "ratio_nuls_inscrits": "FLOAT",
        "ratio_nuls_votants": "FLOAT",
        "ratio_exprimes_inscrits": "FLOAT",
        "ratio_exprimes_votants": "FLOAT",
    },
    "candidats": {
        "id_election": "VARCHAR",
        "id_brut_miom": "VARCHAR",
        "code_departement": "VARCHAR",
        "code_commune": "VARCHAR",
        "code_bv": "VARCHAR",
        "no_panneau": "INT32",
        "voix": "INT32",
        "ratio_voix_inscrits": "FLOAT",
        "ratio_voix_exprimes": "FLOAT",
        "nuance": "VARCHAR",
        "sexe": "VARCHAR",
        "nom": "VARCHAR",
        "prenom": "VARCHAR",
        "liste": "VARCHAR",
        "libelle_abrege_liste": "VARCHAR",
        "libelle_etendu_liste": "VARCHAR",
        "nom_tete_liste": "VARCHAR",
        "binome": "VARCHAR",
    },
}

_types = {
    "INT32": int,
    "FLOAT": float,
}


@task()
def process_election_data():
    # getting preprocessed resources
    resources_url = [
        r["url"]
        for r in Client().get_all_from_api_query(
            # due to https://github.com/MongoEngine/mongoengine/issues/2748
            # we have to specify a sort parameter for now
            "api/1/datasets/community_resources/"
            "?organization=646b7187b50b2a93b1ae3d45&sort=-created_at_internal"
        )
    ]
    resources = {
        "general": [r for r in resources_url if "general-results.csv" in r],
        "candidats": [r for r in resources_url if "candidats-results.csv" in r],
    }

    for scope in ["general", "candidats"]:
        logging.info(f"Processing {scope} resources")
        for idx, url in enumerate(resources[scope]):
            logging.info("> " + url)
            df = pd.read_csv(url, sep=";", dtype=str)
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
        logging.info("Export en parquet...")
        csv_to_parquet(
            csv_file_path=TMP_FOLDER + f"{scope}_results.csv",
            dtype=dtypes[scope],
        )


@task()
def send_results_to_s3():
    s3_open.send_files(
        list_files=[
            File(
                source_path=TMP_FOLDER,
                source_name=f"{scope}_results.{ext}",
                dest_path="elections/",
                dest_name=f"{scope}_results.{ext}",
                content_type=(
                    "application/vnd.apache.parquet"
                    if ext == "parquet"
                    else "text/csv"
                ),
            )
            for scope in ["general", "candidats"]
            for ext in ["csv", "parquet"]
        ],
        ignore_airflow_env=True,
    )


@task()
def publish_results_elections():
    with open(
        f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}elections/aggregation/config/dgv.json"
    ) as fp:
        config = json.load(fp)
    for ext in ["csv", "parquet"]:
        local_client.resource(
            id=config["general"][ext][AIRFLOW_ENV]["resource_id"],
            dataset_id=config["dataset_id"][AIRFLOW_ENV],
            fetch=False,
        ).update(
            payload={
                "url": (
                    f"https://object.files.data.gouv.fr/{S3_BUCKET_DATA_PIPELINE_OPEN}"
                    f"/elections/general_results.{ext}"
                ),
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
                "url": (
                    f"https://object.files.data.gouv.fr/{S3_BUCKET_DATA_PIPELINE_OPEN}"
                    f"/elections/candidats_results.{ext}"
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
def send_notification():
    with open(
        f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}elections/aggregation/config/dgv.json"
    ) as fp:
        config = json.load(fp)
    send_message(
        text=(
            ":mega: Données élections mises à jour.\n"
            f"- Données stockées sur S3 - Bucket {S3_BUCKET_DATA_PIPELINE_OPEN}\n"
            f"- Données référencées [sur data.gouv.fr]({local_client.base_url}/datasets/"
            f"{config['dataset_id'][AIRFLOW_ENV]})"
        )
    )
