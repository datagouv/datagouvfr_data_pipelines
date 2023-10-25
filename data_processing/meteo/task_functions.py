from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_HOME,
    AIRFLOW_DAG_TMP,
    DATAGOUV_SECRET_API_KEY,
    AIRFLOW_ENV,
    MINIO_URL,
    MINIO_BUCKET_DATA_PIPELINE_OPEN,
    SECRET_MINIO_DATA_PIPELINE_USER,
    SECRET_MINIO_DATA_PIPELINE_PASSWORD,
    SECRET_FTP_METEO_USER,
    SECRET_FTP_METEO_PASSWORD,
    SECRET_FTP_METEO_ADDRESS
)
from datagouvfr_data_pipelines.utils.datagouv import (
    post_remote_resource,
    get_all_from_api_query,
    DATAGOUV_URL
)
from datagouvfr_data_pipelines.utils.mattermost import send_message
from datagouvfr_data_pipelines.utils.minio import send_files
import numpy as np
import os
import pandas as pd
import json
from itertools import chain
from datetime import datetime

DAG_FOLDER = "datagouvfr_data_pipelines/data_processing/"
DATADIR = f"{AIRFLOW_DAG_TMP}meteo/data"


def send_results_to_minio():
    send_files(
        MINIO_URL=MINIO_URL,
        MINIO_BUCKET=MINIO_BUCKET_DATA_PIPELINE_OPEN,
        MINIO_USER=SECRET_MINIO_DATA_PIPELINE_USER,
        MINIO_PASSWORD=SECRET_MINIO_DATA_PIPELINE_PASSWORD,
        list_files=[
            {
                "source_path": f"{DATADIR}/",
                "source_name": f"{t}_results.csv",
                "dest_path": "elections/",
                "dest_name": f"{t}_results.csv",
            } for t in ['general', 'candidats']
        ],
    )


def publish_results_elections():
    with open(f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}elections/config/dgv.json") as fp:
        data = json.load(fp)
    # post_remote_resource(
    #     api_key=DATAGOUV_SECRET_API_KEY,
    #     remote_url=f"https://object.files.data.gouv.fr/{MINIO_BUCKET_DATA_PIPELINE_OPEN}/{AIRFLOW_ENV}/elections/general_results.csv",
    #     dataset_id=data["general"][AIRFLOW_ENV]["dataset_id"],
    #     resource_id=data["general"][AIRFLOW_ENV]["resource_id"],
    #     filesize=os.path.getsize(os.path.join(DATADIR, "general_results.csv")),
    #     title="Résultats généraux",
    #     format="csv",
    #     description=f"Résultats généraux des élections agrégés au niveau des bureaux de votes, créés à partir des données du Ministère de l'Intérieur (dernière modification : {datetime.today()})",
    # )


def send_notification():
    with open(f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}elections/config/dgv.json") as fp:
        data = json.load(fp)
    # send_message(
    #     text=(
    #         ":mega: Données élections mises à jour.\n"
    #         f"- Données stockées sur Minio - Bucket {MINIO_BUCKET_DATA_PIPELINE_OPEN}"
    #         f"- Données référencées [sur data.gouv.fr]({DATAGOUV_URL}/fr/datasets/"
    #         f"{data['general'][AIRFLOW_ENV]['dataset_id']})"
    #     )
    # )
