from collections import defaultdict
from datetime import datetime, timedelta
import json
import logging
import os
import requests
import shutil
import subprocess

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_HOME,
    AIRFLOW_DAG_TMP,
    AIRFLOW_ENV,
    MINIO_URL,
    SECRET_MINIO_METEO_PE_USER,
    SECRET_MINIO_METEO_PE_PASSWORD,
)
from datagouvfr_data_pipelines.utils.datagouv import (
    post_remote_resource,
    delete_dataset_or_resource,
    DATAGOUV_URL,
)
from datagouvfr_data_pipelines.utils.minio import MinIOClient
from datagouvfr_data_pipelines.utils.sftp import SFTPClient


DATADIR = f"{AIRFLOW_DAG_TMP}meteo_pnt/"
ROOT_FOLDER = "datagouvfr_data_pipelines/data_processing/"
TIME_DEPTH_TO_KEEP = timedelta(hours=24)
bucket_pe = "meteofrance-pnt"
minio_meteo = MinIOClient(
    bucket=bucket_pe,
    user=SECRET_MINIO_METEO_PE_USER,
    pwd=SECRET_MINIO_METEO_PE_PASSWORD,
)
minio_folder = "data"
upload_dir = "/uploads/"

with open(f"{AIRFLOW_DAG_HOME}{ROOT_FOLDER}meteo/previsions_densemble/config.json") as fp:
    CONFIG = json.load(fp)


def create_client():
    return SFTPClient(
        conn_name="SSH_TRANSFER_INFRA_DATA_GOUV_FR",
        user="meteofrance",
        # you may have to edit the dev value depending on your local conf
        key_type="RSA" if AIRFLOW_ENV == "dev" else "Ed25519",
    )


