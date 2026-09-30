"""Shared helpers for the standalone scripts preparing the elections sources.

These scripts run outside Airflow. Expected environment variables:
S3_ENDPOINT, S3_BUCKET, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY.
"""

import json
import os
import shutil
from pathlib import Path

import boto3
import requests

from datagouvfr_data_pipelines.data_processing.elections.aggregation.schema import (
    SCOPES,
)

SOURCES_FILE = Path(__file__).resolve().parents[1] / "sources.json"
S3_PREFIX = "elections/sources/"
DATAGOUV_API = "https://www.data.gouv.fr/api/1/"
TIMEOUT = 60
_EXTRA_ARGS = {"ContentType": "text/csv", "ACL": "public-read"}


def get_s3_bucket():
    return boto3.resource(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    ).Bucket(os.environ["S3_BUCKET"])


def source_key(key: str, scope: str) -> str:
    return f"{S3_PREFIX}{key}/{scope}-results.csv"


def upload_file(bucket, local_path: str | Path, key: str) -> None:
    bucket.upload_file(str(local_path), key, ExtraArgs=_EXTRA_ARGS)


def download_file(url: str, local_path: str | Path) -> None:
    with requests.get(url, stream=True, timeout=TIMEOUT) as response:
        response.raise_for_status()
        # the static server gzips responses, and raw does not decode by default
        response.raw.decode_content = True
        with open(local_path, "wb") as fp:
            shutil.copyfileobj(response.raw, fp)


def get_dataset(dataset_id: str) -> dict:
    response = requests.get(f"{DATAGOUV_API}datasets/{dataset_id}/", timeout=TIMEOUT)
    response.raise_for_status()
    return response.json()


def load_sources() -> dict:
    return json.loads(SOURCES_FILE.read_text())


def save_sources(sources: dict) -> None:
    SOURCES_FILE.write_text(
        json.dumps(dict(sorted(sources.items())), indent=4, ensure_ascii=False) + "\n"
    )


def register_source(
    sources: dict,
    key: str,
    id_elections: list[str],
    source_dataset_id: str,
    source_last_update: str,
) -> None:
    sources[key] = {
        "id_elections": sorted(id_elections),
        "source_dataset_id": source_dataset_id,
        "source_last_update": source_last_update,
        "files": {scope: source_key(key, scope) for scope in SCOPES},
    }
