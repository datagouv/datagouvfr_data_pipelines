import pandas as pd
from datetime import datetime, timedelta
import requests
import json

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
    AIRFLOW_DAG_HOME,
    AIRFLOW_ENV,
)
from datagouvfr_data_pipelines.utils.datagouv import (
    DATAGOUV_MATOMO_ID,
    local_client,
)
from datagouvfr_data_pipelines.utils.filesystem import File

TMP_FOLDER = f"{AIRFLOW_DAG_TMP}dgv_stats/"
DAG_FOLDER = "datagouvfr_data_pipelines/dgv/stats/"
DATADIR = f"{TMP_FOLDER}data/"
with open(f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}config/dgv.json") as fp:
    config = json.load(fp)
yesterday = datetime.today() - timedelta(days=1)


def get_current_resources():
    resources = requests.get(
        f"{local_client.base_url}/api/1/datasets/{config[AIRFLOW_ENV]['dataset_id']}/",
        headers={"X-fields": "resources{id,title}"},
    ).json()["resources"]
    return resources


def create_year_if_missing():
    resources = get_current_resources()
    yesterdays_year = yesterday.year
    for resource in resources:
        if str(yesterdays_year) in resource["title"]:
            return
    # landing here means there is no file for yesterday's year, so we create one
    # (its content doesn't matter, it will be replaced downstream)
    file = File(
        source_path=DATADIR,
        source_name="placeholder.csv",
        remote_source=True,  # not remote but not created yet
    )
    with open(file.full_source_path, "w") as f:
        f.write("tmp")
    local_client.resource().create_static(
        file_to_upload=file.full_source_path,
        payload={
            "title": f"Statistiques de consultation pour l'année {yesterdays_year}"
        },
        dataset_id=config[AIRFLOW_ENV]["dataset_id"],
    )


def get_months(site_id, year):
    params = {
        "idSite": site_id,
        "module": "API",
        "method": "API.get",
        "format": "json",
        "period": "day",
        "date": f"{year}-01-01,yesterday",
    }
    r = requests.get("https://stats.data.gouv.fr/", params=params)
    r.raise_for_status()
    df = pd.DataFrame(r.json()).transpose()
    return df


def update_year():
    resources = get_current_resources()
    yesterdays_year = yesterday.year
    current_year_resource_id = None
    for resource in resources:
        if str(yesterdays_year) in resource["title"]:
            current_year_resource_id = resource["id"]
            break
    if not current_year_resource_id:
        # this should not happen by construction but we never know
        raise ValueError("Missing current year resource")
    df = get_months(DATAGOUV_MATOMO_ID, yesterdays_year)
    df.index.name = "date"
    file = File(
        source_path=DATADIR,
        source_name=f"{yesterdays_year}-days.csv",
        remote_source=True,  # not remote but not created yet
    )
    df.to_csv(file.full_source_path)
    local_client.resource(
        id=current_year_resource_id,
        dataset_id=config[AIRFLOW_ENV]["dataset_id"],
        fetch=False,
        _from_response={
            "filetype": "file"
        },  # to be able to update the file without fetching
    ).update(
        file_to_upload=file.full_source_path,
        payload={
            "title": f"Statistiques de consultation pour l'année {yesterdays_year}"
        },
    )
