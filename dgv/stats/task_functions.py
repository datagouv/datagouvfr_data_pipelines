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
    DATAGOUV_URL,
    post_resource,
)

TMP_FOLDER = f"{AIRFLOW_DAG_TMP}dgv_stats/"
DAG_FOLDER = "datagouvfr_data_pipelines/dgv/stats/"
DATADIR = f"{TMP_FOLDER}data"
with open(f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}config/dgv.json") as fp:
    config = json.load(fp)
yesterday = datetime.today() - timedelta(days=1)


def get_current_resources():
    resources = requests.get(
        f"{DATAGOUV_URL}/api/1/datasets/{config[AIRFLOW_ENV]['dataset_id']}/",
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
    with open(DATADIR + "/placeholder.csv", "w") as f:
        f.write("tmp")
    post_resource(
        file_to_upload={"dest_path": DATADIR, "dest_name": "placeholder.csv"},
        dataset_id=config[AIRFLOW_ENV]["dataset_id"],
        payload={"title": f"Statistiques de consultation pour l'année {yesterdays_year}"},
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
    df.index.name = ("date")
    df.to_csv(DATADIR + f"/{yesterdays_year}-days.csv")
    post_resource(
        file_to_upload={"dest_path": DATADIR, "dest_name": f"{yesterdays_year}-days.csv"},
        dataset_id=config[AIRFLOW_ENV]["dataset_id"],
        resource_id=current_year_resource_id,
        payload={"title": f"Statistiques de consultation pour l'année {yesterdays_year}"},
    )
