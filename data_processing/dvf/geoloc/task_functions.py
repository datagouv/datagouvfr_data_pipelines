import json
import logging
import os
import re

from zipfile import ZipFile

import requests
from airflow.sdk import task
from datagouv import Dataset
from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_HOME,
    AIRFLOW_DAG_TMP,
    AIRFLOW_ENV,
)
from datagouvfr_data_pipelines.utils.datagouv import local_client
from datagouvfr_data_pipelines.utils.s3 import S3Client
from datagouvfr_data_pipelines.utils.tchap import send_message
from dags.datagouvfr_data_pipelines.data_processing.dvf.geoloc.utils.enrich_year import (
    enrich_year,
)

DAG_FOLDER = AIRFLOW_DAG_HOME + "datagouvfr_data_pipelines/data_processing/"
TMP_FOLDER = f"{AIRFLOW_DAG_TMP}dvf/"
SOURCE_DATASET_ID = "5c4ae55a634f4117716d5656"
GEOLOC_DATASET_ID = "5cc1b94a634f4165e96436c1"
bucket = "dataeng-open"


def check_if_modif():
    # triggering the pipeline if any of the source dataset's resource has been
    # updated more recently than the agregated file
    # with open(DAG_FOLDER + "dvf/explore/config.json", "r") as f:
    #     config = json.load(f)
    # return Resource(
    #     id=config["concat"]["prod"]["resource_id"],
    # ).check_if_more_recent_update(dataset_id=SOURCE_DATASET_ID)

    # bypassing for now, the DAG has not completed yet
    return True


@task()
def download_source_data(**context):
    dvf_dataset = Dataset(SOURCE_DATASET_ID)
    data = [res for res in dvf_dataset.resources if res.type == "main"]
    if len(data) not in {5, 6}:
        # 5 full years, or half first and last years and 4 full years
        raise ValueError(f"Unexpected number of resources: {len(data)}")
    files = []
    max_year = 2000
    for res in data:
        logging.info(res.title)
        file_name = res.url.split("/")[-1]
        # checking that the year is where we expect it in the resource's title
        if not re.match(r"^valeursfoncieres-\d{4}\.txt\.zip$", file_name):
            raise ValueError(f"Unexpected file name: {file_name}")
        max_year = max(max_year, int(file_name.split(".")[0].split("-")[1]))
        dest_path = TMP_FOLDER + file_name
        res.download(dest_path)
        with ZipFile(dest_path, mode="r") as z:
            zipped = z.namelist()
            if len(zipped) != 1:
                raise ValueError("Unexpected number of files in zip")
            z.extractall(TMP_FOLDER)
            files.append(zipped[0])
        os.remove(dest_path)

    logging.info("Retrieving reference data...")
    # downloading cadastre data for downstream merges
    # one major version per year since 2020, which was version 0.x.y (then incremented each year, ask Thomas for more infos)
    version = max_year - 2020
    r = requests.get(
        f"https://unpkg.com/@etalab/decoupage-administratif@{version}/data/communes.json"
    ).json()
    arrondissements_muni = [
        {"nom": k["nom"], "code": k["code"], "type": "COM"}
        for k in r
        if k["type"] == "arrondissement-municipal"
    ]
    context["ti"].xcom_push(key="arrondissements_muni", value=arrondissements_muni)
    return files


@task()
def enrich_years(files, **context):
    # we can't parallelize for RAM containment

    # arrondissements_muni = context["ti"].xcom_pull(
    #     key="arrondissements_muni", task_ids="download_source_data"
    # )
    map_cultures = {}
    for scope in ["cultures", "cultures-speciales"]:
        with open(DAG_FOLDER + f"dvf/geoloc/data/{scope}.json", "r") as f:
            map_cultures[scope] = json.load(
                f
            )  # {"cultures": {"AB": "terrains a bâtir", ...}, "cultures-speciales": {"ABREU": "Abreuvoirs",...}}
    with open(DAG_FOLDER + "dvf/geoloc/data/output_schema.json", "r") as f:
        output_schema = json.load(
            f
        )  # {"id_mutation": "object", ...}, in the published column order
    s3_client = S3Client(
        bucket=bucket,
        conn_name="S3_OVH_RBX",
    )
    # Maps each cadastre snapshot date to its S3 file path
    available_dates = {
        o.split(".")[0].split("-", maxsplit=3)[-1]: o
        for o in s3_client.get_files_from_prefix(
            "parcelles/",
            ignore_airflow_env=True,
        )
    }  # {"2020-01-01": "parcelles/cadastre-point-wgs84-2020-01-01.parquet", ...}
    logging.info(f"Available cadastre snapshots : {available_dates}")
    for file in files:
        enrich_year(
            file,
            TMP_FOLDER,
            map_cultures=map_cultures,
            output_schema=output_schema,
            available_dates=available_dates,
            s3_client=s3_client,
            bucket=bucket,
        )
    # deleting in the end so that if the loop above fails, we can rerun safely
    for file in files:
        os.remove(TMP_FOLDER + file)


@task()
def publish_datagouv():
    # april delivery: five full years, october delivery: one more file
    # (oldest year last semester and latest year first semester)
    files = sorted(f for f in os.listdir(TMP_FOLDER) if f.startswith("full-"))
    if len(files) not in {5, 6}:
        raise ValueError(f"Unexpected number of files to publish: {len(files)}")

    # matching resources to files by year (and not by index), so that a given year always
    # keeps the same resource: we want to replace resources when we can, so that we keep
    # the history. Legacy resources that don't hold a yearly file (the single file and the CSV
    # tree) are not managed here.
    dataset = local_client.dataset(GEOLOC_DATASET_ID)
    existing = {
        match.group(1): res
        for res in dataset.resources
        if res.type == "main"
        and (
            match := re.search(r"full-(\d{4})\.", res.url)
        )  # url match avoids any wrong match with a resource renamed on UI
    }  # {"2021": Resource(title="DVF 2021"...), ..., "2025": Resource(...)}

    to_publish = set()
    for file in files:
        year = file.split(".")[0].split("-")[1]  # "full-2022.csv.gz" => "2022"
        to_publish.add(year)
        kwargs = {
            "payload": {"title": f"DVF {year}"},
            "file_to_upload": TMP_FOLDER + file,
        }
        if year in existing:
            logging.info(f"Updating resource for {year}")
            existing[year].update(**kwargs)
        else:
            logging.info(f"Creating resource for {year}")
            dataset.create_static(**kwargs)

    # oldest year last semester that fell out of the rolling window
    for year, res in existing.items():
        if year not in to_publish:
            logging.info(f"Deleting resource for {year}, out of the window")
            res.delete()


@task()
def notification() -> None:
    send_message(
        f"DVF géolocalisé mis à jour :\n"
        f"\n- publié [sur {'demo.' if AIRFLOW_ENV == 'dev' else ''}data.gouv.fr]"
        f"({local_client.base_url}/datasets/{GEOLOC_DATASET_ID})"
    )
