import json
import logging
import os
import re

from zipfile import ZipFile
from datetime import date
import duckdb
import requests
from airflow.sdk import task
from datagouv import Dataset
from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_HOME,
    AIRFLOW_DAG_TMP,
    AIRFLOW_ENV,
)
from datagouvfr_data_pipelines.utils.datagouv import local_client
from datagouvfr_data_pipelines.utils.retry import simple_connection_retry
from datagouvfr_data_pipelines.utils.s3 import S3Client
from datagouvfr_data_pipelines.utils.tchap import send_message
from datagouvfr_data_pipelines.data_processing.dvf.geoloc.utils.yearly_enrich import (
    enrich_year,
)
from datagouvfr_data_pipelines.data_processing.dvf.geoloc.utils.cadastre_index import (
    enrich_with_cadastre,
)

DAG_FOLDER = AIRFLOW_DAG_HOME + "datagouvfr_data_pipelines/data_processing/"
TMP_FOLDER = f"{AIRFLOW_DAG_TMP}dvf/"
CADASTRE_FILE = f"{TMP_FOLDER}cadastre.parquet"
SOURCE_DATASET_ID = "5c4ae55a634f4117716d5656"  # "Demandes de valeurs foncières" by Ministères économiques et financiers
GEOLOC_DATASET_ID = "5cc1b94a634f4165e96436c1"  # "Demandes de valeurs foncières géolocalisées" by data.gouv.fr
bucket = "dataeng-open"


@simple_connection_retry
def download_resource(res, dest_path: str) -> None:
    """Download one source file, retrying on network hiccups.
    data.gouv's static host can stall mid-stream and httpx's default read timeout is 5s,
    which is short for these ~70 MB files. Resource.download() drops its **kwargs, so the
    timeout cannot be raised from here: we retry the whole file instead."""
    res.download(dest_path)


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
def download_dvf_source_data(params: dict, **context):
    dvf_dataset = Dataset(SOURCE_DATASET_ID)
    data = [res for res in dvf_dataset.resources if res.type == "main"]
    if len(data) not in {5, 6}:
        # 5 full years, or half first and last years and 4 full years
        raise ValueError(f"Unexpected number of resources: {len(data)}")
    titles = [f'"{res.title}"' for res in data]
    logging.info(f"Available ressources : {','.join(titles)}...")

    file_names = {res.url.split("/")[-1]: res for res in data}
    # checking that the year is where we expect it in the resource's url
    for file_name in file_names:
        if not re.match(r"^valeursfoncieres-\d{4}\.txt\.zip$", file_name):
            raise ValueError(f"Unexpected file name: {file_name}")
    # computed on the whole window, before any filtering, so that the reference data
    # downloaded below is the same as in a complete run
    max_year = max(int(f.split(".")[0].split("-")[1]) for f in file_names)

    if params.get("year_to_run"):
        # debug mode: rebuilding a single year
        year_to_run = params["year_to_run"]
        file_names = {
            f: res
            for f, res in file_names.items()
            if f == f"valeursfoncieres-{year_to_run}.txt.zip"
        }
        if not file_names:
            raise ValueError(f"No source resource for year {year_to_run}")
        logging.info(f"year_to_run={year_to_run}, restricting the run to this year.")

    files = []
    for file_name, res in file_names.items():
        logging.info(f'Downloading ressource "{res.title}"...')
        dest_path = TMP_FOLDER + file_name
        download_resource(res, dest_path)
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
    logging.info("Loading config data...")
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
    logging.info(f"Starting year enrichment over {len(files)} files...")
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
    logging.info("Year enrichment over. Deleting temporary files...")
    # deleting in the end so that if the loop above fails, we can rerun safely
    for file in files:
        os.remove(TMP_FOLDER + file)

@task()
def download_cadastre_source_data(**context):
    """
    Load the right cadastre file and store locally its indexed parcelle geometries
    for faster match against centroid geopoint of the parcelles from DVF.
    """
    # Technical choice notes: Measurement on samples shows it is faster to load the full file
    # then filter it locally with duckdb rather than directly filter and load with duckdb.
    # This current task takes about 15min on a PC with wifi +350Mbps for download
    # todo : later add a parametrize way to run specifically for october or april run for debug
    year = str(date.today().year)
    millesime = f"{year}-03-01" if 4 <= date.today().month < 10 else f"{year}-06-01"
    url = (
        "https://cadastre.data.gouv.fr/data/etalab-cadastre/"
        f"{millesime}/geoparquet/france/cadastre.parquet"
    )
    if not requests.head(url, allow_redirects=True, timeout=15).ok:
        raise Exception(
            f"The required millesime {millesime} is not available. Checkout on https://cadastre.data.gouv.fr/data/etalab-cadastre/"
        )
    logging.info(f"Start loading the required millesime {millesime}...")
    raw = f"{TMP_FOLDER}cadastre-raw.parquet"  # about 21go, all layers
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(raw, "wb") as f:
            for chunk in r.iter_content(chunk_size=8 << 20):
                f.write(chunk)

    logging.info("Cadastre downloaded, filtering on parcelles...")
    con = duckdb.connect()
    con.execute(
        f"SET memory_limit='4GB'; SET temp_directory='{TMP_FOLDER}duckdb_spill';"
    )
    con.execute(f"""
        COPY (
            SELECT
                departement,
                -- 2154 for all departments but DOM : 5490 (971/972), 2972 (973), 2975 (974), 4471 (976).
                geom_srid,
                commune,
                id AS parcelle_id,
                geometry
            FROM read_parquet('{raw}')
            WHERE type_objet = 'parcelles'
        ) TO '{CADASTRE_FILE}' (FORMAT parquet, COMPRESSION zstd)
    """)
    os.remove(raw)
    logging.info(f"Parcelles written to {CADASTRE_FILE}")


@task()
def process_cadastre_cols(cadastre_file=CADASTRE_FILE):
    files = sorted(
        f for f in os.listdir(TMP_FOLDER) if f.startswith("temp-")
    )  # If year_to_run set up, len(files) == 1
    for file in files:
        enrich_with_cadastre(file, cadastre_file, tmp_folder=TMP_FOLDER)
    # deleting in the end so that if the loop above fails, we can rerun safely
    for file in files:
        os.remove(TMP_FOLDER + file)


@task()
def publish_datagouv(params: dict):
    year_to_run = params.get("year_to_run")
    # april delivery: five full years, october delivery: one more file
    # (oldest year last semester and latest year first semester)
    files = sorted(f for f in os.listdir(TMP_FOLDER) if f.startswith("full-"))
    if not year_to_run and len(files) not in {5, 6}:
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
    # (skipped in debug mode: only one year is built, the others are not obsolete)
    if not year_to_run:
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
