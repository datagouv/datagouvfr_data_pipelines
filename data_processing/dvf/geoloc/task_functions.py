import gzip
import json
import logging
import os
import re
import shutil
from datetime import date
from zipfile import ZipFile

import duckdb
import requests
from airflow.sdk import task
from datagouv import Dataset
from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_HOME,
    AIRFLOW_DAG_TMP,
    AIRFLOW_ENV,
)
from datagouvfr_data_pipelines.data_processing.dvf.geoloc.utils.cadastre_index import (
    enrich_with_cadastre,
)
from datagouvfr_data_pipelines.data_processing.dvf.geoloc.utils.delivery import (
    Delivery,
)
from datagouvfr_data_pipelines.data_processing.dvf.geoloc.utils.dvf_resource import (
    get_dvf_resources,
)
from datagouvfr_data_pipelines.data_processing.dvf.geoloc.utils.dvf_version import (
    DvfVersion,
)
from datagouvfr_data_pipelines.data_processing.dvf.geoloc.utils.yearly_enrich import (
    enrich_year,
)
from datagouvfr_data_pipelines.utils.datagouv import demo_client, local_client
from datagouvfr_data_pipelines.utils.retry import simple_connection_retry
from datagouvfr_data_pipelines.utils.s3 import S3Client
from datagouvfr_data_pipelines.utils.tchap import send_message

DAG_FOLDER = AIRFLOW_DAG_HOME + "datagouvfr_data_pipelines/data_processing/"
TMP_FOLDER = f"{AIRFLOW_DAG_TMP}dvf/"
CADASTRE_FILE = f"{TMP_FOLDER}cadastre.parquet"

# TODO: delete after the test phase, and replace publish_client / get_source_dataset by
# local_client / Dataset(SOURCE_DATASET_ID)
DRY_RUN = True  # publish on demo even when running on prod, to review the output first
APRIL_SOURCE_DATASET_ID = "6ace2da98d1331e15433ff9f"  # April delivery copied on demo
publish_client = demo_client if DRY_RUN else local_client


def get_source_dataset(delivery: str | None) -> Dataset:
    if delivery == "april" and AIRFLOW_ENV == "dev":
        return Dataset(APRIL_SOURCE_DATASET_ID, _client=demo_client)
    return Dataset(SOURCE_DATASET_ID)


def get_geoloc_dataset_id(delivery: str | None) -> str:
    if not (delivery == "april" and AIRFLOW_ENV == "dev"):
        return GEOLOC_OCTOBER_DATASET_ID
    return GEOLOC_DATASET_ID


# TODO: stop delete here

SOURCE_DATASET_ID = "5c4ae55a634f4117716d5656"  # "Demandes de valeurs foncières" by Ministères économiques et financiers
GEOLOC_DATASET_ID = "5cc1b94a634f4165e96436c1"  # "Demandes de valeurs foncières géolocalisées" by data.gouv.fr
GEOLOC_OCTOBER_DATASET_ID = "6ad249b7c56822dfb61a3585"  # demo: copy of "Demandes de valeurs foncières géolocalisées" by sylvain.lesage
bucket = "dataeng-open"
# same file name as the single-file resource already published, which is how we find it back
FULL_FILE = "dvf.csv.gz"


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


def _get_delivery(params: dict, run_date: date) -> Delivery:
    delivery_key = params.get("delivery")
    if delivery_key is None:
        return Delivery.from_date(run_date)
    else:
        return Delivery.from_key(delivery_key)


def _check_output_file_names(params: dict, output_file_names: list[str]) -> None:
    """Check that all the files are present and correct with respect to the delivery"""
    run_date = date.today()
    delivery = _get_delivery(params, run_date)
    dvf_versions = [
        DvfVersion.from_output_file_name(output_file_name)
        for output_file_name in output_file_names
    ]
    delivery.check_dvf_versions(dvf_versions)


@task()
def download_dvf_source_data(params: dict, **context):
    """Download the source data (5 or 6 .txt.zip files) and return the list of paths to the unzipped .txt files"""
    dvf_dataset = Dataset(SOURCE_DATASET_ID)

    # Each task calls date.today() independently; the risk of a mismatch between
    # tasks is limited since only the month and year are used.
    run_date = date.today()
    dvf_resources = get_dvf_resources(
        dvf_dataset.resources, _get_delivery(params, run_date)
    )

    if params.get("year_to_run"):
        # debug mode: rebuilding a single year
        year_to_run = params["year_to_run"]
        dvf_resources = [
            dvf_resource
            for dvf_resource in dvf_resources
            if dvf_resource.dvf_version.year == year_to_run
        ]
        if not dvf_resources:
            raise ValueError(f"No source resource for year {year_to_run}")
        logging.info(f"year_to_run={year_to_run}, restricting the run to this year.")

    unzipped_file_names: list[str] = []
    for dvf_resource in dvf_resources:
        logging.info(f'Downloading ressource "{dvf_resource.resource.title}"...')
        dest_path = TMP_FOLDER + dvf_resource.dvf_version.get_zip_file_name()
        download_resource(dvf_resource.resource, dest_path)
        with ZipFile(dest_path, mode="r") as z:
            zipped = z.namelist()
            if len(zipped) != 1:
                raise ValueError("Unexpected number of files in zip")
            z.extractall(TMP_FOLDER)
            unzipped_file_name = zipped[0]
            expected_file_name = dvf_resource.dvf_version.get_unzipped_file_name()
            if unzipped_file_name != expected_file_name:
                raise ValueError(
                    f"Unexpected unzipped file name {unzipped_file_name}, expected {expected_file_name}."
                )
            unzipped_file_names.append(unzipped_file_name)
        os.remove(dest_path)

    return unzipped_file_names


@task()
def enrich_years(unzipped_file_names: list[str], **context):
    """Enrich the data (change column names, improve date, codes and texts, add latitude/longitude and id_mutation).

    See enrich_year() docstring.

    The results are put in one Parquet file per source file. The source files (unzipped txt files) are deleted.
    """

    # we can't parallelize for RAM containment

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
    logging.info(f"Starting year enrichment over {len(unzipped_file_names)} files...")
    for unzipped_file_name in unzipped_file_names:
        enrich_year(
            unzipped_file_name,
            TMP_FOLDER,
            map_cultures=map_cultures,
            output_schema=output_schema,
            available_dates=available_dates,
            s3_client=s3_client,
            bucket=bucket,
        )
    logging.info("Year enrichment over. Deleting temporary files...")
    # deleting in the end so that if the loop above fails, we can rerun safely
    for unzipped_file_name in unzipped_file_names:
        os.remove(TMP_FOLDER + unzipped_file_name)


def get_cadastre_millesime(delivery: Delivery, today: date) -> str:
    """Return the cadastre millesime of the latest given delivery,
    e.g. "2026-06-01" for an October 2026 delivery."""
    # before the delivery month, the latest delivery of this kind is last year's
    year = today.year if today.month >= delivery.month else today.year - 1
    return f"{year}-{delivery.cadastre_millesime}-01"


@task()
def download_cadastre_source_data(params: dict, **context):
    """Prepare a local Parquet file with the "current" cadastre parcelles geometries.

    "current": the cadastre that corresponds the delivery (april or october) and year.

    Download the cadastre file (Parquet)
    Filter it to keep only the "parcelles" rows and the following columns: departement,
    geom_srid, commune, parcelle_id, geometry.
    Save it to a local Parquet file.
    """
    # Technical choice notes: Measurement on samples shows it is faster to load the full file
    # then filter it locally with duckdb rather than directly filter and load with duckdb.
    # This current task takes about 15min on a PC with wifi +350Mbps for download

    run_date = date.today()
    millesime = get_cadastre_millesime(_get_delivery(params, run_date), run_date)
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
    """Match the latitude and longitude with the current cadastre to fix the parcelle_id

    If parcelle_id and (latitude, longitude) don't match the current cadastre
    (comparing with the centroid geopoint of the parcelles), find the current
    parcelle_id, fill ancien_code_commune, ancien_nom_commune, ancien_id_parcelle,
    and replace code_commune, nom_commune and id_parcelle

    Write the result as .csv.gz files, and delete the temporary Parquet ones.
    """
    temp_parquet_file_names = sorted(
        f for f in os.listdir(TMP_FOLDER) if DvfVersion.is_temp_parquet_file_name(f)
    )  # If year_to_run set up, len(files) == 1
    for temp_parquet_file_name in temp_parquet_file_names:
        enrich_with_cadastre(
            temp_parquet_file_name, cadastre_file, tmp_folder=TMP_FOLDER
        )
    # deleting in the end so that if the loop above fails, we can rerun safely
    for temp_parquet_file_name in temp_parquet_file_names:
        os.remove(TMP_FOLDER + temp_parquet_file_name)


@task()
def build_full_file(params: dict):
    """Concatenate the yearly files into a single file covering the whole window.
    Streamed, as the window is several GB uncompressed."""
    if params.get("year_to_run"):
        logging.info(
            "year_to_run is set: skipping the full file, the window is incomplete"
        )
        return
    output_file_names = sorted(
        f for f in os.listdir(TMP_FOLDER) if DvfVersion.is_output_file_name(f)
    )

    _check_output_file_names(params, output_file_names)

    expected_header = None
    with gzip.open(TMP_FOLDER + FULL_FILE, "wb") as out:
        for output_file_name in output_file_names:
            logging.info(f"Appending {output_file_name}...")
            with gzip.open(TMP_FOLDER + output_file_name, "rb") as f:
                header = f.readline()
                if expected_header is None:
                    expected_header = header
                    out.write(header)
                elif header != expected_header:
                    raise ValueError(
                        f"{output_file_name} does not have the same columns as {output_file_names[0]}"
                    )
                shutil.copyfileobj(f, out)
    logging.info(f"{FULL_FILE} built from {len(output_file_names)} files")


@task()
def publish_datagouv(params: dict):
    year_to_run = params.get("year_to_run")
    output_file_names = sorted(
        f for f in os.listdir(TMP_FOLDER) if DvfVersion.is_output_file_name(f)
    )
    if not year_to_run:
        _check_output_file_names(params, output_file_names)

    # matching resources to files by year (and not by index), so that a given year always
    # keeps the same resource: we want to replace resources when we can, so that we keep
    # the history. Legacy resources that don't hold a yearly file (the single file and the CSV
    # tree) are not managed here.
    # note that a year may be full year of half year (s1 or s2).
    dataset = publish_client.dataset(get_geoloc_dataset_id(params.get("delivery")))
    # dataset = local_client.dataset(get_geoloc_dataset_id(params.get("delivery"))) # TODO: delete line above and uncomment this one
    existing = {
        year: res
        for res in dataset.resources
        if res.type == "main"
        and (
            year := DvfVersion.search_year_in_resource_url(res.url)
        )  # url match avoids any wrong match with a resource renamed on UI
    }  # {"2021": Resource(title="DVF 2021 (2nd semestre)"...), ..., "2025": Resource(...)}

    to_publish = set()
    for output_file_name in output_file_names:
        dvf_version = DvfVersion.from_output_file_name()
        id = dvf_version.get_id()
        year = dvf_version.year
        to_publish.add(year)
        kwargs = {
            "payload": {"title": dvf_version.title},
            "file_to_upload": TMP_FOLDER + output_file_name,
        }
        if year in existing:
            logging.info(f"Updating resource for {id}")
            existing[year].update(**kwargs)
        else:
            logging.info(f"Creating resource for {id}")
            dataset.create_static(**kwargs)

    # oldest year last semester that fell out of the rolling window
    # (skipped in debug mode: only one year is built, the others are not obsolete)
    if not year_to_run:
        for year, res in existing.items():
            if year not in to_publish:
                logging.info(f"Deleting resource for {year}, out of the window")
                res.delete()
        publish_full_file(dataset, sorted(to_publish))


def publish_full_file(dataset, years: list[str]) -> None:
    # april delivery: five full years, october delivery: from the last semester of the
    # oldest year to the first semester of the latest one
    first, last = ("janvier", "décembre") if len(years) == 5 else ("juillet", "juin")
    label = f"{first} {years[0]} - {last} {years[-1]}"
    existing = [
        res
        for res in dataset.resources
        if res.type == "main" and res.url.endswith(f"/{FULL_FILE}")
    ]
    if len(existing) > 1:
        raise ValueError(
            f"Several resources hold {FULL_FILE}: {[r.id for r in existing]}"
        )
    if not existing:
        logging.info(f"Creating the single-file resource ({label})")
        dataset.create_static(
            payload={"title": f"DVF {label} - fichier unique"},
            file_to_upload=TMP_FOLDER + FULL_FILE,
        )
        return
    res = existing[0]
    # the title is managed in the UI: only its period is ours to update
    title, replaced = re.subn(r"\w+ \d{4} - \w+ \d{4}", label, res.title)
    if not replaced:
        logging.warning(f"No period found in {res.title!r}, keeping the title as is")
    logging.info(f"Updating the single-file resource: {title!r}")
    res.update(payload={"title": title}, file_to_upload=TMP_FOLDER + FULL_FILE)


@task()
def notification(params: dict) -> None:
    send_message(
        f"DVF géolocalisé mis à jour :\n"
        f"\n- publié [sur {'demo.' if publish_client is demo_client else ''}data.gouv.fr]"
        f"({publish_client.base_url}/datasets/{get_geoloc_dataset_id(params.get('delivery'))})"
    )
