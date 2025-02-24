from collections import defaultdict
from datetime import datetime, timedelta
import json
import logging
import os
from requests.exceptions import RequestException, Timeout
import pygrib
import requests
import time
import random
import shutil
import subprocess

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_HOME,
    AIRFLOW_DAG_TMP,
    AIRFLOW_ENV,
    MINIO_BUCKET_PNT,
    SECRET_MINIO_PNT_USER,
    SECRET_MINIO_PNT_PASSWORD,
)
from datagouvfr_data_pipelines.data_processing.meteo.previsions_numeriques_temps.config import (
    PACKAGES,
    MAX_LAST_BATCHES,
    BATCH_URL_SIZE,
    BATCH_URL_SIZE_PACKAGE,
)
from datagouvfr_data_pipelines.data_processing.meteo.previsions_numeriques_temps.utils import (
    MeteoClient,
    load_issues,
    save_issues,
)
from datagouvfr_data_pipelines.utils.datagouv import (
    post_remote_resource,
    delete_dataset_or_resource,
    DATAGOUV_URL,
)
from datagouvfr_data_pipelines.utils.minio import MinIOClient


DATADIR = f"{AIRFLOW_DAG_TMP}meteo_pnt/"
LOG_PATH = f"{DATADIR}logs/"
ROOT_FOLDER = "datagouvfr_data_pipelines/data_processing/"
TIME_DEPTH_TO_KEEP = timedelta(hours=24)
minio_pnt = MinIOClient(
    bucket=MINIO_BUCKET_PNT,
    user=SECRET_MINIO_PNT_USER,
    pwd=SECRET_MINIO_PNT_PASSWORD,
)
minio_folder = "pnt" if AIRFLOW_ENV == "prod" else "dev"
meteo_client = MeteoClient()


def get_last_batch_hour() -> datetime:
    now = datetime.now()
    if now.hour < 6:
        batch_hour = 0
    elif now.hour < 12:
        batch_hour = 6
    elif now.hour < 18:
        batch_hour = 12
    else:
        batch_hour = 18
    return now.replace(
        second=0,
        microsecond=0,
        minute=0,
        hour=batch_hour,
    )


def get_new_batches(batches: list, url: str) -> list:
    r = meteo_client.get(url, timeout=10)
    r.raise_for_status()
    new_batches = []
    if "links" in r.json():
        for batch in batches:
            for link in r.json()["links"]:
                if batch in link["href"]:
                    new_batches.append(batch)
    return new_batches


def get_latest_theorical_batches(ti, model: str, pack: str, grid: str, **kwargs):
    batches = []
    if model != "arome":
        # arome runs every 3h
        for i in range(MAX_LAST_BATCHES):
            batches.append((
                get_last_batch_hour() - timedelta(hours=6 * i)
            ).strftime("%Y-%m-%dT%H:%M:%SZ"))
    else:
        # the others every 6h
        for i in range(MAX_LAST_BATCHES * 2):
            batches.append((
                get_last_batch_hour() - timedelta(hours=3 * i)
            ).strftime("%Y-%m-%dT%H:%M:%SZ"))
        batch3hlater = datetime.strptime(
            batches[0], "%Y-%m-%dT%H:%M:%SZ"
        ) + timedelta(hours=3)
        if batch3hlater < datetime.now():
            batches.append(batch3hlater.strftime("%Y-%m-%dT%H:%M:%SZ"))
    logging.info(f"All batches: {batches}")
    tested_batches: list = get_new_batches(
        batches,
        PACKAGES[model][pack][grid]["check_availability_url"],
    )
    logging.info(f"Tested batches: {tested_batches}")
    ti.xcom_push(key="batches", value=batches)
    ti.xcom_push(key="tested_batches", value=tested_batches)


def clean_old_runs_in_minio(ti):
    batches = ti.xcom_pull(key="batches", task_ids="get_latest_theorical_batches")
    # we get the runs' names from the folders
    get_list_runs = minio_pnt.get_files_from_prefix(
        prefix=minio_folder,
        recursive=False,
    )
    old_dates = []
    keep_dates = []
    for run in get_list_runs:
        # run.object_name looks like "{minio_folder}/2024-10-02T00:00:00Z/"
        run = run.object_name.split('/')[1]
        if run < batches[-1] and run not in old_dates:
            old_dates.append(run)
        if run >= batches[-1] and run not in keep_dates:
            keep_dates.append(run)
    if len(keep_dates) > 3:
        for od in old_dates:
            minio_pnt.delete_files_from_prefix(prefix=f"{minio_folder}/{od}")


def build_folder_path(model: str, pack: str, grid: str) -> str:
    base_path = model if pack.startswith("$") else f"{model}/{pack}"
    return f"{base_path}/{grid.replace('.', '')}"


def construct_all_possible_files(ti, model: str, pack: str, grid: str, **kwargs):
    tested_batches = ti.xcom_pull(key="tested_batches", task_ids="get_latest_theorical_batches")
    nb_files = 0
    minio_paths = []
    url_to_infos = {}
    minio_path_to_url = {}
    for batch in tested_batches:
        for package in PACKAGES[model][pack][grid]["packages"]:
            for timeslot in package.time:
                url = (
                    f"{kwargs['infos']['base_url']}/{grid}/packages/"
                    + f"{package.name}/{kwargs['infos']['product']}"
                    + f"?&referencetime={batch}&time={timeslot}&format={kwargs['infos']['extension']}"
                )
                base_name = model if pack.startswith("$") else f"{model}-{pack}"
                filename = (
                    f"{base_name}__{grid.replace('.', '')}__{package.name}__"
                    + f"{timeslot}__{batch}.{kwargs['infos']['extension']}"
                )
                path = build_folder_path(model, pack, grid)
                minio_path = f"{minio_folder}/{batch}/{path}/{package.name}/{filename}"
                nb_files += 1
                minio_paths.append(minio_path)
                url_to_infos[url] = {
                    "filename": filename,
                    "minio_path": minio_path,
                    "package": package.name,
                }
                minio_path_to_url[minio_path] = url

    logging.info(f"{nb_files} possible files")

    issues = load_issues(f"{DATADIR}{path}")
    to_get = [
        minio_path for minio_path in minio_paths
        if (
            not minio_pnt.does_file_exist_on_minio(minio_path)
            # the urls are stored in issues, we get them from the minio path
            or minio_path_to_url[minio_path] in issues
        )
    ]

    logging.info(f"{len(to_get)} possible files after removing already processed files")

    if len(to_get) == 0:
        logging.info("No new data, exit")
        return False

    ti.xcom_push(key="url_to_infos", value=url_to_infos)
    ti.xcom_push(key="to_get", value=to_get)
    ti.xcom_push(key="minio_path_to_url", value=minio_path_to_url)
    return True


def test_file_structure(filepath: str) -> bool:
    # open and check that grib file is properly structured
    try:
        grib = pygrib.open(filepath)
        for msg in grib:
            msg.values.shape
        return True
    except Exception as e:
        logging.warning(f"An error occured for {filepath}: `{e}`")
        return False


def log_and_send_error(filename):
    log_name = f"{filename.split('.')[0]}-{int(datetime.now().timestamp())}.log"
    with open(LOG_PATH + log_name, "w") as f:
        f.write(f"{filename};{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    minio_pnt.send_files(
        list_files=[
            {
                "source_path": LOG_PATH,
                "source_name": log_name,
                "dest_path": "logs/" if AIRFLOW_ENV == "prod" else "dev/logs/",
                "dest_name": log_name,
            }
        ],
        ignore_airflow_env=True,
        burn_after_sending=True,
    )
    logging.info(f"Sent and locally erased log for {filename}")


def send_files_to_minio(ti, model: str, pack: str, grid: str, **kwargs) -> None:
    url_to_infos = ti.xcom_pull(key="url_to_infos", task_ids="construct_all_possible_files")
    to_get = ti.xcom_pull(key="to_get", task_ids="construct_all_possible_files")
    minio_path_to_url = ti.xcom_pull(key="minio_path_to_url", task_ids="construct_all_possible_files")
    path = build_folder_path(model, pack, grid)
    logging.info(f"Getting {len(to_get)} files")
    # we could also put the content of the loop within an async function and process the files simultaneously
    uploaded = []
    my_packages = []
    for minio_path in to_get:
        url = minio_path_to_url[minio_path]
        package = url_to_infos[url]['package']
        logging.info("_________________________")
        logging.info(url_to_infos[url]["filename"])
        if os.path.isdir(f"{DATADIR}{path}/{package}") and package not in my_packages:
            logging.info(f"{url_to_infos[url]['package']} is already being processed by another run")
            continue
        else:
            # this is to make sure concurrent runs don't interfere or process the same data
            os.makedirs(f"{DATADIR}{path}/{package}", exist_ok=True)
            my_packages.append(package)
        local_filename = f"{DATADIR}{path}/{package}/{url_to_infos[url]['filename']}"
        # ideally we'd like to minio_pnt.send_from_url directly but we have to test the file structure first
        with meteo_client.get(
            url,
            stream=True,
            timeout=60,
            # do we actually need these headers? response content type is binary
            headers={"Content-Type": "application/json; charset=utf-8"},
        ) as r:
            r.raise_for_status()
            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=32768):
                    f.write(chunk)

        issues = load_issues(f"{DATADIR}{path}")
        if test_file_structure(local_filename):
            # if the test is successful, we upload the file (and remove it from issues if needed)
            logging.info("Structure test successful")
            minio_pnt.send_from_url(
                url=url,
                destination_file_path=minio_path,
                session=meteo_client,
            )
            if url in issues:
                logging.info("Removing it from issues")
                issues.remove(url)
                save_issues(issues, f"{DATADIR}{path}")
            uploaded.append(minio_path)
        elif url not in issues:
            # if the test is not successful and the file wasn't already flagged as an issue we:
            # - send it anyway (specifically requested by Météo France)
            # - add it to the issues
            logging.warning(url_to_infos[url]["filename"] + " is badly structured, but sending anyway")
            minio_pnt.send_from_url(
                url=url,
                destination_file_path=minio_path,
                session=meteo_client,
            )
            issues.append(url)
            logging.info("Adding it to issues")
            save_issues(issues, f"{DATADIR}{path}")
            log_and_send_error(url_to_infos[url]["filename"])
            uploaded.append(minio_path)
        else:
            # known issue, just passing, we'll try again next time
            logging.info("This file is an already known issue, passing")
        # deleting the file locally
        os.remove(local_filename)
    for p in my_packages:
        # making way for later occurrences
        os.removedirs(f"{DATADIR}{path}/{p}")
    ti.xcom_push(key="uploaded", value=uploaded)
