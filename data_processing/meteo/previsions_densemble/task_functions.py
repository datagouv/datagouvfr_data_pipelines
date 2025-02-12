from collections import defaultdict
from datetime import datetime, timedelta
import json
import logging
import os
import requests
import shutil

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
    DATAGOUV_URL,
)
from datagouvfr_data_pipelines.utils.minio import MinIOClient
from datagouvfr_data_pipelines.utils.sftp import SFTPClient


DATADIR = f"{AIRFLOW_DAG_TMP}meteo_pe/"
ROOT_FOLDER = "datagouvfr_data_pipelines/data_processing/"
TIME_DEPTH_TO_KEEP = timedelta(hours=24)
bucket_pe = "meteofrance-pe"
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
    return SFTPClient(conn_name="SFTP_TRANSFER", user="meteofrance")


def get_file_infos(file_name: str):
    # files look like this: arome_pecaledonie_202409230600_mb0_ncaled0025_00:00.grib
    pack, _, date, membre, subpack, grid = file_name.split(".")[0].split("_")
    return {
        "pack": pack,
        "subpack": subpack,
        "date": date,
        "membre": membre,
        "grid": grid,
    }


def get_files_list_on_sftp(ti):
    sftp = create_client()
    files = sftp.list_files_in_directory(upload_dir)
    logging.info(f"{len(files)} files in {upload_dir}")
    # we recreate the structure of the config file: packs => subpacks => files
    to_process = defaultdict(lambda: defaultdict(dict))
    nb = 0
    for f in files:
        if not f.endswith(".grib"):
            logging.warning(f"> ignoring {f}")
        else:
            infos = get_file_infos(f)
            if (
                infos["pack"] not in CONFIG
                or infos["subpack"] not in CONFIG[infos["pack"]]
            ):
                raise ValueError(f"Got an unexpected pack: {infos['pack']}_{infos['subpack']}")
            to_process[infos["pack"]][infos["subpack"]].update({
                f: infos,
            })
            nb += 1
    logging.info(f"{nb} files to process")
    for pack in to_process:
        for subpack in to_process[pack]:
            ti.xcom_push(key=f"{pack}_{subpack}", value=to_process[pack][subpack])


def process_members(members: list[str], date: str, grid: str, pack: str, subpack: str, sftp):
    tmp_folder = f"{pack}_{subpack}_{date}_{grid}/"
    logging.info(f"Processing {tmp_folder}")
    os.mkdir(DATADIR + tmp_folder)
    # temporarily consider only a few files to make it quicker
    for file in members[:2]:
        sftp.download_file(
            remote_file_path=upload_dir + file,
            local_file_path=DATADIR + tmp_folder + file,
        )
    # grouping all members of the occurrence in a zip
    logging.info("> Zipping")
    shutil.make_archive(DATADIR + tmp_folder[:-1], 'zip', DATADIR + tmp_folder)
    minio_meteo.send_files(
        [
            {
                "source_path": DATADIR,
                "source_name": tmp_folder[:-1] + ".zip",
                "dest_path": f"{minio_folder}/{pack}/{subpack}/{date}/",
                "dest_name": tmp_folder[:-1] + ".zip",
            },
        ],
        ignore_airflow_env=False,
        burn_after_sending=True,
    )
    logging.info("> Cleaning")
    shutil.rmtree(DATADIR + tmp_folder)
    for file in members:
        sftp.delete_file(upload_dir + file)


def transfer_files_to_minio(ti, pack: str, subpack: str):
    files = ti.xcom_pull(key=f"{pack}_{subpack}", task_ids="get_files_list_on_sftp")
    if not files:
        logging.info("No file to process, skipping")
        return
    # we are storing files by datetime => grid => members
    dates_grids = defaultdict(lambda: defaultdict(list))
    for file, infos in files.items():
        dates_grids[infos["date"]][infos["grid"]].append(file)
    count = 0
    sftp = create_client()
    for date in dates_grids:
        for grid in dates_grids[date]:
            # checking if all members of the occurrence have arrived
            nb = len(dates_grids[date][grid])
            if nb == CONFIG[pack][subpack]["nb_membres"]:
                process_members(
                    members=dates_grids[date][grid],
                    date=date,
                    grid=grid,
                    pack=pack,
                    subpack=subpack,
                    sftp=sftp,
                )
                count += 1
                # if count > 200:
                #     print("Early stop")
                #     return count
            elif nb < CONFIG[pack][subpack]["nb_membres"]:
                logging.info(
                    f"Only {nb} members have arrived, waiting until {CONFIG[pack][subpack]['nb_membres']}"
                )
            else:
                raise ValueError(
                    f"Too many members: {nb} for {CONFIG[pack][subpack]['nb_membres']} expected"
                )
    logging.info(f"{nb} file{'s' * bool(nb)} transfered")
    return count


def build_zipfile_id_and_date(file_name: str):
    # zipped files look like "arome_ncaled0025_202501021800_03:00.zip"
    # on data.gouv we will expose only the latest occurrence of pack+subpack+grid
    # so we build an id (aka just remove the date) to compare files
    pack, subpack, date, grid = file_name.split(".")[0].split("_")
    return f"{pack}_{subpack}_{grid}", date


def get_current_resources(pack: str, subpack: str):
    current_resources = {}
    for r in requests.get(
        f"{DATAGOUV_URL}/api/1/datasets/{CONFIG[pack][subpack]['dataset_id'][AIRFLOW_ENV]}/",
        headers={"X-fields": "resources{id,url}"},
    ).json()["resources"]:
        file_id, file_date = build_zipfile_id_and_date(r["url"].split("/")[-1])
        current_resources[file_id] = {
            "date": file_date,
            "resource_id": r["id"],
        }
    return current_resources


def publish_on_datagouv(pack: str, subpack: str):
    # getting the latest available occurrence of each file on Minio
    latest_files = {}
    for obj, size in minio_meteo.get_all_files_names_and_sizes_from_parent_folder(
        folder=f"{AIRFLOW_ENV}/{minio_folder}/{pack}/{subpack}/",
    ).items():
        file_id, file_date = build_zipfile_id_and_date(obj.split("/")[-1])
        if file_id not in latest_files or file_date > latest_files[file_id]["date"]:
            latest_files[file_id] = {
                "date": file_date,
                "url": f"https://{MINIO_URL}/{bucket_pe}/{obj}",
                "title": obj.split("/")[-1],
                "size": size,
            }

    # getting the current state of the resources
    current_resources: dict = get_current_resources(pack, subpack)

    for file_id, infos in latest_files.items():
        if file_id not in current_resources:
            # uploading files that are not on data.gouv yet
            logging.info(f"🆕 Creating resource for {file_id}")
            post_remote_resource(
                dataset_id=CONFIG[pack][subpack]['dataset_id'][AIRFLOW_ENV],
                payload={
                    "url": infos["url"],
                    "filesize": infos["size"],
                    "title": infos["title"],
                    "format": "zip",
                },
            )
        elif infos["date"] > current_resources["date"]:
            # updating existing resources if fresher occurrences are available
            logging.info(f"🔃 Uptdating resource for {file_id}")
            post_remote_resource(
                dataset_id=CONFIG[pack][subpack]['dataset_id'][AIRFLOW_ENV],
                resource_id=current_resources["resource_id"],
                payload={
                    "url": infos["url"],
                    "filesize": infos["size"],
                    "title": infos["title"],
                    "format": "zip",
                },
            )


def remove_old_occurrences(pack: str, subpack: str):
    current_resources: dict = get_current_resources(pack, subpack)
    oldest_available_date = datetime.strptime(
        min([r["date"] for r in current_resources.values()]),
        "%Y%m%d%H%M",
    )
    print(oldest_available_date)
    threshold = oldest_available_date - TIME_DEPTH_TO_KEEP
    print(threshold)
    dates_on_minio = {
        path: datetime.strptime(path.split("/")[-2], "%Y%m%d%H%M")
        for path in minio_meteo.get_files_from_prefix(
            prefix=f"{minio_folder}/{pack}/{subpack}/",
            ignore_airflow_env=False,
            recursive=False,
        )
    }
    print(dates_on_minio)
    for path, date in dates_on_minio.items():
        if date < threshold:
            files_to_delete = minio_meteo.get_files_from_prefix(
                prefix=path,
                ignore_airflow_env=False,
                recursive=True,
            )
            for file in files_to_delete:
                # minio_meteo.delete_file(file)
                print("would delete", file)
