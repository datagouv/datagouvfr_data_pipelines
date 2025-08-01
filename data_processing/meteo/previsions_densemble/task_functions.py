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
    DATAGOUV_SECRET_API_KEY,
    DEMO_DATAGOUV_SECRET_API_KEY,
    MINIO_URL,
    SECRET_MINIO_METEO_PE_USER,
    SECRET_MINIO_METEO_PE_PASSWORD,
)
from datagouvfr_data_pipelines.utils.datagouv import local_client
from datagouvfr_data_pipelines.utils.filesystem import File
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
    return SFTPClient(
        conn_name="SSH_TRANSFER_INFRA_DATA_GOUV_FR",
        user="meteofrance",
        # you may have to edit the dev value depending on your local conf
        key_type="RSA" if AIRFLOW_ENV == "dev" else "Ed25519",
    )


def get_file_infos(file_name: str):
    # files look like this: arome_pecaledonie_202409230600_mb0_ncaled0025_00:00.grib
    pack, _, date, membre, grid, echeance = file_name.split(".")[0].split("_")
    return {
        "pack": pack,
        "grid": grid,
        "date": date,
        "membre": membre,
        "echeance": echeance,
    }


def get_files_list_on_sftp(ti, pack: str, grid: str):
    sftp = create_client()
    files = sftp.list_files_in_directory(upload_dir)
    logging.info(f"{len(files)} files in {upload_dir}")
    # we recreate the structure of the config file: packs => grids => files
    to_process = {}
    nb = 0
    for f in files:
        if not f.endswith(".grib"):
            logging.warning(f"> ignoring {f}")
        else:
            infos = get_file_infos(f)
            if (
                infos["pack"] not in CONFIG
                or infos["grid"] not in CONFIG[infos["pack"]]
            ):
                raise ValueError(f"Got an unexpected pack: {infos['pack']}_{infos['grid']}")
            if infos["pack"] == pack and infos["grid"] == grid:
                to_process.update({f: infos})
                nb += 1
    logging.info(f"{nb} files to process")
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    if to_process:
        with open(DATADIR + f"{pack}_{grid}_{timestamp}.json", "w") as f:
            json.dump(to_process, f)
    ti.xcom_push(key="timestamp", value=timestamp)


def process_members(members: list[str], date: str, echeance: str, pack: str, grid: str, sftp):
    tmp_folder = f"{pack}_{grid}_{date}_{echeance}/"
    if os.path.isdir(DATADIR + tmp_folder):
        logging.info(f"{tmp_folder} is already being processed by another run")
        return 0
    logging.info(f"Processing {tmp_folder}")
    os.mkdir(DATADIR + tmp_folder)
    for file in members:
        try:
            sftp.download_file(
                remote_file_path=upload_dir + file,
                local_file_path=DATADIR + tmp_folder + file,
            )
        except FileNotFoundError:
            logging.warning("Seems like it has already been processed")
            shutil.rmtree(DATADIR + tmp_folder)
            return 0
    # concatenating all members of the occurrence into a grib
    logging.info("> Concatenating")
    subprocess.run(
        f"cat {DATADIR + tmp_folder}* > {DATADIR + tmp_folder[:-1]}.grib",
        shell=True,
        stderr=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
    )
    minio_meteo.send_files(
        [
            File(
                source_path=DATADIR,
                source_name=tmp_folder[:-1] + ".grib",
                dest_path=f"{minio_folder}/{pack}/{grid}/{date}/",
                dest_name=tmp_folder[:-1] + ".grib",
            ),
        ],
        ignore_airflow_env=False,
        burn_after_sending=True,
    )
    logging.info("> Cleaning")
    shutil.rmtree(DATADIR + tmp_folder)
    if AIRFLOW_ENV == "prod":
        for file in members:
            sftp.delete_file(upload_dir + file)
    return 1


def transfer_files_to_minio(ti, pack: str, grid: str):
    timestamp = ti.xcom_pull(key="timestamp", task_ids="get_files_list_on_sftp")
    if not os.path.isfile(DATADIR + f"{pack}_{grid}_{timestamp}.json"):
        logging.info("No file to process, skipping")
        return
    with open(DATADIR + f"{pack}_{grid}_{timestamp}.json", "r") as f:
        files = json.load(f)
    # we are storing files by datetime => echeance => members
    dates_echeances = defaultdict(lambda: defaultdict(list))
    for file, infos in files.items():
        dates_echeances[infos["date"]][infos["echeance"]].append(file)
    count = 0
    sftp = create_client()
    for date in dates_echeances:
        for echeance in dates_echeances[date]:
            # checking if all members of the occurrence have arrived
            nb = len(dates_echeances[date][echeance])
            if nb == CONFIG[pack][grid]["nb_membres"]:
                count += process_members(
                    members=dates_echeances[date][echeance],
                    date=date,
                    echeance=echeance,
                    pack=pack,
                    grid=grid,
                    sftp=sftp,
                )
            elif nb < CONFIG[pack][grid]["nb_membres"]:
                logging.info(
                    f"{pack}_{grid}_{date}_{echeance}: only {nb} members have arrived, "
                    f"waiting until {CONFIG[pack][grid]['nb_membres']}"
                )
            else:
                # this should not happen, so raising feels fair
                raise ValueError(
                    f"Too many members: {nb} for {CONFIG[pack][grid]['nb_membres']} expected"
                )
    logging.info(f"{count} file{'s' * (count > 1)} transfered")
    os.remove(DATADIR + f"{pack}_{grid}_{timestamp}.json")
    return count


def build_file_id_and_date(file_name: str):
    # final files look like "arome_ncaled0025_202501021800_03:00.grib"
    # on data.gouv we will expose only the latest occurrence of pack+grid+echeance
    # so we build an id (aka just remove the date) to compare files
    pack, grid, date, echeance = file_name.split(".")[0].split("_")
    return f"{pack}_{grid}_{echeance}", date


def get_current_resources(pack: str, grid: str):
    current_resources = {}
    for r in requests.get(
        f"{local_client.base_url}/api/1/datasets/{CONFIG[pack][grid]['dataset_id'][AIRFLOW_ENV]}/",
        headers={
            "X-fields": "resources{id,url,type}",
            "X-API-KEY": (
                DATAGOUV_SECRET_API_KEY if AIRFLOW_ENV == "prod"
                else DEMO_DATAGOUV_SECRET_API_KEY
            ),
        },
    ).json()["resources"]:
        if r["type"] != "main":
            continue
        file_id, file_date = build_file_id_and_date(r["url"].split("/")[-1])
        current_resources[file_id] = {
            "date": file_date,
            "resource_id": r["id"],
        }
    return current_resources


def fix_title(file_name: str):
    # names are not perfectly accurate, but it's cleaner to modify only the title
    # as the whole file structure is automatically made from original names
    return file_name.replace("arome", "pearome").replace("arpege", "pearp")


def publish_on_datagouv(pack: str, grid: str):
    # getting the latest available occurrence of each file on Minio
    latest_files = {}
    for obj, size in minio_meteo.get_all_files_names_and_sizes_from_parent_folder(
        folder=f"{AIRFLOW_ENV}/{minio_folder}/{pack}/{grid}/",
    ).items():
        try:
            file_id, file_date = build_file_id_and_date(obj.split("/")[-1])
            if file_id not in latest_files or file_date > latest_files[file_id]["date"]:
                latest_files[file_id] = {
                    "date": file_date,
                    "url": f"https://{MINIO_URL}/{bucket_pe}/{obj}",
                    "title": fix_title(obj.split("/")[-1]),
                    "size": size,
                }
        except Exception:
            # skipping cases of relicates folders, not clean though
            logging.error(f"Issue with {obj}, skipping")

    # getting the current state of the resources
    current_resources: dict = get_current_resources(pack, grid)

    for file_id, infos in latest_files.items():
        if file_id not in current_resources:
            # uploading files that are not on data.gouv yet
            logging.info(f"🆕 Creating resource for {file_id}")
            local_client.resource().create_remote(
                dataset_id=CONFIG[pack][grid]['dataset_id'][AIRFLOW_ENV],
                payload={
                    "url": infos["url"],
                    "filesize": infos["size"],
                    "title": infos["title"],
                    "format": "grib",
                    "type": "main",
                },
            )
        elif infos["date"] > current_resources[file_id]["date"]:
            # updating existing resources if fresher occurrences are available
            logging.info(f"🔃 Updating resource for {file_id}")
            local_client.resource(
                dataset_id=CONFIG[pack][grid]['dataset_id'][AIRFLOW_ENV],
                id=current_resources[file_id]["resource_id"],
                fetch=False,
            ).update(
                payload={
                    "url": infos["url"],
                    "filesize": infos["size"],
                    "title": infos["title"],
                    "format": "grib",
                    "type": "main",
                },
            )


def remove_old_occurrences(pack: str, grid: str):
    current_resources: dict = get_current_resources(pack, grid)
    oldest_available_date = datetime.strptime(
        min([r["date"] for r in current_resources.values()]),
        "%Y%m%d%H%M",
    )
    logging.info(f"Oldest date in dataset: {oldest_available_date}")
    threshold = oldest_available_date - TIME_DEPTH_TO_KEEP
    logging.info(f"Will delete everything before {threshold}")
    dates_on_minio = {
        path: datetime.strptime(path.split("/")[-2], "%Y%m%d%H%M")
        for path in minio_meteo.get_files_from_prefix(
            prefix=f"{minio_folder}/{pack}/{grid}/",
            ignore_airflow_env=False,
            recursive=False,
        )
    }
    logging.info(f"Current dates on Minio: {dates_on_minio}")
    for path, date in dates_on_minio.items():
        if date < threshold:
            files_to_delete = minio_meteo.get_files_from_prefix(
                prefix=path,
                ignore_airflow_env=True,
                recursive=True,
            )
            logging.info(f"Will delete {len(files_to_delete)} files from {path}")
            for file in files_to_delete:
                minio_meteo.delete_file(file)


def handle_cyclonic_alert(pack: str, grid: str):
    # during a cyclonic alert, some packs have a higher number of members (79 instead of 49)
    # so when a cyclonic alert is stopped, we have to remove the additional resources from the dataset
    current_resources: dict = get_current_resources(pack, grid)
    if len(current_resources) not in [49, 79]:
        raise ValueError(f"{pack}_{grid} has an unexpected number of resources: {len(current_resources)}")
    if len(current_resources) == 49:
        logging.info("Nothing to do here")
        return
    latest_date = max(
        path.split("/")[-2] for path in minio_meteo.get_files_from_prefix(
            prefix=f"{minio_folder}/{pack}/{grid}/",
            ignore_airflow_env=False,
            recursive=False,
        )
    )
    logging.info(f"Latest date {latest_date}")
    nb_files_latest_date = len(
        minio_meteo.get_files_from_prefix(
            prefix=f"{minio_folder}/{pack}/{grid}/{latest_date}/",
            ignore_airflow_env=False,
            recursive=False,
        )
    )
    logging.info(f"Nb files latest date {nb_files_latest_date}")
    if nb_files_latest_date == 49:
        logging.info("Deleting cyclonic alert additional resources")
        for file_id, infos in current_resources.items():
            # by construction
            _, _, echeance = file_id.split("_")
            echeance = int(echeance.replace(":", ""))
            if echeance > 4800:
                logging.info(f"Deleting {file_id}")
                local_client.resource(
                    dataset_id=CONFIG[pack][grid]['dataset_id'][AIRFLOW_ENV],
                    id=infos["resource_id"],
                ).delete()


def clean_directory():
    # in case processes crash and leave stuff behind
    files_and_folders = os.listdir(DATADIR)
    threshold = datetime.now() - timedelta(hours=6)
    for f in files_and_folders:
        creation_date = datetime.fromtimestamp(os.path.getctime(DATADIR + f))
        if creation_date < threshold:
            try:
                shutil.rmtree(DATADIR + f)
            except NotADirectoryError:
                os.remove(DATADIR + f)
            logging.warning(f"Deleted {f} (created at {creation_date.strftime('%Y-%m-%d %H:%M-%S')})")
