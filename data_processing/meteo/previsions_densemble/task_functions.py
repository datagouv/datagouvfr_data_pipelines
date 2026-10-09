import json
import logging
import os
import shutil
import subprocess
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

import requests
from airflow.sdk import task
from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_HOME,
    AIRFLOW_DAG_TMP,
    AIRFLOW_ENV,
    DATAGOUV_SECRET_API_KEY,
    DEMO_DATAGOUV_SECRET_API_KEY,
)
from datagouvfr_data_pipelines.utils.datagouv import local_client
from datagouvfr_data_pipelines.utils.filesystem import File
from datagouvfr_data_pipelines.utils.s3 import S3Client, S3ClientKwargs
from datagouvfr_data_pipelines.utils.sftp import SFTPClient

TMP_FOLDER = f"{AIRFLOW_DAG_TMP}meteo_pe/"
ROOT_FOLDER = "datagouvfr_data_pipelines/data_processing/"
TIME_DEPTH_TO_KEEP = timedelta(days=15)
# When True, deletion (S3 and SFTP) is only logged, never actually performed.
DRY_RUN = True
bucket_pe = "meteofrance-pe"
s3_folder = "data"
upload_dir = "/uploads/"  # this is where MF pushes the files
s3_client_kwargs: S3ClientKwargs = {
    "bucket": bucket_pe,
    "conn_name": "S3_OVH_RBX",
}

with open(
    f"{AIRFLOW_DAG_HOME}{ROOT_FOLDER}meteo/previsions_densemble/config.json"
) as fp:
    CONFIG = json.load(fp)


def create_client() -> SFTPClient:
    return SFTPClient(
        conn_name="SSH_TRANSFER_DATA_GOUV_FR",
        # you may have to edit the dev value depending on your local conf
        key_type="RSA" if AIRFLOW_ENV == "dev" else "Ed25519",
    )


def get_file_infos(file_name: str):
    # files look like this: arome_pecaledonie_202409230600_mb0_ncaled0025_00:00.grib
    pack, _, date, membre, grid, echeance = file_name.split(".")[0].split("_")
    # the labels are not exactly MF-approved, but that doesn't really matter, we handle files, not data
    return {
        "pack": pack,
        "grid": grid,
        "date": date,
        "membre": membre,
        "echeance": echeance,
    }


@task()
def get_files_list_on_sftp(pack: str, grid: str, **context):
    sftp = create_client()
    files = sftp.list_files_in_directory(upload_dir)
    logging.info(f"{len(files)} files in {upload_dir}")
    # we recreate the structure of the config file: packs => grids => files
    to_process = {}
    nb = 0
    for f in files:
        if not f.endswith(".grib"):
            # most likely files that are not done uploading
            logging.warning(f"> ignoring {f}")
        else:
            infos = get_file_infos(f)
            if (
                infos["pack"] not in CONFIG
                or infos["grid"] not in CONFIG[infos["pack"]]
            ):
                raise ValueError(
                    f"Got an unexpected pack: {infos['pack']}_{infos['grid']}"
                )
            if infos["pack"] == pack and infos["grid"] == grid:
                to_process.update({f: infos})
                nb += 1
    logging.info(f"{nb} files to process")
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    if to_process:
        with open(TMP_FOLDER + f"{pack}_{grid}_{timestamp}.json", "w") as f:
            json.dump(to_process, f)
    context["ti"].xcom_push(key="timestamp", value=timestamp)


def process_members(
    members: list[str],
    date: str,
    echeance: str,
    pack: str,
    grid: str,
    s3_client: S3Client,
    sftp,
) -> int:
    tmp_folder = f"{pack}_{grid}_{date}_{echeance}/"
    if os.path.isdir(TMP_FOLDER + tmp_folder):
        # we allow concurrent DAG runs for these DAGs, this is how they "communicate", aka know if another run is already processing a batch
        logging.info(f"{tmp_folder} is already being processed by another run")
        return 0
    logging.info(f"Processing {tmp_folder}")
    os.mkdir(TMP_FOLDER + tmp_folder)
    for file in members:
        try:
            sftp.download_file(
                remote_file_path=upload_dir + file,
                local_file_path=TMP_FOLDER + tmp_folder + file,
            )
        except FileNotFoundError:
            logging.warning("Seems like it has already been processed")
            shutil.rmtree(TMP_FOLDER + tmp_folder)
            return 0
    # concatenating all members of the occurrence into a grib, MF said that was the thing to do, they know it better
    logging.info("> Concatenating")
    subprocess.run(
        f"cat {TMP_FOLDER + tmp_folder}* > {TMP_FOLDER + tmp_folder[:-1]}.grib",
        shell=True,
        stderr=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
    )
    s3_client.send_file(
        File(
            source_path=TMP_FOLDER,
            source_name=tmp_folder[:-1] + ".grib",
            dest_path=f"{s3_folder}/{pack}/{grid}/{date}/",
            dest_name=tmp_folder[:-1] + ".grib",
        ),
        ignore_airflow_env=False,
        burn_after_sending=True,
        is_public=True,
    )
    logging.info("> Cleaning")
    shutil.rmtree(TMP_FOLDER + tmp_folder)
    if AIRFLOW_ENV == "prod":
        # only cleaning the SFTP in production
        for file in members:
            sftp.delete_file(upload_dir + file)
    return 1


def transfer_files_to_s3(pack: str, grid: str, **context):
    timestamp = context["ti"].xcom_pull(
        key="timestamp", task_ids="get_files_list_on_sftp"
    )
    if not os.path.isfile(TMP_FOLDER + f"{pack}_{grid}_{timestamp}.json"):
        logging.info("No file to process, skipping")
        return
    with open(TMP_FOLDER + f"{pack}_{grid}_{timestamp}.json", "r") as f:
        files = json.load(f)
    # we are storing files by datetime => echeance => members
    dates_echeances: dict = defaultdict(lambda: defaultdict(list))
    for file, infos in files.items():
        dates_echeances[infos["date"]][infos["echeance"]].append(file)
    count = 0
    sftp = create_client()
    s3_client = S3Client(**s3_client_kwargs)
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
                    s3_client=s3_client,
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
    os.remove(TMP_FOLDER + f"{pack}_{grid}_{timestamp}.json")
    return count


def build_file_id_and_date(file_name: str) -> tuple[str, str]:
    # final files look like "arome_ncaled0025_202501021800_03:00.grib"
    # on data.gouv we will expose only the latest occurrence of pack+grid+echeance
    # so we build an id (aka just remove the date) to compare files
    pack, grid, date, echeance = file_name.split(".")[0].split("_")
    return f"{pack}_{grid}_{echeance}", date


def get_current_resources(pack: str, grid: str) -> dict:
    current_resources = {}
    for r in requests.get(
        f"{local_client.base_url}/api/1/datasets/{CONFIG[pack][grid]['dataset_id'][AIRFLOW_ENV]}/",
        headers={
            "X-fields": "resources{id,url,type}",
            "X-API-KEY": (
                DATAGOUV_SECRET_API_KEY
                if AIRFLOW_ENV == "prod"
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


def fix_title(file_name: str) -> str:
    # names are not perfectly accurate, but it's cleaner to modify only the title
    # as the whole file structure is automatically made from original names
    return file_name.replace("arome", "pearome").replace("arpege", "pearp")


@task()
def publish_on_datagouv(pack: str, grid: str):
    # getting the latest available occurrence of each file on S3
    latest_files: dict = {}
    s3_client = S3Client(**s3_client_kwargs)
    for obj, size in s3_client.get_all_files_names_and_sizes_from_parent_folder(
        folder=f"{AIRFLOW_ENV}/{s3_folder}/{pack}/{grid}/",
    ).items():
        try:
            file_id, file_date = build_file_id_and_date(obj.split("/")[-1])
            if file_id not in latest_files or file_date > latest_files[file_id]["date"]:
                latest_files[file_id] = {
                    "date": file_date,
                    "url": s3_client.get_file_url(obj),
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
                dataset_id=CONFIG[pack][grid]["dataset_id"][AIRFLOW_ENV],
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
                dataset_id=CONFIG[pack][grid]["dataset_id"][AIRFLOW_ENV],
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


def _compute_retention_threshold(current_resources: dict) -> datetime:
    """Retention threshold: newest run of this grid minus the retention window.

    A *run* is the whole set of échéances (00:00..48:00) that Météo-France
    computes together for a given (pack, grid, date). They share the same date,
    are stored in a single S3 date folder and exposed on data.gouv as one
    resource per échéance pointing to that date, so a run is kept or deleted as
    a whole. Anchoring the threshold to the *newest* run (``max``) prunes runs
    older than TIME_DEPTH_TO_KEEP; the previous code anchored to the *oldest*
    run (``min``), which sat at the floor of the available dates and therefore
    never pruned anything.
    """
    # The newest run of this pack+grid: resources are keyed by échéance and all
    # point to their own run date, so we take the most recent of them.
    newest_run_date = datetime.strptime(
        max(r["date"] for r in current_resources.values()),
        "%Y%m%d%H%M",
    )
    logging.info(f"Newest run in dataset: {newest_run_date}")
    threshold = newest_run_date - TIME_DEPTH_TO_KEEP
    logging.info(f"Will delete everything before {threshold}")
    return threshold


@task()
def remove_old_occurrences(pack: str, grid: str):
    # removing too old files from S3 and cleaning SFTP if remainders
    current_resources: dict = get_current_resources(pack, grid)
    threshold = _compute_retention_threshold(current_resources)
    s3_meteo = S3Client(**s3_client_kwargs)
    run_dates_on_s3 = {
        path: datetime.strptime(path.split("/")[-2], "%Y%m%d%H%M")
        for path in s3_meteo.get_folders_from_prefix(
            prefix=f"{s3_folder}/{pack}/{grid}/",
            ignore_airflow_env=False,
        )
    }
    logging.info(f"Current run dates on S3: {run_dates_on_s3}")
    total_s3_folders = len(run_dates_on_s3)
    deleted_s3_folders = 0
    delete_errors_s3 = 0
    freed_s3 = 0
    first_removed_s3 = None
    last_removed_s3 = None
    for path, run_date in run_dates_on_s3.items():
        if run_date < threshold:
            # the whole run (all échéances) is obsolete -> delete the folder
            files_to_delete = list(
                s3_meteo.get_files_from_prefix(
                    prefix=path,
                    ignore_airflow_env=True,
                    as_objects=True,
                )
            )
            freed_s3 += sum(obj.size for obj in files_to_delete)
            if not DRY_RUN:
                for obj in files_to_delete:
                    try:
                        s3_meteo.delete_file(obj.key)
                    except Exception as e:
                        delete_errors_s3 += 1
                        logging.error("Error while deleting", obj.key, ":", e)
            deleted_s3_folders += 1
            first_removed_s3 = first_removed_s3 or path
            last_removed_s3 = path
    if deleted_s3_folders:
        start = "DRY RUN: would delete" if DRY_RUN else "deleted"
        logging.info(
            f"{start} {deleted_s3_folders}/{total_s3_folders} "
            f"S3 run folder(s) ({_human_size(freed_s3)}) "
            f"(first: {first_removed_s3}, last: {last_removed_s3})"
        )
    if delete_errors_s3:
        logging.warning(
            f"Failed to delete {delete_errors_s3} S3 file(s); "
            f"the deleted folder count above may be optimistic"
        )
    # removing old files on SFTP (to prevent accumulation)
    total_sftp = 0
    deleted_old = 0
    delete_errors_sftp = 0
    freed_sftp = 0
    first_removed_sftp = None
    last_removed_sftp = None
    size_errors_sftp = 0
    sftp = create_client()
    for file in sftp.list_files_in_directory(upload_dir):
        if not file.endswith(".grib"):
            # most likely files that are not done uploading
            logging.warning(f"> ignoring {file}")
            continue
        total_sftp += 1
        # computing the run datetime of the file from its name, to compare full
        # datetimes instead of the (date-only) threshold as a raw string
        run_date = datetime.strptime(get_file_infos(file)["date"], "%Y%m%d%H%M")
        if run_date < threshold:
            deleted_old += 1
            try:
                freed_sftp += sftp.get_file_stats(upload_dir + file).st_size
            except Exception:
                size_errors_sftp += 1
            if not DRY_RUN:
                try:
                    sftp.delete_file(upload_dir + file)
                except Exception as e:
                    delete_errors_sftp += 1
                    logging.error("Error while deleting", file, ":", e)
            first_removed_sftp = first_removed_sftp or file
            last_removed_sftp = file
    if deleted_old:
        start = "DRY RUN: would delete" if DRY_RUN else "deleted"
        logging.info(
            f"{start} {deleted_old}/{total_sftp} "
            f"SFTP file(s) ({_human_size(freed_sftp)}) "
            f"(first: {first_removed_sftp}, last: {last_removed_sftp})"
        )
    if delete_errors_sftp:
        logging.warning(
            f"Failed to delete {delete_errors_sftp} SFTP file(s); "
            f"the deleted count above may be optimistic"
        )
    if size_errors_sftp:
        logging.warning(
            f"Could not compute the size of {size_errors_sftp} SFTP file(s); "
            f"the freed size above may be understated"
        )


def _human_size(num_bytes: int) -> str:
    """Format a byte count in a concise, human-readable way."""
    size = float(num_bytes)
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if size < 1024 or unit == "TiB":
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size} B"


@task()
def handle_cyclonic_alert(pack: str, grid: str):
    # during a cyclonic alert, some packs have a higher number of members (79 instead of 49)
    # so when a cyclonic alert is stopped, we have to remove the additional resources from the dataset
    current_resources: dict = get_current_resources(pack, grid)
    if len(current_resources) not in [49, 79]:
        raise ValueError(
            f"{pack}_{grid} has an unexpected number of resources: {len(current_resources)}"
        )
    if len(current_resources) == 49:
        logging.info("Nothing to do here")
        return
    s3_meteo = S3Client(**s3_client_kwargs)
    latest_date = max(
        path.split("/")[-2]
        for path in s3_meteo.get_folders_from_prefix(
            prefix=f"{s3_folder}/{pack}/{grid}/",
            ignore_airflow_env=False,
        )
    )
    logging.info(f"Latest date {latest_date}")
    nb_files_latest_date = len(
        list(
            s3_meteo.get_files_from_prefix(
                prefix=f"{s3_folder}/{pack}/{grid}/{latest_date}/",
                ignore_airflow_env=False,
            )
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
                    dataset_id=CONFIG[pack][grid]["dataset_id"][AIRFLOW_ENV],
                    id=infos["resource_id"],
                ).delete()


@task()
def clean_directory():
    # in case processes crash and leave stuff behind, so that upcoming DAG runs don't consider they're being processed (see above about how they "communicate")
    tmp_folder = Path(TMP_FOLDER)
    # TMP_FOLDER may not exist yet (clean_directory runs before create_working_dir in the DAG)
    if not tmp_folder.is_dir():
        return
    threshold = datetime.now() - timedelta(hours=6)
    for path in tmp_folder.iterdir():
        creation_date = datetime.fromtimestamp(path.stat().st_ctime)
        if creation_date < threshold:
            if path.is_dir():
                shutil.rmtree(path)
            else:
                path.unlink()
            logging.warning(
                f"Deleted {path.name} (created at {creation_date.strftime('%Y-%m-%d %H:%M-%S')})"
            )
