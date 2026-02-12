from datetime import datetime, timedelta
import logging
import os
import requests
import shutil
from airflow.decorators import task

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
    AIRFLOW_ENV,
    DATAGOUV_SECRET_API_KEY,
    DEMO_DATAGOUV_SECRET_API_KEY,
    MINIO_URL,
    S3_BUCKET_PNT,
    SECRET_S3_PNT_USER,
    SECRET_S3_PNT_PASSWORD,
)
from datagouvfr_data_pipelines.data_processing.meteo.previsions_numeriques_temps.config import (
    PACKAGES,
    MAX_LAST_BATCHES,
)
from datagouvfr_data_pipelines.data_processing.meteo.previsions_numeriques_temps.utils import (
    MeteoClient,
)
from datagouvfr_data_pipelines.utils.datagouv import local_client
from datagouvfr_data_pipelines.utils.s3 import S3Client
from datagouvfr_data_pipelines.utils.retry import simple_connection_retry

# if you want to roll back to dev mode
# AIRFLOW_ENV = "dev"
# DATAGOUV_URL = "https://demo.data.gouv.fr"

TMP_FOLDER = f"{AIRFLOW_DAG_TMP}meteo_pnt/"
LOG_PATH = f"{TMP_FOLDER}logs/"
ROOT_FOLDER = "datagouvfr_data_pipelines/data_processing/"
TIME_DEPTH_TO_KEEP = timedelta(hours=24)
s3_pnt = S3Client(
    bucket=S3_BUCKET_PNT,
    user=SECRET_S3_PNT_USER,
    pwd=SECRET_S3_PNT_PASSWORD,
)
s3_folder = "pnt" if AIRFLOW_ENV == "prod" else "dev"
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


@simple_connection_retry
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


@task()
def get_latest_theorical_batches(model: str, pack: str, grid: str, **context):
    batches = []
    if model == "arome":
        # arome runs every 3h
        for i in range(MAX_LAST_BATCHES * 2):
            batches.append(
                (get_last_batch_hour() - timedelta(hours=3 * i)).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                )
            )
        batch3hlater = datetime.strptime(batches[0], "%Y-%m-%dT%H:%M:%SZ") + timedelta(
            hours=3
        )
        if batch3hlater < datetime.now():
            batches.append(batch3hlater.strftime("%Y-%m-%dT%H:%M:%SZ"))
    else:
        # the others run every 6h
        for i in range(MAX_LAST_BATCHES):
            batches.append(
                (get_last_batch_hour() - timedelta(hours=6 * i)).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                )
            )
    logging.info(f"All batches: {batches}")
    tested_batches: list = get_new_batches(
        batches,
        PACKAGES[model][pack][grid]["check_availability_url"],
    )
    logging.info(f"Tested batches: {tested_batches}")
    context["ti"].xcom_push(key="batches", value=batches)
    context["ti"].xcom_push(key="tested_batches", value=tested_batches)


@task()
def clean_old_runs_in_s3(**context):
    batches = context["ti"].xcom_pull(key="batches", task_ids="get_latest_theorical_batches")
    # we get the runs' names from the folders
    runs = s3_pnt.get_folders_from_prefix(
        prefix=f"{s3_folder}/",
        ignore_airflow_env=True,
    )
    logging.info(runs)
    old_dates = set()
    keep_dates = set()
    for run_path in runs:
        # run.object_name looks like "{s3_folder}/2024-10-02T00:00:00Z/"
        run = run_path.split("/")[-2]
        if run < batches[-1]:
            old_dates.add(run)
        else:
            keep_dates.add(run)
    if len(keep_dates) > 3:
        for od in old_dates:
            s3_pnt.delete_files_from_prefix(prefix=f"{s3_folder}/{od}/")


def build_folder_path(model: str, pack: str, grid: str) -> str:
    base_path = model if pack.startswith("$") else f"{model}/{pack}"
    return f"{base_path}/{grid.replace('.', '')}"


def construct_all_possible_files(model: str, pack: str, grid: str, **kwargs):
    tested_batches = kwargs["ti"].xcom_pull(
        key="tested_batches", task_ids="get_latest_theorical_batches"
    )
    nb_files = 0
    s3_paths = []
    url_to_infos = {}
    s3_path_to_url = {}
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
                s3_path = f"{s3_folder}/{batch}/{path}/{package.name}/{filename}"
                nb_files += 1
                s3_paths.append(s3_path)
                url_to_infos[url] = {
                    "filename": filename,
                    "s3_path": s3_path,
                    "package": package.name,
                }
                s3_path_to_url[s3_path] = url

    logging.info(f"{nb_files} possible files")

    to_get = [
        s3_path for s3_path in s3_paths if not s3_pnt.does_file_exist_in_bucket(s3_path)
    ]

    logging.info(f"{len(to_get)} possible files after removing already processed files")

    if len(to_get) == 0:
        logging.info("No new data, exit")
        return False

    kwargs["ti"].xcom_push(key="url_to_infos", value=url_to_infos)
    kwargs["ti"].xcom_push(key="to_get", value=to_get)
    kwargs["ti"].xcom_push(key="s3_path_to_url", value=s3_path_to_url)
    return True


# to restore the file structure test, code is here: https://github.com/datagouv/datagouvfr_data_pipelines/blob/ec30e343ced4be8442c141a5473a349c3de331d5/data_processing/meteo/previsions_numeriques_temps/task_functions.py#L197
# def test_file_structure(filepath: str) -> bool:
#     # open and check that grib file is properly structured
#     try:
#         grib = pygrib.open(filepath)
#         for msg in grib:
#             msg.values.shape
#         return True
#     except Exception as e:
#         logging.warning(f"An error occured for {filepath}: `{e}`")
#         return False


@simple_connection_retry
def is_file_available(url: str) -> bool:
    # we'd prefer to use HEAD but the method is currently not allowed
    r = meteo_client.get(
        url,
        headers={"Range": "bytes=1024-2047"},
    )
    if r.status_code == 404:
        logging.warning(f"Not available yet, skipping. URL is: {url}")
        return False
    r.raise_for_status()
    return True


@task()
def send_files_to_s3(model: str, pack: str, grid: str, **context) -> None:
    url_to_infos = context["ti"].xcom_pull(
        key="url_to_infos", task_ids="construct_all_possible_files"
    )
    to_get = context["ti"].xcom_pull(key="to_get", task_ids="construct_all_possible_files")
    s3_path_to_url = context["ti"].xcom_pull(
        key="s3_path_to_url", task_ids="construct_all_possible_files"
    )
    path = build_folder_path(model, pack, grid)
    logging.info(f"Getting {len(to_get)} files")
    # we could also put the content of the loop within an async function and process the files simultaneously
    uploaded = []
    my_packages = set()
    for s3_path in to_get:
        url = s3_path_to_url[s3_path]
        package = url_to_infos[url]["package"]
        logging.info("_________________________")
        logging.info(url_to_infos[url]["filename"])
        # we don't download the files anymore, but we keep the folder creation for cross-run communication
        if os.path.isdir(f"{TMP_FOLDER}{path}/{package}") and package not in my_packages:
            logging.info(
                f"{url_to_infos[url]['package']} is already being processed by another run"
            )
            continue
        else:
            # this is to make sure concurrent runs don't interfere or process the same data
            os.makedirs(f"{TMP_FOLDER}{path}/{package}", exist_ok=True)
            my_packages.add(package)
        if not is_file_available(url):
            continue
        s3_pnt.send_from_url(
            url=url,
            destination_file_path=s3_path,
            session=meteo_client,
        )
        uploaded.append(s3_path)
    for p in my_packages:
        # making way for later occurrences
        os.removedirs(f"{TMP_FOLDER}{path}/{p}")
    context["ti"].xcom_push(key="uploaded", value=uploaded)


def build_file_id_and_date(file_name: str):
    # final files look like "arome__001__HP1__00H__2025-02-24T09:00:00Z.grib2"
    # on data.gouv we will expose only the latest occurrence of model+grid+package+batch
    # so we build an id (aka just remove the date) to compare files
    model, grid, package, batch, date = file_name.split(".")[0].split("__")
    return f"{model}_{grid}_{package}_{batch}", date


def get_current_resources(model: str, pack: str, grid: str):
    current_resources = {}
    for r in requests.get(
        f"{local_client.base_url}/api/1/datasets/{PACKAGES[model][pack][grid]['dataset_id'][AIRFLOW_ENV]}/",
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


@task()
def publish_on_datagouv(model: str, pack: str, grid: str, **kwargs):
    # getting the current state of the resources
    current_resources: dict = get_current_resources(model, pack, grid)

    # getting the latest available occurrence of each file on S3
    latest_files = {}
    batches_on_s3 = [
        path.split("/")[-2]
        for path in s3_pnt.get_folders_from_prefix(
            prefix=f"{s3_folder}/",
            ignore_airflow_env=True,
        )
    ]
    logging.info(f"Current batches on S3: {batches_on_s3}")

    # starting with latest timeslots
    path = build_folder_path(model, pack, grid)
    for batch in reversed(sorted(batches_on_s3)):
        if len(latest_files) == len(current_resources) and len(current_resources) > 0:
            # we have found all files
            break
        for obj, size in s3_pnt.get_all_files_names_and_sizes_from_parent_folder(
            folder=f"{s3_folder}/{batch}/{path}/",
        ).items():
            file_id, file_date = build_file_id_and_date(obj.split("/")[-1])
            if file_id not in latest_files or file_date > latest_files[file_id]["date"]:
                latest_files[file_id] = {
                    "date": file_date,
                    "url": f"https://{MINIO_URL}/{S3_BUCKET_PNT}/{obj}",
                    "title": obj.split("/")[-1],
                    "size": size,
                }
            if (
                len(latest_files) == len(current_resources)
                and len(current_resources) > 0
            ):
                # we have found all files
                break
        logging.info(
            f"{len(latest_files)}/{len(current_resources)} files found after {batch}"
        )

    for file_id, infos in latest_files.items():
        if file_id not in current_resources:
            # uploading files that are not on data.gouv yet
            logging.info(f"🆕 Creating resource for {file_id}")
            local_client.resource().create_remote(
                dataset_id=PACKAGES[model][pack][grid]["dataset_id"][AIRFLOW_ENV],
                payload={
                    "url": infos["url"],
                    "filesize": infos["size"],
                    "title": infos["title"],
                    "format": PACKAGES[model][pack]["extension"],
                    "type": "main",
                },
            )
        elif infos["date"] > current_resources[file_id]["date"]:
            # updating existing resources if fresher occurrences are available
            logging.info(f"🔃 Updating resource for {file_id}")
            local_client.resource(
                dataset_id=PACKAGES[model][pack][grid]["dataset_id"][AIRFLOW_ENV],
                id=current_resources[file_id]["resource_id"],
                fetch=False,
            ).update(
                payload={
                    "url": infos["url"],
                    "filesize": infos["size"],
                    "title": infos["title"],
                    "format": PACKAGES[model][pack]["extension"],
                    "type": "main",
                },
            )


@task()
def clean_directory(model: str, pack: str, grid: str, **kwargs):
    # in case processes crash and leave stuff behind
    path = build_folder_path(model, pack, grid)
    files_and_folders = os.listdir(f"{TMP_FOLDER}{path}")
    threshold = datetime.now() - timedelta(hours=3)
    for f in files_and_folders:
        creation_date = datetime.fromtimestamp(os.path.getctime(f"{TMP_FOLDER}{path}/{f}"))
        if creation_date < threshold and "issues" not in f:
            try:
                shutil.rmtree(f"{TMP_FOLDER}{path}/{f}")
            except NotADirectoryError:
                os.remove(f"{TMP_FOLDER}{path}/{f}")
            logging.warning(
                f"Deleted {TMP_FOLDER}{path}/{f} (created at "
                f"{creation_date.strftime('%Y-%m-%d %H:%M-%S')})"
            )
