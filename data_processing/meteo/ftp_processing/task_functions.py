import ftplib
import logging
import os
import json
from datetime import datetime, timedelta, timezone
import re
import requests
from dateutil import parser
from collections import defaultdict

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_HOME,
    AIRFLOW_DAG_TMP,
    AIRFLOW_ENV,
    MINIO_URL,
)
from datagouvfr_data_pipelines.utils.datagouv import local_client
from datagouvfr_data_pipelines.utils.filesystem import File
from datagouvfr_data_pipelines.utils.mattermost import send_message
from datagouvfr_data_pipelines.utils.minio import MinIOClient

ROOT_FOLDER = "datagouvfr_data_pipelines/data_processing/"
DATADIR = f"{AIRFLOW_DAG_TMP}meteo/data"
minio_folder = "data/synchro_ftp/"
bucket = "meteofrance"
with open(f"{AIRFLOW_DAG_HOME}{ROOT_FOLDER}meteo/config/dgv.json") as fp:
    config = json.load(fp)
hooks = ["latest", "previous"]
minio_meteo = MinIOClient(bucket=bucket)


def clean_hooks(string: str, hooks: list = hooks) -> str:
    _ = string
    for h in hooks:
        _ = _.replace(f"{h}-", "")
    return _


def previous_date_parse(date_string: str) -> datetime:
    # this returns the last occurrence of the date, not a future one
    tmp = parser.parse(date_string)
    if tmp > datetime.today():
        tmp = tmp.replace(year=tmp.year - 1)
    return tmp


def get_path(full_file_path: str) -> str:
    # get (BASE/DECAD, BASE/DECAD) for BASE/DECAD/DECADQ_01_1852-1949.csv.gz
    # and (BASE/INFOS_POSTES/01, BASE/INFOS_POSTES) for BASE/INFOS_POSTES/01/01010001_01_ANGLEFORT.pdf
    # NB: global_path is the key used to access config parameters
    #     true_path is the actual path to the file
    true_path = "/".join(full_file_path.split("/")[:-1])
    global_path = true_path
    while global_path and global_path not in config:
        global_path = "/".join(global_path.split("/")[:-1])
    if not global_path:
        # files that are misplaced or in unhandled folders
        global_path = true_path
    return true_path, global_path


def get_resource_lists() -> dict:
    resources_lists = {
        path: {
            r["url"]: {
                "id": r["id"],
                "last_modified": datetime.fromisoformat(
                    r["internal"]["last_modified_internal"]
                ),
            }
            for r in resp.json()["resources"]
        }
        for path in config.keys()
        if (
            resp := requests.get(
                f"{local_client.base_url}/api/1/datasets/{config[path]['dataset_id'][AIRFLOW_ENV]}/",
                headers={
                    "X-fields": "resources{id,url,internal{last_modified_internal}}"
                },
            )
        ).ok
    }
    return resources_lists


def build_file_id(file: str, path: str) -> str:
    # we are creating ids for files based on their name so that we can
    # match files that have been updated with a name change
    # for instance:
    # QUOT_SIM2_latest-2020-202310.csv.gz becomes QUOT_SIM2_latest-2020-202311.csv.gz
    # when Nov2023 is added, but it actually *is* just a content update of the same file
    # for this case, id of both files would be QUOT_SIM2_latest
    file_id = file
    if config.get(path, {}).get("source_pattern"):
        params = re.match(config[path]["source_pattern"], file)
        params = params.groupdict() if params else {}
        if "PERIOD" in params and any([h in params["PERIOD"] for h in hooks]):
            # this will have to change if more hooks are added
            params["PERIOD"] = "latest" if "latest" in params["PERIOD"] else "previous"
        if params:
            file_id = config[path]["name_template"].format(**params)
    return file_id


def build_resource(
    file_path: str,
    minio_folder: str,
) -> tuple[str, str, str, str, str, bool]:
    # file_path has to be the full file path as structured on the FTP
    # for files on minio, removing the minio folder upstream is required
    _, global_path = get_path(file_path)
    file_with_ext = file_path.split("/")[-1]
    url = f"https://{MINIO_URL}/{bucket}/{minio_folder + file_path}"
    # differenciation ressource principale VS documentation
    is_doc = False
    description = ""
    # two known errors:
    # 1. files that don't match either pattern
    # 2. files that are contained in subfolders => creating new paths in config
    if global_path not in config:
        resource_name = file_with_ext
        description = ""
    elif config[global_path]["doc_pattern"] and re.match(
        config[global_path]["doc_pattern"], file_with_ext
    ):
        resource_name = file_with_ext.split(".")[0]
        is_doc = True
    else:
        # peut-être mettre un 'if params else' ici pour publier les ressources
        # qui posent problème en doc quoiqu'il ?
        try:
            params = re.match(
                config[global_path]["source_pattern"], file_with_ext
            ).groupdict()
            for k in params:
                # removing hook names for data.gouv display
                params[k] = clean_hooks(params[k])
            resource_name = config[global_path]["name_template"].format(**params)
            description = config[global_path]["description_template"].format(**params)
        except AttributeError:
            logging.warning("File is not matching pattern")
            resource_name = file_with_ext
            description = ""
    return file_with_ext, global_path, resource_name, description, url, is_doc


def list_ftp_files_recursive(ftp, path: str = "", base_path: str = "") -> list:
    files = []
    try:
        ftp.cwd(path)
        current_path = f"{base_path}/{path}" if base_path else path
        ftp.retrlines(
            "LIST",
            lambda x: files.append(
                (
                    current_path.split("//")[-1],
                    x.split()[-1],
                    int(x.split()[4]),
                    x.split()[5:8],
                )
            ),
        )
        for item in files:
            # assuming no folder contains a dot (we'll make sure it doesn't happen)
            if "." not in item[1]:
                files += list_ftp_files_recursive(
                    ftp, f"{path}/{item[1]}", current_path
                )
    except ftplib.error_perm:
        pass
    return files


def get_current_files_on_ftp(ti, ftp) -> None:
    raw_ftp_files = list_ftp_files_recursive(ftp)
    ftp_files = {}
    # pour distinguer les nouveaux fichiers (nouvelle décennie révolue, période stock...)
    # des fichiers qui changent de nom lors de mises à jour (QUOT_SIM2_2020-202309.csv.gz
    # qui devient QUOT_SIM2_2020-202310.csv.gz), on utilise des balises afin de cibler ces fichiers
    # et de remplacer l'ancien par le nouveau au lieu d'ajouter le nouveau fichier
    # et de laisser les deux coexister
    for path, file, size, date_list in raw_ftp_files:
        if "." in file:
            path = path.lstrip("/")
            # we keep path in the key just in case two files would have the same name/id
            # but in different folders
            _, global_path = get_path(path + "/" + file)
            ftp_files[path + "/" + build_file_id(file, global_path)] = {
                "file_path": path + "/" + file,
                "size": size,
                "modif_date": previous_date_parse(" ".join(date_list)),
            }
    for f in ftp_files:
        logging.info(f"{f}: {ftp_files[f]}")
    ti.xcom_push(key="ftp_files", value=ftp_files)


def get_current_files_on_minio(ti) -> None:
    minio_files = minio_meteo.get_all_files_names_and_sizes_from_parent_folder(
        folder=minio_folder
    )
    # getting the start of each time period to update datasets temporal_coverage
    period_starts = {}
    for file in minio_files:
        path = get_path(file.replace(minio_folder, ""))
        file_with_ext = file.split("/")[-1]
        if config.get(path, {}).get("source_pattern"):
            params = re.match(config[path]["source_pattern"], file_with_ext)
            params = params.groupdict() if params else {}
            if params and "PERIOD" in params:
                period_starts[path] = min(
                    int(clean_hooks(params["PERIOD"]).split("-")[0]),
                    period_starts.get(path, datetime.today().year),
                )
    logging.info(period_starts)

    # de même ici, on utilise les balises pour cibler les fichiers du Minio
    # qui devront être remplacés
    final_minio_files = {}
    for file_path in minio_files:
        clean_file_path = file_path.replace(minio_folder, "")
        file_name = clean_file_path.split("/")[-1]
        true_path, global_path = get_path(clean_file_path)
        final_minio_files[true_path + "/" + build_file_id(file_name, global_path)] = {
            "file_path": clean_file_path,
            "size": minio_files[file_path],
        }
    for f in final_minio_files:
        logging.info(f"{f}: {final_minio_files[f]}")
    ti.xcom_push(key="minio_files", value=final_minio_files)
    ti.xcom_push(key="period_starts", value=period_starts)


def has_file_been_updated_already(ftp_file: dict, resources_lists: dict) -> bool:
    file_url = f"https://{MINIO_URL}/{bucket}/{minio_folder}{ftp_file['file_path']}"
    _, global_path = get_path(ftp_file["file_path"])
    last_modified_datagouv = (
        resources_lists.get(global_path, {}).get(file_url, {}).get("last_modified")
    )
    if not last_modified_datagouv:
        logging.info(f"This file is not on datagouv yet: {ftp_file['file_path']}")
        return False
    has_been_modified = last_modified_datagouv > ftp_file["modif_date"].replace(
        tzinfo=timezone.utc
    )
    if has_been_modified:
        logging.info(
            f"> {ftp_file['file_path']} has already been modified on data.gouv"
        )
    return has_been_modified


def get_and_upload_file_diff_ftp_minio(ti, ftp) -> None:
    minio_files = ti.xcom_pull(key="minio_files", task_ids="get_current_files_on_minio")
    ftp_files = ti.xcom_pull(key="ftp_files", task_ids="get_current_files_on_ftp")
    # much debated part of the code: how to best get which files to consider here
    # first it was only done with the files' names but what if a file is updated but not renamed?
    # then we thought about checking the size and comparing with Minio
    # but size can vary for an identical file depending on where/how it is stored
    # we also thought about a checksum, but hard to compute on the FTP and downloading
    # all files to compute checksums and compare is inefficient
    # our best try: check the modification date on the FTP and take the file if its modification
    # date on datagouv is less recent
    resources_lists = get_resource_lists()
    diff_files = [
        f
        for f in ftp_files
        if f not in minio_files
        or not has_file_been_updated_already(ftp_files[f], resources_lists)
    ]
    logging.info(
        f"Synchronizing {len(diff_files)} file{'s' if len(diff_files) > 1 else ''}"
    )
    logging.info(diff_files)
    if len(diff_files) == 0:
        raise ValueError("No new file today, is that normal?")

    new_files = []
    files_to_update_same_name = []
    files_to_update_new_name = {}
    updated_datasets = set()
    # doing it one file at a time in order not to overload production server
    for file_to_transfer in diff_files:
        logging.info("___________________________")
        # if the file_id is in minio_files, it means that the current file is not
        # a true new file, but an updated file. We delete the old file at the end of
        # the process (to prevent downtimes) only if the file's name has changed
        # (otherwise the new one will just overwrite the old one) and upload the new
        # one, which will replace its previous version (we change the resource URL).
        logging.info(f"Transfering {ftp_files[file_to_transfer]['file_path']}...")
        if file_to_transfer in minio_files:
            if (
                minio_files[file_to_transfer]["file_path"]
                != ftp_files[file_to_transfer]["file_path"]
            ):
                logging.info(
                    f"♻️ Old version {minio_files[file_to_transfer]['file_path']} "
                    f"will be replaced with {ftp_files[file_to_transfer]['file_path']}"
                )
                # storing files that have changed name with update as {"new_name": "old_name"}
                files_to_update_new_name[ftp_files[file_to_transfer]["file_path"]] = (
                    minio_files[file_to_transfer]["file_path"]
                )
            else:
                logging.info("🔃 This file already exists, it will only be updated")
                files_to_update_same_name.append(
                    minio_files[file_to_transfer]["file_path"]
                )
        else:
            logging.info("🆕 This is a completely new file")
            new_files.append(ftp_files[file_to_transfer]["file_path"])
        # we are recreating the file structure from FTP to Minio
        true_path, global_path = get_path(ftp_files[file_to_transfer]["file_path"])
        file_name = ftp_files[file_to_transfer]["file_path"].split("/")[-1]
        ftp.cwd("/" + true_path)
        # downloading the file from FTP
        with open(DATADIR + "/" + file_name, "wb") as local_file:
            ftp.retrbinary("RETR " + file_name, local_file.write)

        # sending file to Minio
        try:
            minio_meteo.send_files(
                list_files=[
                    File(
                        source_path=f"{DATADIR}/",
                        source_name=file_name,
                        dest_path=minio_folder + true_path + "/",
                        dest_name=file_name,
                    )
                ],
                ignore_airflow_env=True,
            )
            updated_datasets.add(global_path)
        except Exception:
            logging.error("⚠️ Unable to send file")
        os.remove(f"{DATADIR}/{file_name}")
    logging.info("___________________________")
    logging.info(f"{len(new_files)} new files: {new_files}")
    logging.info(
        f"{len(files_to_update_same_name)} updated same name: {files_to_update_same_name}"
    )
    logging.info(
        f"{len(files_to_update_new_name)} updated new name: {files_to_update_new_name}"
    )

    # re-getting Minio files in case new files have been transfered for downstream tasks
    minio_files = minio_meteo.get_all_files_names_and_sizes_from_parent_folder(
        folder=minio_folder,
    )
    ti.xcom_push(key="minio_files", value=minio_files)
    ti.xcom_push(key="updated_datasets", value=updated_datasets)
    ti.xcom_push(key="new_files", value=new_files)
    ti.xcom_push(key="files_to_update_new_name", value=files_to_update_new_name)
    ti.xcom_push(key="files_to_update_same_name", value=files_to_update_same_name)


def get_file_extention(file: str) -> str:
    return ".".join(file.split("_")[-1].split(".")[1:])


def upload_new_files(ti) -> None:
    new_files = ti.xcom_pull(
        key="new_files", task_ids="get_and_upload_file_diff_ftp_minio"
    )
    updated_datasets = ti.xcom_pull(
        key="updated_datasets", task_ids="get_and_upload_file_diff_ftp_minio"
    )
    minio_files = ti.xcom_pull(
        key="minio_files", task_ids="get_and_upload_file_diff_ftp_minio"
    )
    files_to_update_new_name = ti.xcom_pull(
        key="files_to_update_new_name", task_ids="get_and_upload_file_diff_ftp_minio"
    )
    # adding files that have been spotted as new files in other processings
    spotted_new_files = ti.xcom_pull(
        key="new_files", task_ids="handle_updated_files_same_name"
    ) + ti.xcom_pull(key="new_files", task_ids="handle_updated_files_new_name")
    resources_lists = get_resource_lists()
    new_files += spotted_new_files
    new_files = list(set(new_files))

    # adding files that are on minio, not updated from FTP in this batch,
    # but that are missing on data.gouv
    for file_path in reversed(minio_files.keys()):
        clean_file_path = file_path.replace(minio_folder, "")
        _, global_path = get_path(clean_file_path)
        file_with_ext = file_path.split("/")[-1]
        url = f"https://{MINIO_URL}/{bucket}/{file_path}"
        # we add the file to the new files list if the URL is not in the dataset
        # it is supposed to be in, and if it's not already in the list,
        # and if it's not an old file that has been renamed (values of new_name)
        # though they should have been deleted by now, but just in case
        if (
            url not in resources_lists.get(global_path, [])
            and clean_file_path not in new_files
            and clean_file_path not in files_to_update_new_name.values()
        ):
            # this handles the case of files having been deleted from data.gouv
            # but not from Minio
            logging.info(f"This file is not on data.gouv, uploading: {clean_file_path}")
            new_files.append(clean_file_path)

    new_files_datasets = set()
    went_wrong = []
    for idx, clean_file_path in enumerate(reversed(sorted(new_files))):
        file_with_ext, global_path, resource_name, description, url, is_doc = (
            build_resource(
                clean_file_path,
                minio_folder,
            )
        )
        logging.info(
            f"Creating new {'documentation ' if is_doc else ''}resource for: "
            + file_with_ext
        )
        try:
            local_client.resource().create_remote(
                dataset_id=config[global_path]["dataset_id"][AIRFLOW_ENV],
                payload={
                    "url": url,
                    "filesize": minio_files[minio_folder + clean_file_path],
                    "title": resource_name if not is_doc else file_with_ext,
                    "type": "main" if not is_doc else "documentation",
                    "format": get_file_extention(file_with_ext),
                    "description": description,
                },
            )
            raise_if_duplicates(idx)
            new_files_datasets.add(global_path)
            updated_datasets.add(global_path)
        except KeyError:
            logging.warning("⚠️ no config for this file")
            # the file was not uploaded, removing it from the list of new files
            went_wrong.append(clean_file_path)
    ti.xcom_push(key="new_files_datasets", value=new_files_datasets)
    ti.xcom_push(key="updated_datasets", value=updated_datasets)
    ti.xcom_push(key="new_files", value=[f for f in new_files if f not in went_wrong])


def handle_updated_files_same_name(ti) -> None:
    updated_datasets = ti.xcom_pull(
        key="updated_datasets", task_ids="get_and_upload_file_diff_ftp_minio"
    )
    files_to_update_same_name = ti.xcom_pull(
        key="files_to_update_same_name", task_ids="get_and_upload_file_diff_ftp_minio"
    )
    minio_files = ti.xcom_pull(
        key="minio_files", task_ids="get_and_upload_file_diff_ftp_minio"
    )
    resources_lists = get_resource_lists()

    new_files = []
    for idx, file_path in enumerate(files_to_update_same_name):
        _, global_path = get_path(file_path)
        if global_path not in resources_lists:
            logging.warning(f"⚠️ no config for this file: {file_path}")
            continue
        file_with_ext = file_path.split("/")[-1]
        url = f"https://{MINIO_URL}/{bucket}/{minio_folder + file_path}"
        logging.info(f"Resource already exists and name unchanged: {file_with_ext}")
        # only pinging the resource to update the size of the file
        # and therefore also updating the last modification date
        if not resources_lists[global_path].get(url):
            logging.info(
                "⚠️ this file is not on data.gouv yet, it will be added as a new file"
            )
            # new_files.append(file_path)
            continue
        local_client.resource(
            id=resources_lists[global_path][url]["id"],
            dataset_id=config[global_path]["dataset_id"][AIRFLOW_ENV],
            fetch=False,
        ).update(
            payload={
                "filesize": minio_files[minio_folder + file_path],
            },
        )
        raise_if_duplicates(idx)
        updated_datasets.add(global_path)
    ti.xcom_push(key="updated_datasets", value=updated_datasets)
    ti.xcom_push(key="new_files", value=new_files)


def handle_updated_files_new_name(ti) -> None:
    updated_datasets = ti.xcom_pull(
        key="updated_datasets", task_ids="get_and_upload_file_diff_ftp_minio"
    )
    files_to_update_new_name = ti.xcom_pull(
        key="files_to_update_new_name", task_ids="get_and_upload_file_diff_ftp_minio"
    )
    minio_files = ti.xcom_pull(
        key="minio_files", task_ids="get_and_upload_file_diff_ftp_minio"
    )
    resources_lists = get_resource_lists()

    new_files = []
    for idx, file_path in enumerate(files_to_update_new_name):
        file_with_ext, global_path, resource_name, description, url, is_doc = (
            build_resource(file_path, minio_folder)
        )
        # resource is updated with a new name, we redirect the existing resource, rename it
        # and update size and description
        logging.info(f"Updating URL and metadata for: {file_with_ext}")
        # accessing the file's old path using its new one
        old_file_path = files_to_update_new_name[file_path]
        old_url = f"https://{MINIO_URL}/{bucket}/{minio_folder + old_file_path}"
        if not resources_lists[global_path].get(old_url):
            logging.info(
                "⚠️ this file is not on data.gouv yet, it will be added as a new file"
            )
            new_files.append(file_path)
            continue
        local_client.resource(
            id=resources_lists[global_path][old_url]["id"],
            dataset_id=config[global_path]["dataset_id"][AIRFLOW_ENV],
            fetch=False,
        ).update(
            payload={
                "url": url,
                "title": resource_name if not is_doc else file_with_ext,
                "description": description,
                "filesize": minio_files[minio_folder + file_path],
            },
        )
        raise_if_duplicates(idx)
        updated_datasets.add(global_path)
    ti.xcom_push(key="updated_datasets", value=updated_datasets)
    ti.xcom_push(key="new_files", value=new_files)


def update_temporal_coverages(ti) -> None:
    period_starts = ti.xcom_pull(
        key="period_starts", task_ids="get_current_files_on_minio"
    )
    updated_datasets = set()
    # datasets have been updated in all three tasks, we gather them here
    for task in [
        "upload_new_files",
        "handle_updated_files_same_name",
        "handle_updated_files_new_name",
    ]:
        updated_datasets = updated_datasets | ti.xcom_pull(
            key="updated_datasets", task_ids=task
        )
    logging.info("Updating datasets temporal_coverage")
    for path in updated_datasets:
        if path in period_starts:
            # for now the tags are erased when touching the metadata so we save them and put them back
            tags = requests.get(
                f"{local_client.base_url}/api/1/datasets/{config[path]['dataset_id'][AIRFLOW_ENV]}/"
            ).json()["tags"]
            local_client.dataset(
                config[path]["dataset_id"][AIRFLOW_ENV], fetch=False
            ).update(
                payload={
                    "temporal_coverage": {
                        "start": datetime(period_starts[path], 1, 1).strftime(
                            "%Y-%m-%dT%H:%M:%S.%fZ"
                        ),
                        "end": datetime.today().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                    },
                    "tags": tags,
                },
            )
    ti.xcom_push(key="updated_datasets", value=updated_datasets)


def log_modified_files(ti) -> None:
    new_files = ti.xcom_pull(key="new_files", task_ids="upload_new_files")
    files_to_update_new_name = ti.xcom_pull(
        key="files_to_update_new_name", task_ids="get_and_upload_file_diff_ftp_minio"
    )
    files_to_update_same_name = ti.xcom_pull(
        key="files_to_update_same_name", task_ids="get_and_upload_file_diff_ftp_minio"
    )
    log_file_path = "data/updated_files.json"
    log_file = json.loads(minio_meteo.get_file_content(log_file_path))
    today = datetime.now().strftime("%Y-%m-%d")
    log_file["latest_update"] = today
    if log_file.get(today):
        # the DAG runs more than once a day, we don't want to erase the info
        # from previous runs, so we append what's not already here
        already_here = set(file["name"] for file in log_file[today])
        log_file[today] += [
            {
                "folder": "/".join(k.split("/")[:-1]),
                "name": k.split("/")[-1],
            }
            for k in new_files
            + files_to_update_same_name
            + list(files_to_update_new_name.keys())
            if k.split("/")[-1] not in already_here
        ]
    else:
        log_file[today] = [
            {
                "folder": "/".join(k.split("/")[:-1]),
                "name": k.split("/")[-1],
            }
            for k in new_files
            + files_to_update_same_name
            + list(files_to_update_new_name.keys())
        ]
    # keeping logs for the last month only, and all keys that are not dates
    threshold = (datetime.today() - timedelta(days=30)).strftime("%Y-%m-%d")
    log_file = {
        k: v
        for k, v in log_file.items()
        if not re.match(r"\d+-\d{2}-\d{2}", k) or k >= threshold
    }
    logging.info(f"{len(log_file)} clés dans le fichier : {list(log_file.keys())}")
    with open(f"{DATADIR}/{log_file_path.split('/')[-1]}", "w") as f:
        json.dump(log_file, f)
    minio_meteo.send_files(
        list_files=[
            File(
                source_path=f"{DATADIR}/",
                source_name=log_file_path.split("/")[-1],
                dest_path=get_path(log_file_path)[0] + "/",
                dest_name=log_file_path.split("/")[-1],
            )
        ],
        ignore_airflow_env=True,
    )


def delete_replaced_minio_files(ti) -> None:
    # files that have been renamed while update will be removed
    files_to_update_new_name = ti.xcom_pull(
        key="files_to_update_new_name", task_ids="get_and_upload_file_diff_ftp_minio"
    )
    for old_file in files_to_update_new_name.values():
        minio_meteo.delete_file(file_path=minio_folder + old_file)


def notification_mattermost(ti) -> None:
    new_files_datasets = ti.xcom_pull(
        key="new_files_datasets", task_ids="upload_new_files"
    )
    updated_datasets = ti.xcom_pull(
        key="updated_datasets", task_ids="update_temporal_coverages"
    )
    logging.info(new_files_datasets)
    logging.info(updated_datasets)

    message = "##### 🌦️ Données météo mises à jour :"
    if not (new_files_datasets or updated_datasets):
        message += "\nAucun changement."
    else:
        for path in updated_datasets:
            if path in config:
                message += f"\n- [{path}]"
                message += f"({local_client.base_url}/fr/datasets/{config[path]['dataset_id'][AIRFLOW_ENV]}/) : "
                if path in new_files_datasets:
                    message += "nouvelles données :new:"
                else:
                    message += "mise à jour des métadonnées"

    issues = defaultdict(dict)
    allowed_patterns = defaultdict(list)
    paths = {}
    for path in config:
        allowed_patterns[config[path]["dataset_id"][AIRFLOW_ENV]].append(
            config[path]["name_template"]
        )
        paths[config[path]["dataset_id"][AIRFLOW_ENV]] = path
    for dataset_id in allowed_patterns:
        resp = requests.get(
            f"{local_client.base_url}/api/1/datasets/{dataset_id}/",
            headers={"X-fields": "resources{title,id,type}"},
        )
        if not resp.ok:
            logging.warning(f"Could not access dataset {local_client.base_url}/api/1/datasets/{dataset_id}/")
            continue
        resources = resp.json()["resources"]
        for r in resources:
            if (
                r["type"] == "main"
                and any(k for k in allowed_patterns[dataset_id])
                and not any(
                    r["title"].startswith(template.split("_")[0])
                    for template in allowed_patterns[dataset_id]
                )
            ):
                issues[dataset_id][r["id"]] = r["title"]
    if issues:
        message += "\n:alert: Des ressources semblent mal placées :\n"
        for dataset_id in issues:
            message += (
                f"- [{paths[dataset_id]}]"
                f"({local_client.base_url}/fr/datasets/{dataset_id}/) :\n"
            )
            for rid in issues[dataset_id]:
                message += (
                    f"   - [{issues[dataset_id][rid]}]({local_client.base_url}/fr/datasets/"
                    f"{dataset_id}/#/resources/{rid})\n"
                )
    send_message(message)


# %% TO BE REMOVED SOMEWHEN
import pandas as pd  # noqa


def raise_if_duplicates(idx: int) -> None:
    if idx > 9 and idx % 10 != 0:
        # checking for duplicates if not many cases (new file or new name)
        # or every 10 files processed
        return
    catalog = pd.read_csv(
        "https://www.data.gouv.fr/fr/organizations/meteo-france/datasets-resources.csv",
        sep=";",
        usecols=["url", "id", "dataset.id"],
    )
    dups = catalog["id"].value_counts().reset_index()
    dups = dups.loc[dups["count"] > 1, "id"].to_list()
    if dups:
        raise Exception("Duplicates:", dups)
