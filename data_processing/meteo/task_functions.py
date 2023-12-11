from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_HOME,
    AIRFLOW_DAG_TMP,
    DATAGOUV_SECRET_API_KEY,
    AIRFLOW_ENV,
    MINIO_URL,
    SECRET_MINIO_DATA_PIPELINE_USER,
    SECRET_MINIO_DATA_PIPELINE_PASSWORD,
)
from datagouvfr_data_pipelines.utils.datagouv import (
    post_remote_resource,
    update_dataset_or_resource_metadata,
    DATAGOUV_URL
)
from datagouvfr_data_pipelines.utils.mattermost import send_message
from datagouvfr_data_pipelines.utils.minio import (
    send_files,
    get_all_files_names_and_sizes_from_parent_folder,
    delete_file,
)
import ftplib
import os
import json
from datetime import datetime, timedelta, timezone
import re
import requests
from dateutil import parser

DAG_FOLDER = "datagouvfr_data_pipelines/data_processing/"
DATADIR = f"{AIRFLOW_DAG_TMP}meteo/data"
with open(f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}meteo/config/dgv.json") as fp:
    config = json.load(fp)
hooks = ["latest", "previous"]


def clean_hooks(string, hooks=hooks):
    _ = string
    for h in hooks:
        _ = _.replace(f"{h}-", "")
    return _


def build_file_id(file, path):
    # we are creating ids for files based on their name so that we can
    # match files that have been updated with a name change
    # for instance:
    # QUOT_SIM2_latest-2020-202310.csv.gz becomes QUOT_SIM2_latest-2020-202311.csv.gz
    # when Nov2023 is added, but it actually *is* just a content update of the same file
    file_id = file
    if config.get(path, {}).get("source_pattern"):
        params = re.match(config[path]['source_pattern'], file)
        params = params.groupdict() if params else {}
        if "PERIOD" in params and any([
            h in params["PERIOD"] for h in hooks
        ]):
            # this will have to change if more hooks are added
            params["PERIOD"] = "latest" if "latest" in params["PERIOD"] else "previous"
        if params:
            file_id = config[path]["name_template"].format(**params)
    return file_id


def list_ftp_files_recursive(ftp, path='', base_path=''):
    files = []
    try:
        ftp.cwd(path)
        current_path = f"{base_path}/{path}" if base_path else path
        ftp.retrlines(
            'LIST',
            lambda x: files.append((
                current_path.split('//')[-1],
                x.split()[-1],
                int(x.split()[4]),
                x.split()[5:7],
            ))
        )
        for item in files:
            if '.' not in item[1]:
                files += list_ftp_files_recursive(ftp, f"{path}/{item[1]}", current_path)
    except ftplib.error_perm:
        pass
    return files


def get_current_files_on_ftp(ti, ftp):
    raw_ftp_files = list_ftp_files_recursive(ftp)
    ftp_files = {}
    # pour distinguer les nouveaux fichiers (nouvelles décennie révolue, période stock...)
    # des fichiers qui changent de nom lors de mises à jour (QUOT_SIM2_2020-202309.csv.gz
    # qui devient QUOT_SIM2_2020-202310.csv.gz), on utilise des balises afin de cibler ces fichiers
    # et de remplacer l'ancien par le nouveau au lieu d'ajouter le nouveau fichier
    # et de laisser les deux coexister
    for (path, file, size, date_list) in raw_ftp_files:
        if '.' in file:
            path = path.lstrip("/")
            file_id = build_file_id(file, path)
            ftp_files.update({
                # we keep path in the id just in case two files would have the same name/id
                # but in different folders
                path + '/' + file_id: {
                    "file_path": path + '/' + file,
                    "size": size,
                    "modif_date": parser.parse(' '.join(date_list))
                }
            })
    for f in ftp_files:
        print(f, ':', ftp_files[f])
    ti.xcom_push(key='ftp_files', value=ftp_files)


def get_current_files_on_minio(ti, minio_folder):
    minio_files = get_all_files_names_and_sizes_from_parent_folder(
        MINIO_URL=MINIO_URL,
        MINIO_BUCKET="meteofrance",
        MINIO_USER=SECRET_MINIO_DATA_PIPELINE_USER,
        MINIO_PASSWORD=SECRET_MINIO_DATA_PIPELINE_PASSWORD,
        folder=minio_folder
    )
    # getting the start of each time period to update datasets temporal_coverage
    period_starts = {}
    for file in minio_files:
        path = '/'.join(file.replace(minio_folder, '').split('/')[:-1])
        file_with_ext = file.split('/')[-1]
        if config.get(path, {}).get('source_pattern'):
            params = re.match(config[path]['source_pattern'], file_with_ext)
            params = params.groupdict() if params else {}
            if params and 'PERIOD' in params:
                period_starts[path] = min(
                    int(clean_hooks(params['PERIOD']).split('-')[0]),
                    period_starts.get(path, datetime.today().year)
                )
    print(period_starts)

    # de même ici, on utilise les balises pour cibler les fichiers du Minio
    # qui devront être remplacés
    final_minio_files = {}
    for file_path in minio_files:
        clean_file_path = file_path.replace(minio_folder, '')
        file_name = clean_file_path.split('/')[-1]
        path = '/'.join(clean_file_path.split('/')[:-1])
        final_minio_files.update({
            path + '/' + build_file_id(file_name, path): {
                "file_path": clean_file_path,
                "size": minio_files[file_path],
            }
        })
    for f in final_minio_files:
        print(f, ':', final_minio_files[f])
    ti.xcom_push(key='minio_files', value=final_minio_files)
    ti.xcom_push(key='period_starts', value=period_starts)


def get_and_upload_file_diff_ftp_minio(ti, minio_folder, ftp):
    minio_files = ti.xcom_pull(key='minio_files', task_ids='get_current_files_on_minio')
    ftp_files = ti.xcom_pull(key='ftp_files', task_ids='get_current_files_on_ftp')
    # much debated part of the code: how to best get which files to consider here
    # first it was only done with the files' names but what if a file is updated but not renamed?
    # then we thought about checking the size and comparing with Minio
    # but sizes can vary for an identical file depending on where/how it is stored
    # we also thought about a checksum, but hard to compute on the FTP and downloading
    # all files to compute checksums and compare is inefficient
    # our best try: check the modification date on the FTP and take the file if it has
    # been changed since the previous day (DAG will run daily)
    diff_files = [
        f for f in ftp_files
        if f not in minio_files
        or ftp_files[f]['modif_date'] > datetime.now(timezone.utc) - timedelta(days=1)
    ]
    print(f"Synchronizing {len(diff_files)} file{'s' if len(diff_files) > 1 else ''}")
    print(diff_files)
    if len(diff_files) == 0:
        raise ValueError("No new file today, is that normal?")

    new_files = []
    files_to_update_same_name = []
    files_to_update_new_name = {}
    updated_datasets = set()
    # doing it one file at a time in order not to overload production server
    for file_to_transfer in diff_files:
        print("___________________________")
        # if the file id is in minio_files, it means that the current file is not
        # a true new file, but an updated file. We delete the old file only if the file's name
        # has changed, otherwise new one will just overwrite the old one, and upload the new
        # one right after. NB: this will cause short downtimes on these files when the
        # real file names are changed, because the remote URL on data.gouv won't be reachable
        # until it is updated in the next step
        print(f"Transfering {ftp_files[file_to_transfer]['file_path']}...")
        if file_to_transfer in minio_files:
            if minio_files[file_to_transfer]['file_path'] != ftp_files[file_to_transfer]['file_path']:
                print(
                    f"♻️ Old version {minio_files[file_to_transfer]['file_path']}",
                    f"will be replaced with {ftp_files[file_to_transfer]['file_path']}"
                )
                # storing files that have changed name with update as {'new_name': 'old_name'}
                files_to_update_new_name[
                    ftp_files[file_to_transfer]['file_path']
                ] = minio_files[file_to_transfer]['file_path']
            else:
                print("🔃 This file already exists, it will only be updated")
                files_to_update_same_name.append(minio_files[file_to_transfer]['file_path'])
        else:
            print("🆕 This is a completely new file")
            new_files.append(ftp_files[file_to_transfer]['file_path'])
        # we are recreating the file structure from FTP to Minio
        path = '/'.join(ftp_files[file_to_transfer]['file_path'].split('/')[:-1])
        file_name = ftp_files[file_to_transfer]['file_path'].split('/')[-1]
        ftp.cwd('/' + path)
        # downloading the file from FTP
        with open(DATADIR + '/' + file_name, 'wb') as local_file:
            ftp.retrbinary('RETR ' + file_name, local_file.write)

        # sending file to Minio
        try:
            send_files(
                MINIO_URL=MINIO_URL,
                MINIO_BUCKET="meteofrance",
                MINIO_USER=SECRET_MINIO_DATA_PIPELINE_USER,
                MINIO_PASSWORD=SECRET_MINIO_DATA_PIPELINE_PASSWORD,
                list_files=[
                    {
                        "source_path": f"{DATADIR}/",
                        "source_name": file_name,
                        "dest_path": minio_folder + path + '/',
                        "dest_name": file_name,
                    }
                ],
                ignore_airflow_env=True
            )
            updated_datasets.add(path)
        except:
            print("⚠️ Unable to send file")
        os.remove(f"{DATADIR}/{file_name}")
    print(new_files)
    print(files_to_update_same_name)
    print(files_to_update_new_name)
    ti.xcom_push(key='updated_datasets', value=updated_datasets)
    ti.xcom_push(key='new_files', value=new_files)
    ti.xcom_push(key='files_to_update_new_name', value=files_to_update_new_name)
    ti.xcom_push(key='files_to_update_same_name', value=files_to_update_same_name)


def get_file_extention(file):
    return '.'.join(file.split('.')[-file.count('.'):])


def upload_files_datagouv(ti, minio_folder):
    # this uploads/synchronizes all resources on Minio and data.gouv.fr
    period_starts = ti.xcom_pull(key="period_starts", task_ids="get_current_files_on_minio")
    updated_datasets = ti.xcom_pull(key="updated_datasets", task_ids="get_and_upload_file_diff_ftp_minio")
    new_files = ti.xcom_pull(key="new_files", task_ids="get_and_upload_file_diff_ftp_minio")
    files_to_update_new_name = ti.xcom_pull(
        key="files_to_update_new_name",
        task_ids="files_to_update_new_name"
    )
    files_to_update_same_name = ti.xcom_pull(
        key="files_to_update_same_name",
        task_ids="files_to_update_same_name"
    )

    # re-getting Minio files in case new files have been transfered
    minio_files = get_all_files_names_and_sizes_from_parent_folder(
        MINIO_URL=MINIO_URL,
        MINIO_BUCKET="meteofrance",
        MINIO_USER=SECRET_MINIO_DATA_PIPELINE_USER,
        MINIO_PASSWORD=SECRET_MINIO_DATA_PIPELINE_PASSWORD,
        folder=minio_folder
    )

    # check for presence is done with URLs (could also be with resource name)
    resources_lists = {
        path: {
            r['url']: r['id'] for r in requests.get(
                f"{DATAGOUV_URL}/api/1/datasets/{config[path]['dataset_id'][AIRFLOW_ENV]}/",
                headers={'X-fields': 'resources{id,url}'}
            ).json()['resources']
        }
        for path in config.keys()
    }

    new_files_datasets = set()
    # reversed so that files get uploaded in a better order for UI
    for file in reversed(minio_files.keys()):
        file_path = minio_files[file]["file_path"]
        path = '/'.join(file_path.replace(minio_folder, '').split('/')[:-1])
        file_with_ext = file_path.split('/')[-1]
        url = f"https://object.files.data.gouv.fr/meteofrance/{file_path}"
        if file in files_to_update_same_name:
            print("Resource already exists and name unchanged:", file_with_ext)
            # should we touch the resource's metadata to update the last
            # modification date on data.gouv.fr?
            continue
        else:
            # differenciation ressource principale VS documentation
            is_doc = False
            description = ""
            try:
                # two known errors:
                # 1. files that don't match neither pattern
                # 2. files that are contained in subfolders => creating new paths in config
                if config[path]['doc_pattern'] and re.match(config[path]['doc_pattern'], file_with_ext):
                    resource_name = file_with_ext.split(".")[0]
                    is_doc = True
                else:
                    # peut-être mettre un 'if params else' ici pour publier les ressources
                    # qui posent problème en doc quoiqu'il ?
                    params = re.match(config[path]['source_pattern'], file_with_ext).groupdict()
                    for k in params:
                        # removing hook names for data.gouv display
                        params[k] = clean_hooks(params[k])
                    resource_name = config[path]['name_template'].format(**params)
                    description = config[path]['description_template'].format(**params)
                # if the resource doesn't exist yet, we simply create it
                if file in new_files:
                    print(
                        f'Creating new {"documentation " if is_doc else ""}resource for:',
                        file_with_ext
                    )
                    post_remote_resource(
                        api_key=DATAGOUV_SECRET_API_KEY,
                        remote_url=url,
                        dataset_id=config[path]["dataset_id"][AIRFLOW_ENV],
                        filesize=minio_files[file],
                        title=resource_name if not is_doc else file_with_ext,
                        type="main" if not is_doc else "documentation",
                        format=get_file_extention(file_with_ext),
                        description=description,
                    )
                    new_files_datasets.add(path)
                    updated_datasets.add(path)
                # resource is updated with a new name, we redirect the existing resource, rename it
                # and update size and description
                # values because we are dealing with the resource's new name
                elif file in files_to_update_new_name.values:
                    print(
                        'Updating URL and metadata for:',
                        file_with_ext
                    )
                    # accessing the file's old path using its new one
                    old_file_path = files_to_update_new_name[file_path]
                    old_url = f"https://object.files.data.gouv.fr/meteofrance/{old_file_path}"
                    update_dataset_or_resource_metadata(
                        api_key=DATAGOUV_SECRET_API_KEY,
                        payload={
                            "url": url,
                            "title": resource_name if not is_doc else file_with_ext,
                            "description": description,
                            "filesize": minio_files[file],
                        },
                        dataset_id=config[path]["dataset_id"][AIRFLOW_ENV],
                        resource_id=resources_lists[old_url],
                    )
                else:
                    print("⚠️ This should never be seen: ", file_with_ext)
            except:
                print('issue with file:', file_with_ext)
    print("Updating datasets temporal_coverage")
    for path in updated_datasets:
        if path in period_starts:
            update_dataset_or_resource_metadata(
                api_key=DATAGOUV_SECRET_API_KEY,
                payload={"temporal_coverage": {
                    "start": datetime(period_starts[path], 1, 1).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                    "end": datetime.today().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                },
                    # to be removed when data is ready to opened
                    "private": True
                },
                dataset_id=config[path]['dataset_id'][AIRFLOW_ENV]
            )
    ti.xcom_push(key='new_files_datasets', value=new_files_datasets)
    ti.xcom_push(key='updated_datasets', value=updated_datasets)


def delete_replaced_minio_files(ti, minio_folder):
    # files that have been renamed while update will be removed
    files_to_update_new_name = ti.xcom_pull(
        key="files_to_update_new_name",
        task_ids="get_and_upload_file_diff_ftp_minio"
    )
    for old_file in files_to_update_new_name:
        delete_file(
            MINIO_URL=MINIO_URL,
            MINIO_BUCKET="meteofrance",
            MINIO_USER=SECRET_MINIO_DATA_PIPELINE_USER,
            MINIO_PASSWORD=SECRET_MINIO_DATA_PIPELINE_PASSWORD,
            file_path=minio_folder + old_file
        )


def notification_mattermost(ti):
    new_files_datasets = ti.xcom_pull(key="new_files_datasets", task_ids="upload_files_datagouv")
    updated_datasets = ti.xcom_pull(key="updated_datasets", task_ids="upload_files_datagouv")

    message = "🌦️ Données météo mises à jour :"
    if not (new_files_datasets or updated_datasets):
        message += "\nAucun changement."
    else:
        for path in updated_datasets:
            if path in config:
                message += f"\n- [{path}]"
                message += f"({DATAGOUV_URL}/fr/datasets/{config[path]['dataset_id'][AIRFLOW_ENV]}/) : "
                if path in new_files_datasets:
                    message += "nouvelles données"
                else:
                    message += "mise à jour des métadonnées"
    send_message(message)
