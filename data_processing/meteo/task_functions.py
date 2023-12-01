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
    DATAGOUV_URL
)
from datagouvfr_data_pipelines.utils.mattermost import send_message
from datagouvfr_data_pipelines.utils.minio import (
    send_files,
    get_all_files_names_and_sizes_from_parent_folder
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
    ftp_files = list_ftp_files_recursive(ftp)
    print(ftp_files)
    ftp_files = {
        path + '/' + file: {"size": size, "modif_date": parser.parse(' '.join(date_list))}
        for (path, file, size, date_list) in ftp_files
        if '.' in file
    }
    ti.xcom_push(key='ftp_files', value=ftp_files)


def get_current_files_on_minio(ti, minio_folder):
    minio_files = get_all_files_names_and_sizes_from_parent_folder(
        MINIO_URL=MINIO_URL,
        MINIO_BUCKET="meteofrance",
        MINIO_USER=SECRET_MINIO_DATA_PIPELINE_USER,
        MINIO_PASSWORD=SECRET_MINIO_DATA_PIPELINE_PASSWORD,
        folder=minio_folder
    )
    ti.xcom_push(key='minio_files', value=minio_files)


def get_and_upload_file_diff_ftp_minio(ti, minio_folder, ftp):
    minio_files = ti.xcom_pull(key='minio_files', task_ids='get_current_files_on_minio')
    minio_files = {
        k.replace(minio_folder, ''): v for k, v in minio_files.items()
    }

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

    # doing it one file at a time in order not to overload production server
    for file_to_transfer in diff_files:
        print(f"\nTransfering {file_to_transfer}...")
        # we are recreating the file structure from FTP to Minio
        path_to_file = '/'.join(file_to_transfer.split('/')[:-1])
        file_name = file_to_transfer.split('/')[-1]
        ftp.cwd('/' + path_to_file)
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
                        "dest_path": minio_folder + path_to_file + '/',
                        "dest_name": file_name,
                    }
                ],
                ignore_airflow_env=True
            )
        except:
            print("⚠️ Unable to send file")
        os.remove(f"{DATADIR}/{file_name}")
    ti.xcom_push(key='diff_files', value=diff_files)


def get_file_extention(file):
    return '.'.join(file.split('.')[-file.count('.'):])


def upload_files_datagouv(ti, minio_folder):
    # this uploads/synchronizes all resources on Minio and data.gouv.fr
    with open(f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}meteo/config/dgv.json") as fp:
        config = json.load(fp)

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

    updated = set()
    # reversed so that files get uploaded in a better order for UI
    for file in reversed(minio_files.keys()):
        path = '/'.join(file.replace(minio_folder, '').split('/')[:-1])
        file_with_ext = file.split('/')[-1]
        url = f"https://object.files.data.gouv.fr/meteofrance/{file}"
        # differenciation ressource principale VS documentation
        is_doc = False
        description = ""
        try:
            # two known errors:
            # 1. files that don't match neither pattern
            # 2. files that are contained in subfolders
            if config[path]['doc_pattern'] and re.match(config[path]['doc_pattern'], file_with_ext):
                resource_name = file_with_ext.split(".")[0]
                is_doc = True
            else:
                # peut-être mettre un 'if params else' ici pour publier les ressources
                # qui posent problème en doc quoiqu'il ?
                params = re.match(config[path]['source_pattern'], file_with_ext).groupdict()
                resource_name = config[path]['name_template'].format(**params)
                description = config[path]['description_template'].format(**params)

            if url not in resources_lists[path].keys():
                # si la resource n'existe pas, on la crée
                print('Creating new resource for: ', file_with_ext)
                post_remote_resource(
                    api_key=DATAGOUV_SECRET_API_KEY,
                    remote_url=url,
                    dataset_id=config[path]["dataset_id"][AIRFLOW_ENV],
                    filesize=minio_files[file],
                    title=resource_name,
                    type="main" if not is_doc else "documentation",
                    format=get_file_extention(file_with_ext),
                    description=description,
                )
                updated.add(path)
            else:
                # qu'est-ce qu'on veut faire ici ? un check que rien n'a changé (hydra, taille du fichier) ?
                print("Resource already exists: ", file_with_ext)
        except:
            print('issue with file:', file)
    ti.xcom_push(key='updated', value=updated)


def notification_mattermost(ti):
    with open(f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}meteo/config/dgv.json") as fp:
        config = json.load(fp)
    updated = ti.xcom_pull(key="updated", task_ids="upload_files_datagouv")

    message = "🌦️ Données météo mises à jour :"
    if not updated:
        message += "\nAucun changement."
        send_message(message)
    else:
        for path in updated:
            message += f"\n- [dataset {path}]"
            message += f"({DATAGOUV_URL}/fr/datasets/{config[path]['dataset_id'][AIRFLOW_ENV]}/)"
        send_message(message)
