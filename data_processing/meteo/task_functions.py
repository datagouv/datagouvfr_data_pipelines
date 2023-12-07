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
with open(f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}meteo/config/dgv.json") as fp:
    config = json.load(fp)


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
    ftp_files = {
        path.lstrip('/') + '/' + file: {
            "size": size,
            "modif_date": parser.parse(' '.join(date_list))
        }
        for (path, file, size, date_list) in ftp_files
        if '.' in file
    }
    # printing files on single lines for easier debug
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
    # printing files on single lines for easier debug
    for f in minio_files:
        print(f, ':', minio_files[f])
    # getting the start of each time period to update datasets metadata
    period_starts = {}
    for file in minio_files:
        path = '/'.join(file.replace(minio_folder, '').split('/')[:-1])
        file_with_ext = file.split('/')[-1]
        if config.get(path, {}).get('source_pattern'):
            params = re.match(config[path]['source_pattern'], file_with_ext)
            params = params.groupdict() if params else {}
            if params:
                if 'PERIOD' in params:
                    period_starts[path] = min(
                        int(params['PERIOD'].split('-')[0]),
                        period_starts.get(path, datetime.today().year)
                    )
                elif 'START' in params:
                    period_starts[path] = min(
                        int(params['START'].split('-')[0]),
                        period_starts.get(path, datetime.today().year)
                    )
    print(period_starts)
    ti.xcom_push(key='minio_files', value=minio_files)
    ti.xcom_push(key='period_starts', value=period_starts)


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

    updated_datasets = set()
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
            updated_datasets.add(path_to_file)
        except:
            print("⚠️ Unable to send file")
        os.remove(f"{DATADIR}/{file_name}")
    print(updated_datasets)
    ti.xcom_push(key='diff_files', value=diff_files)
    ti.xcom_push(key='updated_datasets', value=updated_datasets)


def get_file_extention(file):
    return '.'.join(file.split('.')[-file.count('.'):])


def upload_files_datagouv(ti, minio_folder):
    # this uploads/synchronizes all resources on Minio and data.gouv.fr
    period_starts = ti.xcom_pull(key="period_starts", task_ids="get_current_files_on_minio")
    updated_datasets = ti.xcom_pull(key="updated_datasets", task_ids="get_and_upload_file_diff_ftp_minio")

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
        path = '/'.join(file.replace(minio_folder, '').split('/')[:-1])
        file_with_ext = file.split('/')[-1]
        url = f"https://object.files.data.gouv.fr/meteofrance/{file}"
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
                resource_name = config[path]['name_template'].format(**params)
                description = config[path]['description_template'].format(**params)

            if url not in resources_lists[path].keys():
                # si la resource n'existe pas, on la crée
                print(
                    f'Creating new {"documentation " if is_doc else ""}resource for: ',
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
            else:
                # qu'est-ce qu'on veut faire ici ? un check que rien n'a changé (hydra, taille du fichier) ?
                print("Resource already exists: ", file_with_ext)
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


def notification_mattermost(ti):
    new_files_datasets = ti.xcom_pull(key="new_files_datasets", task_ids="upload_files_datagouv")
    updated_datasets = ti.xcom_pull(key="updated_datasets", task_ids="upload_files_datagouv")

    message = "🌦️ Données météo mises à jour :"
    if not (new_files_datasets or updated_datasets):
        message += "\nAucun changement."
    else:
        for path in updated_datasets:
            if path in config:
                message += f"\n- [dataset {path}]"
                message += f"({DATAGOUV_URL}/fr/datasets/{config[path]['dataset_id'][AIRFLOW_ENV]}/) : "
                if path in new_files_datasets:
                    message += "nouvelles données"
                else:
                    message += "mise à jour des métadonnées"
    send_message(message)
