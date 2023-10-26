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
    get_all_from_api_query,
    DATAGOUV_URL
)
from datagouvfr_data_pipelines.utils.mattermost import send_message
from datagouvfr_data_pipelines.utils.minio import send_files, get_all_files_from_parent_folder
import ftplib
import os
import json
from datetime import datetime

DAG_FOLDER = "datagouvfr_data_pipelines/data_processing/"
DATADIR = f"{AIRFLOW_DAG_TMP}meteo/data"


def list_ftp_files_recursive(ftp, path='', base_path=''):
    files = []
    try:
        ftp.cwd(path)
        current_path = f"{base_path}/{path}" if base_path else path
        ftp.retrlines('LIST', lambda x: files.append((current_path.split('//')[-1], x.split()[-1])))
        for item in files:
            if '.' not in item[1]:
                files += list_ftp_files_recursive(ftp, f"{path}/{item[1]}", current_path)
    except ftplib.error_perm:
        pass
    return files


def get_current_files_on_ftp(ti, ftp):
    ftp_files = list_ftp_files_recursive(ftp)
    print(ftp_files)
    ftp_files = [
        path + '/' + file for path, file in ftp_files if '.' in file
    ]
    ti.xcom_push(key='ftp_files', value=ftp_files)


def get_current_files_on_minio(ti, minio_folder):
    minio_files = get_all_files_from_parent_folder(
        MINIO_URL=MINIO_URL,
        MINIO_BUCKET="meteofrance",
        MINIO_USER=SECRET_MINIO_DATA_PIPELINE_USER,
        MINIO_PASSWORD=SECRET_MINIO_DATA_PIPELINE_PASSWORD,
        folder=minio_folder
    )
    ti.xcom_push(key='minio_files', value=minio_files)


def get_and_upload_file_diff_ftp_minio(ti, minio_folder, ftp):
    minio_files = ti.xcom_pull(key='minio_files', task_ids='get_current_files_on_minio')
    minio_files = [f.replace(minio_folder, '') for f in minio_files]

    ftp_files = ti.xcom_pull(key='ftp_files', task_ids='get_current_files_on_ftp')
    diff_files = [f for f in ftp_files if f not in minio_files]

    # doing it one file at a time in order not to overload production server
    for file_to_transfer in diff_files:
        print(f"\nTransfering {file_to_transfer}...")
        # we are recreating the file structure from FTP to Minio
        path_to_file = '/'.join(file_to_transfer.split('/')[:-1])
        file_name = file_to_transfer.split('/')[-1]
        ftp.cwd('/' + path_to_file)
        ftp.pwd()
        # downaloading the file from FTP
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


# def upload_files_datagouv(ti, minio_folder):
#     with open(f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}dvf/config/dgv.json") as fp:
#         config = json.load(fp)
#     diff_files = ti.xcom_pull(key='diff_files', task_ids='get_and_upload_file_diff_ftp_minio')
#     for file in diff_files:
#         path_to_file = '/'.join(file.split('/')[:-1])

#         post_remote_resource(
#             api_key=DATAGOUV_SECRET_API_KEY,
#             remote_url=f"https://object.files.data.gouv.fr/meteofrance/{minio_folder}{file}",
#             dataset_id=config[path_to_file][AIRFLOW_ENV]["dataset_id"],
#             # filesize=,
#             title="Résultats généraux",
#             format="csv.gz",
#             description=f" (dernière modification : {datetime.today()})",
#         )
