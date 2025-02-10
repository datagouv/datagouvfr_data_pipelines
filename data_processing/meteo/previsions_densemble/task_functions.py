import logging

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
    SECRET_MINIO_METEO_PE_USER,
    SECRET_MINIO_METEO_PE_PASSWORD,
)
# from datagouvfr_data_pipelines.utils.mattermost import send_message
from datagouvfr_data_pipelines.utils.minio import MinIOClient
from datagouvfr_data_pipelines.utils.sftp import SFTPClient


DATADIR = f"{AIRFLOW_DAG_TMP}meteo_pe/"
minio_meteo = MinIOClient(
    bucket="meteofrance-pe",
    user=SECRET_MINIO_METEO_PE_USER,
    pwd=SECRET_MINIO_METEO_PE_PASSWORD,
)
minio_folder = "data"
upload_dir = "/uploads/"


def create_client():
    return SFTPClient(conn_name="SFTP_TRANSFER", user="meteofrance")


def get_files_list_on_sftp(ti):
    sftp = create_client()
    files = sftp.list_files_in_directory(upload_dir)
    logging.info(len(files), "files in", upload_dir)
    to_process = []
    for f in files:
        if not f.endswith(".grib"):
            logging.warning(f"> ignoring {f}")
        else:
            to_process.append(f)
    logging.info(len(to_process), "files to process")
    ti.xcom_push(key="files", value=to_process)


def transfer_files_to_minio(ti):
    files = ti.xcom_pull(key="files", task_ids="get_files_list_on_sftp")
    sftp = create_client()
    for file in files[:10]:
        vapp, vconf, date, membre, geom, grid = file.split(".")[0].split("_")
        sftp.download_file(
            remote_file_path=upload_dir + file,
            local_file_path=DATADIR + file,
        )
        minio_meteo.send_files(
            [
                {
                    "source_path": DATADIR,
                    "source_name": file,
                    "dest_path": f"{minio_folder}/{vconf}/{date}/",
                    "dest_name": file,
                },
            ],
            ignore_airflow_env=False,
            burn_after_sending=True,
        )
        sftp.delete_file(upload_dir + file)
