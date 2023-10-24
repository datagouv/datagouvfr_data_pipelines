import os
import zipfile
from datagouvfr_data_pipelines.utils.mattermost import send_message
import logging
from datagouvfr_data_pipelines.utils.minio import send_files
from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
    MINIO_URL,
    MINIO_BUCKET_DATA_PIPELINE,
    SECRET_MINIO_DATA_PIPELINE_USER,
    SECRET_MINIO_DATA_PIPELINE_PASSWORD,
)

DAG_FOLDER = "datagouvfr_data_pipelines/data_processing/"
TMP_FOLDER = f"{AIRFLOW_DAG_TMP}rne/stock/"
DATADIR = f"{TMP_FOLDER}data"
ZIP_FILE_PATH = f"{TMP_FOLDER}stock_rne.zip"
EXTRACTED_FILES_PATH = f"{TMP_FOLDER}extracted/"


def unzip_files_and_upload_minio(**kwargs):
    with zipfile.ZipFile(ZIP_FILE_PATH, mode="r") as z:
        sent_files = 0
        for file_info in z.infolist():
            # Extract each file one by one
            z.extract(file_info, path=EXTRACTED_FILES_PATH)

            logging.info(f"Saving file {file_info.filename} in MinIO.....")
            send_files(
                MINIO_URL=MINIO_URL,
                MINIO_BUCKET=MINIO_BUCKET_DATA_PIPELINE,
                MINIO_USER=SECRET_MINIO_DATA_PIPELINE_USER,
                MINIO_PASSWORD=SECRET_MINIO_DATA_PIPELINE_PASSWORD,
                list_files=[
                    {
                        "source_path": EXTRACTED_FILES_PATH,
                        "source_name": file_info.filename,
                        "dest_path": "rne/stock/data/",
                        "dest_name": file_info.filename,
                    },
                ],
            )
            sent_files += 1
            # Delete the extracted file
            extracted_file_path = os.path.join(EXTRACTED_FILES_PATH, file_info.filename)
            os.remove(extracted_file_path)

    kwargs["ti"].xcom_push(key="stock_files_rne_count", value=sent_files)


def send_notification_mattermost(**kwargs):
    count_files_rne_stock = kwargs["ti"].xcom_pull(
        key="stock_files_rne_count", task_ids="send_stock_rne"
    )
    send_message(
        f"Données stock RNE mise à jour sur Minio "
        f"- Bucket {MINIO_BUCKET_DATA_PIPELINE} :"
        f"\n - Nombre de fichiers stock : {count_files_rne_stock}"
    )
