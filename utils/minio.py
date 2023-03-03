import boto3
from minio import Minio
from typing import List, TypedDict
import os
from datagouvfr_data_pipelines.config import AIRFLOW_ENV


class File(TypedDict):
    source_name: str
    source_path: str
    dest_name: str
    dest_path: str


def send_files(
    MINIO_URL: str,
    MINIO_BUCKET: str,
    MINIO_USER: str,
    MINIO_PASSWORD: str,
    list_files: List[File],
):
    """Send list of file to Minio bucket

    Args:
        MINIO_URL (str): Minio endpoint
        MINIO_BUCKET (str): bucket
        MINIO_USER (str): user
        MINIO_PASSWORD (str): password
        list_files (List[File]): List of Dictionnaries containing for each
        `source_path` and `source_name` : local file location ;
        `dest_path` and `dest_name` : minio location (inside bucket specified) ;

    Raises:
        Exception: when specified local file does not exists
        Exception: when specified bucket does not exist
    """
    client = Minio(
        MINIO_URL,
        access_key=MINIO_USER,
        secret_key=MINIO_PASSWORD,
        secure=True,
    )
    found = client.bucket_exists(MINIO_BUCKET)
    if found:
        for file in list_files:
            is_file = os.path.isfile(
                os.path.join(file["source_path"], file["source_name"])
            )
            if is_file:
                client.fput_object(
                    MINIO_BUCKET,
                    f"{AIRFLOW_ENV}/{file['dest_path']}{file['dest_name']}",
                    os.path.join(file["source_path"], file["source_name"]),
                )
            else:
                raise Exception(
                    f"file {file['source_path']}{file['source_name']} "
                    "does not exists"
                )
    else:
        raise Exception(f"Bucket {MINIO_BUCKET} does not exists")


def get_files(
    MINIO_URL: str,
    MINIO_BUCKET: str,
    MINIO_USER: str,
    MINIO_PASSWORD: str,
    list_files: List[File],
):
    """Retrieve list of files from Minio

    Args:
        MINIO_URL (str): Minio endpoint
        MINIO_BUCKET (str): bucket
        MINIO_USER (str): user
        MINIO_PASSWORD (str): password
        list_files (List[File]): List of Dictionnaries containing for each
        `source_path` and `source_name` : Minio location inside specified bucket ;
        `dest_path` and `dest_name` : local file destination ;

    Raises:
        Exception: _description_
    """
    client = Minio(
        MINIO_URL,
        access_key=MINIO_USER,
        secret_key=MINIO_PASSWORD,
        secure=True,
    )
    found = client.bucket_exists(MINIO_BUCKET)
    if found:
        for file in list_files:
            client.fget_object(
                MINIO_BUCKET,
                f"{AIRFLOW_ENV}/{file['source_path']}{file['source_name']}",
                f"{file['dest_path']}{file['dest_name']}",
            )
    else:
        raise Exception(f"Bucket {MINIO_BUCKET} does not exists")


def compare_files(
    MINIO_URL: str,
    MINIO_BUCKET: str,
    MINIO_USER: str,
    MINIO_PASSWORD: str,
    file_path_1: str,
    file_path_2: str,
    file_name_1: str,
    file_name_2: str,
):
    """Compare two minio files

    Args:
        MINIO_URL (str): Minio endpoint
        MINIO_BUCKET (str): bucket
        MINIO_USER (str): user
        MINIO_PASSWORD (str): password
        both path and name from files to compare

    """
    s3 = boto3.client(
        "s3",
        endpoint_url=f"https://{MINIO_URL}",
        aws_access_key_id=MINIO_USER,
        aws_secret_access_key=MINIO_PASSWORD,
    )

    file_1 = s3.head_object(
        Bucket=MINIO_BUCKET, Key=f"{AIRFLOW_ENV}/{file_path_1}{file_name_1}"
    )
    file_2 = s3.head_object(
        Bucket=MINIO_BUCKET, Key=f"{AIRFLOW_ENV}/{file_path_2}{file_name_2}"
    )

    print(f"Hash file 1 : {file_1['ETag']}")
    print(f"Hash file 2 : {file_2['ETag']}")
    print(bool(file_1["ETag"] == file_2["ETag"]))

    return bool(file_1["ETag"] == file_2["ETag"])
