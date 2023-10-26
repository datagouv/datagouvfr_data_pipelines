import boto3
import botocore
import io
from minio import Minio
from minio.commonconfig import CopySource
from typing import List, TypedDict, Optional
import os
from datagouvfr_data_pipelines.config import AIRFLOW_ENV


class File(TypedDict):
    source_path: str
    source_name: str
    dest_path: str
    dest_name: str
    content_type: Optional[str]


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
            print("Sending ", file["source_name"])
            if is_file:
                client.fput_object(
                    MINIO_BUCKET,
                    f"{AIRFLOW_ENV}/{file['dest_path']}{file['dest_name']}",
                    os.path.join(file["source_path"], file["source_name"]),
                    content_type=file['content_type'] if 'content_type' in file else None
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

    try:
        file_1 = s3.head_object(
            Bucket=MINIO_BUCKET, Key=f"{AIRFLOW_ENV}/{file_path_1}{file_name_1}"
        )
        file_2 = s3.head_object(
            Bucket=MINIO_BUCKET, Key=f"{AIRFLOW_ENV}/{file_path_2}{file_name_2}"
        )
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            print("errorrrrrr")
            return

    print(f"Hash file 1 : {file_1['ETag']}")
    print(f"Hash file 2 : {file_2['ETag']}")
    print(bool(file_1["ETag"] == file_2["ETag"]))

    return bool(file_1["ETag"] == file_2["ETag"])


def get_files_from_prefix(
    MINIO_URL: str,
    MINIO_BUCKET: str,
    MINIO_USER: str,
    MINIO_PASSWORD: str,
    prefix: str,
):
    """Retrieve only the list of files in a Minio pattern

    Args:
        MINIO_URL (str): Minio endpoint
        MINIO_BUCKET (str): bucket
        MINIO_USER (str): user
        MINIO_PASSWORD (str): password
        prefix: (str): prefix to search files

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
        list_objects = []
        objects = client.list_objects(MINIO_BUCKET, prefix=f"{AIRFLOW_ENV}/{prefix}")
        for obj in objects:
            print(obj.object_name)
            list_objects.append(obj.object_name.replace(f"{AIRFLOW_ENV}/", ""))
        return list_objects
    else:
        raise Exception(f"Bucket {MINIO_BUCKET} does not exists")


def copy_object(
    MINIO_URL: str,
    MINIO_USER: str,
    MINIO_PASSWORD: str,
    MINIO_BUCKET_SOURCE: str,
    MINIO_BUCKET_TARGET: str,
    path_source: str,
    path_target: str,
    remove_source_file: bool,
):
    """Copy and paste file to another folder.

    Args:
        MINIO_URL (str): Minio endpoint
        MINIO_USER (str): user
        MINIO_PASSWORD (str): password
        MINIO_BUCKET_SOURCE (str): bucket source
        MINIO_BUCKET_TARGET (str): bucket target
        path_source: path of source file
        path_target: path of target file
        remove_source_file: (bool): remove or not source file

    Raises:
        Exception: _description_
    """
    client = Minio(
        MINIO_URL,
        access_key=MINIO_USER,
        secret_key=MINIO_PASSWORD,
        secure=True,
    )
    if (
        client.bucket_exists(MINIO_BUCKET_SOURCE)
        and client.bucket_exists(MINIO_BUCKET_TARGET)
    ):
        # copy an object from a bucket to another.
        print(MINIO_BUCKET_SOURCE)
        print(f"{AIRFLOW_ENV}/{path_source}")
        client.copy_object(
            MINIO_BUCKET_SOURCE,
            f"{AIRFLOW_ENV}/{path_target}",
            CopySource(MINIO_BUCKET_TARGET, f"{AIRFLOW_ENV}/{path_source}"),
        )
        if remove_source_file:
            client.remove_object(MINIO_BUCKET_SOURCE, f"{AIRFLOW_ENV}/{path_source}")
    else:
        raise Exception("One Bucket does not exists")
