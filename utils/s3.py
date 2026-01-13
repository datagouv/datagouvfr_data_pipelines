import io
import json
import logging
import os
from typing import Iterator

import boto3
from botocore.config import Config
import requests

from datagouvfr_data_pipelines.config import (
    AIRFLOW_ENV,
    MINIO_URL,
    # S3_URL,
    SECRET_S3_DATA_PIPELINE_USER,
    SECRET_S3_DATA_PIPELINE_PASSWORD,
)
from datagouvfr_data_pipelines.utils.filesystem import File
from datagouvfr_data_pipelines.utils.retry import simple_connection_retry


class S3Client:
    def __init__(
        self,
        bucket: str,
        user: str = SECRET_S3_DATA_PIPELINE_USER,
        pwd: str = SECRET_S3_DATA_PIPELINE_PASSWORD,
        login: bool = True,
        config_kwargs: dict | None = None,
        s3_url: str = MINIO_URL,  # to be removed when migration is complete
    ):
        self.url = s3_url
        self.resource = boto3.resource(
            "s3",
            endpoint_url="https://" + self.url,
            aws_access_key_id=user if login else None,
            aws_secret_access_key=pwd if login else None,
            config=Config(**config_kwargs) if config_kwargs else None,
        )
        if bucket not in [b.name for b in self.resource.buckets.all()]:
            raise ValueError(f"Bucket '{bucket}' does not exist.")
        self.client = self.resource.meta.client
        self.bucket = self.resource.Bucket(bucket)

    @simple_connection_retry
    def does_file_exist_in_bucket(self, file_path: str) -> bool:
        try:
            self.bucket.Object(file_path).get()
            return True
        except self.client.exceptions.NoSuchKey:
            return False

    @simple_connection_retry
    def send_file(
        self,
        file: File,
        ignore_airflow_env: bool = False,
        burn_after_sending: bool = False,
    ) -> None:
        """Send a file to a S3 bucket"""
        dest_path = file.full_dest_path
        if not ignore_airflow_env:
            dest_path = f"{AIRFLOW_ENV}/{dest_path}"
        logging.info("⬆️ Sending " + file.full_source_path)
        logging.info(f"to {self.bucket.name}/{dest_path}")
        self.bucket.upload_file(
            file.full_source_path,
            dest_path,
            ExtraArgs={"ContentType": file.content_type},
        )
        if burn_after_sending:
            file.delete()

    def send_files(
        self,
        list_files: list[File],
        ignore_airflow_env: bool = False,
        burn_after_sending: bool = False,
    ) -> None:
        """Send list of files to a S3 bucket"""
        for file in list_files:
            self.send_file(file, ignore_airflow_env, burn_after_sending)

    @simple_connection_retry
    def download_files(
        self,
        list_files: list[File],
        ignore_airflow_env: bool = False,
    ) -> None:
        """Retrieve list of files from a S3 bucket"""
        for file in list_files:
            source_path = file.full_source_path
            if not ignore_airflow_env:
                source_path = f"{AIRFLOW_ENV}/{source_path}"
            logging.info(f"Downloading {source_path} to {file.full_dest_path}")
            if not self.does_file_exist_in_bucket(source_path):
                raise FileNotFoundError(
                    f"File '{source_path}' does not exist in bucket '{self.bucket.name}'"
                )
            self.client.download_file(
                self.bucket.name,
                self.bucket.Object(source_path),
                file.full_dest_path,
            )

    @simple_connection_retry
    def get_file_content(
        self,
        file_path: str,
        encoding: str = "utf-8",
    ) -> str:
        """Return the content of a file from a S3 bucket as a string."""
        return self.bucket.Object(file_path).get()["Body"].read().decode(encoding)

    @simple_connection_retry
    def get_files_from_prefix(
        self,
        prefix: str,
        ignore_airflow_env: bool = False,
        as_objects: bool = False,
    ) -> Iterator:
        """Retrieve the list of the files which keys start with the prefix in a S3 bucket"""
        if not ignore_airflow_env:
            prefix = f"{AIRFLOW_ENV}/{prefix}"
        # we use a paginator to be able to retrieve more than 1000 results from list_objects_v2
        paginator = self.client.get_paginator("list_objects_v2")
        pages = paginator.paginate(
            Bucket=self.bucket.name,
            Prefix=prefix,
        )
        for page in pages:
            for obj in page["Contents"]:
                if as_objects:
                    # use this if you want to access more than the name and file of the object (content_type...)
                    yield self.bucket.Object(obj["Key"])
                else:
                    yield obj["Key"]

    @simple_connection_retry
    def get_folders_from_prefix(
        self,
        prefix: str,
        ignore_airflow_env: bool = False,
    ) -> list[str]:
        """Retrieve the list of the folders which keys start with the prefix in a S3 bucket"""
        if not ignore_airflow_env:
            prefix = f"{AIRFLOW_ENV}/{prefix}"
        # we use a paginator to be able to retrieve more than 1000 results from list_objects_v2
        paginator = self.client.get_paginator("list_objects_v2")
        pages = paginator.paginate(
            Bucket=self.bucket.name,
            Prefix=prefix,
            Delimiter="/",
        )
        folders: list[str] = []
        for page in pages:
            for cp in page.get("CommonPrefixes", []):
                folders.append(cp.get("Prefix"))
        return folders

    @simple_connection_retry
    def are_files_identical(
        self,
        file_1: File,
        file_2: File,
    ) -> bool:
        """Compare two files on a S3 bucket, returns whether they are identical"""

        def get_content_length(response: dict) -> int:
            return (
                response.get("ResponseMetadata", {})
                .get("HTTPHeaders", {})
                .get("content-length", None)
            )

        logging.info(f"File 1: {file_1.full_source_path}")
        logging.info(f"File 2: {file_2.full_source_path}")
        f1: dict = self.client.head_object(
            Bucket=self.bucket.name, Key=file_1.full_source_path
        )
        f2: dict = self.client.head_object(
            Bucket=self.bucket.name, Key=file_2.full_source_path
        )
        logging.info(f"ETag file 1 : {f1['ETag']}")
        logging.info(f"ETag file 2 : {f2['ETag']}")
        if file_1["ETag"] == file_2["ETag"]:
            logging.info("Identical ETags, returning True")
            return True

        # upload process (single vs multi part) can lead to different ETags for identical files
        # so we check content-length too
        cl1 = get_content_length(f1)
        cl2 = get_content_length(f2)
        logging.info(f"content-length file 1 : {cl1}")
        logging.info(f"content-length file 2 : {cl2}")
        logging.info(f"Are content-lengths equal: {cl1 == cl2}")

        return cl1 is not None and cl1 == cl2

    @simple_connection_retry
    def copy_object(
        self,
        path_source: str,
        path_target: str,
        s3_bucket_source: str | None = None,
        s3_bucket_target: str | None = None,
        remove_source_file: bool = False,
    ) -> None:
        """Copy and paste file to another folder, potentially from one bucket to another if specified."""
        if not s3_bucket_source:
            s3_bucket_source = self.bucket.name
        if not s3_bucket_target:
            s3_bucket_target = self.bucket.name
        for bucket in [s3_bucket_source, s3_bucket_target]:
            if bucket not in [b for b in self.resource.buckets.all()]:
                raise ValueError(
                    f"Bucket '{bucket}' does not exist, or the current user does not have access"
                )

        logging.info(
            f"{'Moving' if remove_source_file else 'Copying'} {s3_bucket_source}/{path_source}"
        )
        self.client.copy_object(
            Bucket=s3_bucket_target,
            Key=path_target,
            CopySource={"Bucket": s3_bucket_source, "Key": path_source},
        )
        if remove_source_file:
            self.client.delete_object(Bucket=s3_bucket_source, Key=path_source)
        logging.info(f"to {s3_bucket_source}/{path_target}")

    def copy_many_objects(
        self,
        obj_source_paths: list[str],  # TODO: use a list of Files
        target_directory: str,
        s3_bucket_source: str | None = None,
        s3_bucket_target: str | None = None,
        remove_source_file: bool = False,
    ) -> list[str]:
        """
        Copy multiple objects from a source folder to a target folder in S3.
        Ensure credentials allow to access both buckets.

        Args:
            obj_source_paths (list[str]): List of the objects full paths to be copied.
            target_path (str): The target directory where the objects will be copied.
            s3_bucket_source (str | None): The source S3 bucket name. Defaults to the bucket specified at init.
            s3_bucket_target (str | None): The target S3 bucket name. Defaults to the bucket specified at init.
            remove_source_file (bool): If True, removes the source files after copying. Defaults to False.

        Returns:
            list[str]: List of the new objects paths.
        """
        files_destination_path = []
        for source_path in obj_source_paths:
            source_file = os.path.basename(source_path)
            target_path = target_directory + source_file
            self.copy_object(
                path_source=source_path,
                path_target=target_path,
                s3_bucket_source=s3_bucket_source,
                s3_bucket_target=s3_bucket_target,
                remove_source_file=remove_source_file,
            )
            files_destination_path.append(target_path)
        return files_destination_path

    @simple_connection_retry
    def delete_file(
        self,
        file_path: str,
    ) -> None:
        """/!\ USE WITH CAUTION"""
        if not self.does_file_exist_in_bucket(file_path):
            raise ValueError(
                f"File '{file_path}' does not exist in bucket '{self.bucket.name}'"
            )
        self.client.delete_object(Bucket=self.bucket.name, Key=file_path)
        logging.info(f"🔥 '{file_path}' successfully deleted.")

    @simple_connection_retry
    def delete_files_from_prefix(
        self,
        prefix: str,
    ) -> None:
        for obj in self.get_files_from_prefix(
            prefix, ignore_airflow_env=True, as_objects=True
        ):
            obj.delete()
            logging.info(f"🔥 '{obj.key}' successfully deleted.")

    def get_file_url(
        self,
        file_path: str,
    ) -> str:
        return f"https://{self.url}/{self.bucket.name}/{file_path}"

    @simple_connection_retry
    def get_all_files_names_and_sizes_from_parent_folder(
        self,
        folder: str,
    ) -> dict[str, int]:
        """
        returns a dict of {"file_name": file_size, ...} for all files in the folder
        """
        return {
            obj.key: obj.content_length
            for obj in self.get_files_from_prefix(
                prefix=folder, ignore_airflow_env=True, as_objects=True
            )
        }

    @simple_connection_retry
    def send_dict_as_file(
        self,
        dict_to_send: dict,
        file_path: str,
        encoding: str = "utf-8",
    ) -> None:
        """Send a dictionary to the specified json file path."""
        raw_data = io.BytesIO(json.dumps(dict_to_send, indent=2).encode(encoding))
        self.bucket.put_object(Key=file_path, Body=raw_data)

    @simple_connection_retry
    def send_from_url(
        self,
        url: str,
        destination_file_path: str,
        session: requests.Session | None = None,
    ) -> None:
        # to upload a file from an URL without having to download, save and send
        _req = session or requests
        with _req.get(url, stream=True) as response:
            response.raise_for_status()
            logging.info("⬆️ Stream-sending " + url)
            logging.info(f"to {self.bucket}/{destination_file_path}")
            self.bucket.upload_fileobj(Key=destination_file_path, Fileobj=response.raw)
