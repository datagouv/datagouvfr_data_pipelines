import csv
import glob
import hashlib
import os
from typing import Any

import magic
from datagouvfr_data_pipelines.utils.download import download_files


class File:
    def __init__(
        self,
        dest_path: str | None = None,
        dest_name: str | None = None,
        source_path: str | None = None,
        source_name: str | None = None,
        url: str | None = None,
        content_type: str | None = None,
        column_order: str | None = None,
        remote_source: bool = False,
    ) -> None:
        self.url = url
        self.column_order = column_order
        self.content_type = content_type
        if dest_path and dest_name:
            self.dest_path = self.add_trailing_slash(dest_path)
            self.dest_name = dest_name
            self.full_dest_path = self.dest_path + self.dest_name
        if source_path and source_name:
            self.source_path = self.add_trailing_slash(source_path)
            self.source_name = source_name
            self.full_source_path = self.source_path + self.source_name
            if not remote_source:
                self.assert_file_exists(self.source_path, self.source_name)
                if not content_type:
                    self.content_type = magic.from_file(
                        self.source_path + self.source_name,
                        mime=True,
                    )

    def __getitem__(self, item: str):
        return getattr(self, item)

    def get(self, item: str, default=None):
        return getattr(self, item) if hasattr(self, item) else default

    def delete(self):
        os.remove(self.full_source_path)

    @staticmethod
    def add_trailing_slash(path: str) -> str:
        return path if path.endswith("/") else path + "/"

    @staticmethod
    def assert_file_exists(path: str, file_name: str) -> None:
        if not os.path.isfile(path + file_name):
            raise FileNotFoundError(f"{path + file_name} doesn't exist")

    def download(self, **kwargs):
        if not all((self.url, self.dest_path, self.dest_name)):
            raise ValueError("Downloading requires url, dest_path and dest_name")
        download_files(list_urls=[self], **kwargs)


def save_list_of_dict_to_csv(
    records_list: list[dict[str, Any]], destination_file: str
) -> None:
    """
    Appends a list of dictionaries to a CSV file.

    Creates the CSV if it does not exist yet.
    All dictionaries from the list must share the same keys or a ValueError exception is raised.

    Args:
        records_list (list[dict[str, Any]]): A list of dictionaries where each one
            will represent a row of values in the CSV file. Keys are headers.
        destination_file (str): The path to the destination CSV file.
    """
    file_exists = os.path.isfile(destination_file)
    with open(destination_file, "a", newline="") as csv_file:
        csv_writer = csv.DictWriter(csv_file, records_list[0].keys(), delimiter=";")
        if not file_exists:
            csv_writer.writeheader()
        for row in records_list:
            csv_writer.writerow(row)


def remove_files_from_directory(directory: str) -> None:
    os.makedirs(directory, exist_ok=True)
    for f in glob.glob(f"{directory}/*"):
        os.remove(f)


def compute_checksum_from_file(filename: str, _type: str = "sha256") -> str:
    """Compute sha in blocks"""
    shasum = getattr(hashlib, _type)()
    with open(filename, "rb") as f:
        block = f.read(2**16)
        while len(block) != 0:
            shasum.update(block)
            block = f.read(2**16)
    return shasum.hexdigest()
