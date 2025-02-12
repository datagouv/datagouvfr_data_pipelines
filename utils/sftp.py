import logging
from io import StringIO

import paramiko
from airflow.hooks.base import BaseHook

from datagouvfr_data_pipelines.config import (
    SECRET_SFTP_HOST
)


class SFTPClient:

    def __init__(
        self,
        conn_name: str,
        user: str,
        host: str = SECRET_SFTP_HOST,
        accept_unknown_keys: bool = True,
    ):
        rsa_key = BaseHook.get_connection(conn_name).extra_dejson["private_key"]
        self.ssh = paramiko.SSHClient()
        if accept_unknown_keys:
            self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh.connect(
            host,
            username=user,
            pkey=paramiko.RSAKey.from_private_key(StringIO(rsa_key)),
        )
        self.sftp = self.ssh.open_sftp()

    def list_files_in_directory(self, directory: str):
        return self.sftp.listdir(directory)

    def download_file(self, remote_file_path: str, local_file_path: str):
        self.sftp.get(remote_file_path, local_file_path)
        logging.info(f"⬇️ {local_file_path} successfully dowloaded")

    def delete_file(self, remote_file_path: str):
        self.sftp.remove(remote_file_path)
        logging.info(f"🔥 {remote_file_path} successfully deleted")
