import logging
from io import StringIO

import paramiko
from airflow.hooks.base import BaseHook

from datagouvfr_data_pipelines.config import SECRET_SFTP_HOST
from datagouvfr_data_pipelines.utils.retry import simple_connection_retry


class SFTPClient:
    @simple_connection_retry
    def __init__(
        self,
        conn_name: str,
        user: str,
        key_type: str = "RSA",
        host: str = SECRET_SFTP_HOST,
        accept_unknown_keys: bool = True,
    ):
        if key_type not in ["RSA", "DSS", "ECDSA", "Ed25519"]:
            raise ValueError(f"{key_type} is not a valid key type")
        conn_infos = BaseHook.get_connection(conn_name).extra_dejson
        if "private_key" in conn_infos:
            private_key = getattr(paramiko, key_type + "Key").from_private_key(
                StringIO(conn_infos["private_key"])
            )
        elif "key_file" in conn_infos:
            private_key = getattr(paramiko, key_type + "Key").from_private_key_file(
                conn_infos["key_file"]
            )
        else:
            raise KeyError("None of the required keys could be found")
        self.ssh = paramiko.SSHClient()
        if accept_unknown_keys:
            self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh.connect(
            host,
            username=user,
            pkey=private_key,
        )
        self.sftp = self.ssh.open_sftp()

    def list_files_in_directory(self, directory: str):
        return self.sftp.listdir(directory)

    @simple_connection_retry
    def download_file(self, remote_file_path: str, local_file_path: str):
        self.sftp.get(remote_file_path, local_file_path)
        logging.info(f"⬇️ {local_file_path} successfully dowloaded")

    def delete_file(self, remote_file_path: str):
        self.sftp.remove(remote_file_path)
        logging.info(f"🔥 {remote_file_path} successfully deleted")

