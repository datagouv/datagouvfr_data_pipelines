import logging
from io import StringIO

from airflow.sdk.bases.hook import BaseHook
import paramiko
from datagouvfr_data_pipelines.utils.retry import (
    _simple_connection_retry,
    simple_connection_retry,
)
from tenacity import retry_if_not_exception_type

retry_except_filenotfound = _simple_connection_retry(
    retry=retry_if_not_exception_type(FileNotFoundError)
)


class SFTPClient:
    @simple_connection_retry
    def __init__(
        self,
        conn_name: str,
        key_type: str = "RSA",
        accept_unknown_keys: bool = True,
    ):
        if key_type not in ["RSA", "DSS", "ECDSA", "Ed25519"]:
            raise ValueError(f"{key_type} is not a valid key type")
        conn = BaseHook.get_connection(conn_name)
        extras = conn.extra_dejson
        if "private_key" in extras:
            private_key = getattr(paramiko, key_type + "Key").from_private_key(
                StringIO(extras["private_key"])
            )
        elif "key_file" in extras:
            private_key = getattr(paramiko, key_type + "Key").from_private_key_file(
                extras["key_file"]
            )
        else:
            raise KeyError("None of the required keys could be found")
        self.ssh = paramiko.SSHClient()
        if accept_unknown_keys:
            self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh.connect(
            conn.host,
            username=conn.login,
            pkey=private_key,
        )
        self.sftp = self.ssh.open_sftp()

    def list_files_in_directory(self, directory: str):
        return self.sftp.listdir(directory)

    @retry_except_filenotfound
    def download_file(self, remote_file_path: str, local_file_path: str):
        self.sftp.get(remote_file_path, local_file_path)
        logging.info(f"⬇️ {local_file_path} successfully dowloaded")

    def delete_file(self, remote_file_path: str):
        self.sftp.remove(remote_file_path)
        logging.info(f"🔥 {remote_file_path} successfully deleted")
