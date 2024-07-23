import requests
from requests.auth import HTTPBasicAuth
from typing import List, Optional, TypedDict
import os


class Url(TypedDict):
    url: str
    dest_path: str
    dest_name: str


def download_files(
    list_urls: List[Url],
    auth_user: Optional[str] = None,
    auth_password: Optional[str] = None,
):
    """Retrieve list of files from urls

    Args:
        list_urls (List[File]): List of Dictionnaries containing for each
        `url` `dest_path` and `dest_name` : url and the file properties chosen for destination ;
        `auth_user` and `auth_password` : Optional authentication ;

    Raises:
        Exception: _description_
    """
    if auth_user and auth_password:
        auth = HTTPBasicAuth(auth_user, auth_password)
    else:
        auth = None

    for url in list_urls:
        os.makedirs(url['dest_path'], exist_ok=True)
        with requests.get(
            url["url"],
            auth=auth,
            stream=True,
        ) as r:
            r.raise_for_status()
            with open(f"{url['dest_path']}{url['dest_name']}", "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
