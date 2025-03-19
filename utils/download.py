# import requests
# from requests.auth import HTTPBasicAuth
from typing import List, Optional, TYPE_CHECKING
from pathlib import Path
import aiohttp
import asyncio

if TYPE_CHECKING:
    # to avoid circular imports
    from datagouvfr_data_pipelines.utils.filesystem import File
from datagouvfr_data_pipelines.utils.retry import simple_connection_retry


@simple_connection_retry
async def download_file(session, url, dest_path, dest_name, auth=None):
    Path(dest_path).mkdir(parents=True, exist_ok=True)
    async with session.get(url, auth=auth) as response:
        response.raise_for_status()
        with open(f"{dest_path}{dest_name}", "wb") as f:
            async for chunk in response.content.iter_chunked(8192):
                f.write(chunk)


async def async_download_files(
    list_urls: List["File"],
    auth_user: Optional[str] = None,
    auth_password: Optional[str] = None,
    timeout: int = 300,
):
    """Retrieve list of files from urls

    Args:
        list_urls (List[File]): List of Dictionaries containing for each
        `url` `dest_path` and `dest_name` : url and the file properties chosen for destination ;
        `auth_user` and `auth_password` : Optional authentication ;

    Raises:
        Exception: _description_
    """
    auth = None
    if auth_user and auth_password:
        auth = aiohttp.BasicAuth(auth_user, auth_password)

    timeout = aiohttp.ClientTimeout(total=timeout)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        tasks = []
        for url in list_urls:
            task = download_file(session, url["url"], url["dest_path"], url["dest_name"], auth)
            tasks.append(task)
        await asyncio.gather(*tasks)


# so that we keep the main function synchronous
def download_files(
    list_urls: List["File"],
    auth_user: Optional[str] = None,
    auth_password: Optional[str] = None,
    timeout: int = 300,
):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(async_download_files(list_urls, auth_user, auth_password, timeout))


# def download_files(
#     list_urls: List["File"],
#     auth_user: Optional[str] = None,
#     auth_password: Optional[str] = None,
# ):
#     """Retrieve list of files from urls

#     Args:
#         list_urls (List[File]): List of Dictionnaries containing for each
#         `url` `dest_path` and `dest_name` : url and the file properties chosen for destination ;
#         `auth_user` and `auth_password` : Optional authentication ;

#     Raises:
#         Exception: _description_
#     """
#     if auth_user and auth_password:
#         auth = HTTPBasicAuth(auth_user, auth_password)
#     else:
#         auth = None

#     for url in list_urls:
#         if not url["dest_path"].endswith("/"):
#             url["dest_path"] = url["dest_path"] + "/"
#         Path(url["dest_path"]).mkdir(parents=True, exist_ok=True)
#         with requests.get(
#             url["url"],
#             auth=auth,
#             stream=True,
#         ) as r:
#             r.raise_for_status()
#             with open(f"{url['dest_path']}{url['dest_name']}", "wb") as f:
#                 for chunk in r.iter_content(chunk_size=8192):
#                     f.write(chunk)
