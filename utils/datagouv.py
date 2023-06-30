import dateutil
from typing import List, Optional, TypedDict
import requests
import os
import numpy as np
from datagouvfr_data_pipelines.config import AIRFLOW_ENV

if AIRFLOW_ENV == "dev":
    DATAGOUV_URL = "https://demo.data.gouv.fr"
    ORGA_REFERENCE = "63e3ae4082ddaa6c806b8417"
if AIRFLOW_ENV == "prod":
    DATAGOUV_URL = "https://www.data.gouv.fr"
    ORGA_REFERENCE = "646b7187b50b2a93b1ae3d45"


class File(TypedDict):
    dest_path: str
    dest_name: str


def create_dataset(
    api_key: str,
    payload: TypedDict,
):
    """Create a dataset in data.gouv.fr

    Args:
        api_key (str): API key from data.gouv.fr
        payload (TypedDict): payload for dataset containing at minimum title

    Returns:
        json: return API result in a dictionnary
    """
    headers = {
        "X-API-KEY": api_key,
    }
    r = requests.post(
        f"{DATAGOUV_URL}/api/1/datasets/", json=payload, headers=headers
    )
    r.raise_for_status()
    return r.json()


def get_resource(
    resource_id: str,
    file_to_store: File,
):
    """Download a resource in data.gouv.fr

    Args:
        resource_id (str): ID of the resource
        file_to_store (File): Dictionnary containing `dest_path` and
        `dest_name` where to store downloaded resource

    """
    with requests.get(
        f"{DATAGOUV_URL}/fr/datasets/r/{resource_id}", stream=True
    ) as r:
        r.raise_for_status()
        os.makedirs(os.path.dirname(file_to_store["dest_path"]), exist_ok=True)
        with open(
            f"{file_to_store['dest_path']}{file_to_store['dest_name']}", "wb"
        ) as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)


def post_resource(
    api_key: str,
    file_to_upload: File,
    dataset_id: str,
    resource_id: Optional[str] = None,
    resource_payload: Optional[dict] = None,
):
    """Upload a resource in data.gouv.fr

    Args:
        api_key (str): API key from data.gouv.fr
        file_to_upload (File): Dictionnary containing `dest_path` and
        `dest_name` where resource to upload is stored
        dataset_id (str): ID of the dataset where to store resource
        resource_id (Optional[str], optional): ID of the resource where
        to upload file. If it is a new resource, let it to None.
        Defaults to None.
        resource_payload (Optional[dict], optional): payload to update the
        resource's metadata, only if resource_id is given
        Defaults to None.

    Returns:
        json: return API result in a dictionnary
    """
    headers = {
        "X-API-KEY": api_key,
    }
    files = {
        "file": open(
            f"{file_to_upload['dest_path']}{file_to_upload['dest_name']}",
            "rb",
        )
    }
    if resource_id:
        url = f"{DATAGOUV_URL}/api/1/datasets/{dataset_id}/resources/{resource_id}/upload/"
    else:
        url = f"{DATAGOUV_URL}/api/1/datasets/{dataset_id}/upload/"
    r = requests.post(url, files=files, headers=headers)
    r.raise_for_status()
    if resource_id and resource_payload:
        r_put = requests.put(url.replace('upload/', ''), json=resource_payload, headers=headers)
        r_put.raise_for_status()
    return r.json()


def post_remote_resource(
    api_key: str,
    dataset_id: str,
    title: str,
    format: str,
    remote_url: str,
    filesize: int,
    type: str = "main",
    schema: dict = {},
    description: str = "",
    resource_id: Optional[str] = None,
):
    """Create a post in data.gouv.fr

    Args:
        api_key (str): API key from data.gouv.fr
        dataset_id (str): id of the dataset
        title (str): resource title
        format (str): resource format
        remote_url (str): resource distant URL
        filesize (int): resource size (bytes)
        type (str): type of resource
        schema (str): schema of the resource (if relevant)
        description (str): resource description
        resource_id (str): resource id (if modifying an existing resource)

    Returns:
       json: return API result in a dictionnary containing metadatas
    """
    headers = {
        "X-API-KEY": api_key,
    }
    payload = {
        'title': title,
        'description': description,
        'url': remote_url,
        'type': type,
        'filetype': 'remote',
        'format': format,
        'schema': schema,
        'filesize': filesize
    }
    if resource_id:
        url = f"{DATAGOUV_URL}/api/1/datasets/{dataset_id}/resources/{resource_id}/"
        print(f"Putting '{title}' at {url}")
        r = requests.put(
            url,
            json=payload,
            headers=headers
        )
    else:
        url = f"{DATAGOUV_URL}/api/1/datasets/{dataset_id}/resources/"
        print(f"Posting '{title}' at {url}")
        r = requests.post(
            url,
            json=payload,
            headers=headers
        )
    r.raise_for_status()
    return r.json()


def delete_dataset_or_resource(
    api_key: str,
    dataset_id: str,
    resource_id: Optional[str] = None,
):
    """Delete a dataset or a resource in data.gouv.fr

    Args:
        api_key (str): API key from data.gouv.fr
        dataset_id (str): ID of the dataset
        resource_id (Optional[str], optional): ID of the resource.
        If resource is None, the dataset will be deleted. Else only the resource.
        Defaults to None.

    Returns:
        json: return API result in a dictionnary
    """
    headers = {
        "X-API-KEY": api_key,
    }
    if resource_id:
        url = f"{DATAGOUV_URL}/api/1/datasets/{dataset_id}/resources/{resource_id}/"
    else:
        url = f"{DATAGOUV_URL}/api/1/datasets/{dataset_id}/"

    r = requests.delete(url, headers=headers)
    if r.status_code == 204:
        return {"message": "ok"}
    else:
        return r.json()


def get_dataset_or_resource_metadata(
    dataset_id: str,
    resource_id: Optional[str] = None,
):
    """Retrieve dataset or resource metadata from data.gouv.fr

    Args:
        dataset_id (str): ID ot the dataset
        resource_id (Optional[str], optional): ID of the resource.
        If resource_id is None, it will be dataset metadata which will be
        returned. Else it will be resouce_id's ones. Defaults to None.

    Returns:
       json: return API result in a dictionnary containing metadatas
    """
    if resource_id:
        url = f"{DATAGOUV_URL}/api/1/datasets/{dataset_id}/resources/{resource_id}/"
    else:
        url = f"{DATAGOUV_URL}/api/1/datasets/{dataset_id}"
    r = requests.get(url)
    if r.status_code == 200:
        return r.json()
    else:
        return {"message": "error", "status": r.status_code}


def get_dataset_from_resource_id(
    resource_id: str,
):
    """Return dataset ID from resource ID from data.gouv.fr

    Args:
        resource_id (str): ID of a resource

    Returns:
       json: return API result in a dictionnary containing metadatas
    """
    url = f"{DATAGOUV_URL}/api/2/datasets/resources/{resource_id}/"
    r = requests.get(url)
    if r.status_code == 200:
        return r.json()


def update_dataset_or_resource_metadata(
    api_key: str,
    payload: TypedDict,
    dataset_id: str,
    resource_id: Optional[str] = None,
):
    """Update metadata to dataset or resource in data.gouv.fr

    Args:
        api_key (str): API key from data.gouv.fr
        payload (TypedDict): metadata to upload.
        dataset_id (str): ID of the dataset
        resource_id (Optional[str], optional): ID of the resource.
        If resource_id is None, it will be dataset metadata which will be
        updated. Else it will be resouce_id's ones. Defaults to None.

    Returns:
       json: return API result in a dictionnary containing metadatas
    """
    headers = {
        "X-API-KEY": api_key,
    }
    if resource_id:
        url = f"{DATAGOUV_URL}/api/1/datasets/{dataset_id}/resources/{resource_id}/"
    else:
        url = f"{DATAGOUV_URL}/api/1/datasets/{dataset_id}/"

    r = requests.put(url, json=payload, headers=headers)
    assert r.status_code == 200
    return r.json()


def update_dataset_or_resource_extras(
    api_key: str,
    payload: TypedDict,
    dataset_id: str,
    resource_id: Optional[str] = None,
):
    """Update specific extras to a dataset or resource in data.gouv.fr

    Args:
        api_key (str): API key from data.gouv.fr
        payload (TypedDict): Payload contaning extra and its value
        dataset_id (str): ID of the dataset.
        resource_id (Optional[str], optional): ID of the resource.
        If resource_id is None, it will be dataset extras which will be
        updated. Else it will be resouce_id's ones. Defaults to None.

    Returns:
       json: return API result in a dictionnary containing metadatas
    """
    headers = {
        "X-API-KEY": api_key,
    }
    if resource_id:
        url = f"{DATAGOUV_URL}/api/2/datasets/{dataset_id}/resources/{resource_id}/extras/"
    else:
        url = f"{DATAGOUV_URL}/api/2/datasets/{dataset_id}/extras/"

    r = requests.put(url, json=payload, headers=headers)
    r.raise_for_status()
    return r.json()


def delete_dataset_or_resource_extras(
    api_key: str,
    extras: List,
    dataset_id: str,
    resource_id: Optional[str] = None,
):
    """Delete extras from a dataset or resoruce in data.gouv.fr

    Args:
        api_key (str): API key from data.gouv.fr
        extras (List): List of extras to delete.
        dataset_id (str): ID of the dataset.
        resource_id (Optional[str], optional): ID of the resource.
        If resource_id is None, it will be dataset extras which will be
        deleted. Else it will be resouce_id's ones. Defaults to None.

    Returns:
       json: return API result in a dictionnary containing metadatas
    """
    headers = {
        "X-API-KEY": api_key,
    }
    if resource_id:
        url = f"{DATAGOUV_URL}/api/2/datasets/{dataset_id}/resources/{resource_id}/extras/"
    else:
        url = f"{DATAGOUV_URL}/api/2/datasets/{dataset_id}/extras/"
    r = requests.delete(url, json=extras, headers=headers)
    if r.status_code == 204:
        return {"message": "ok"}
    else:
        return r.json()


def create_post(
    api_key: str,
    name: str,
    headline: str,
    content: str,
    body_type: str,
    tags: Optional[List] = [],
):
    """Create a post in data.gouv.fr

    Args:
        api_key (str): API key from data.gouv.fr
        name (str): name of post.
        headline (str): headline of post
        content (str) : content of post
        body_type (str) : body type of post (html or markdown)
        tags: Option list of tags for post

    Returns:
       json: return API result in a dictionnary containing metadatas
    """
    headers = {
        "X-API-KEY": api_key,
    }

    r = requests.post(
        f"{DATAGOUV_URL}/api/1/posts/",
        json={
            'name': name,
            'headline': headline,
            'content': content,
            'body_type': body_type,
            'tags': tags
        },
        headers=headers
    )
    assert r.status_code == 201
    return r.json()


def get_data(endpoint, page, sort):
    r = requests.get(
        f"{DATAGOUV_URL}/api/1/{endpoint}?page_size=100&sort={sort}&page={page}"
    )
    r.raise_for_status()
    return r.json().get('data', [])


def get_last_items(endpoint, start_date, end_date=None, date_key='created_at', sort_key='-created'):

    got_everything = False
    intermediary_result = []
    results = []
    page = 1

    while not got_everything:
        data = get_data(endpoint, page, sort_key)
        for d in data:
            created = dateutil.parser.parse(d[date_key])
            got_everything = (created.timestamp() < start_date.timestamp())
            if not got_everything:
                intermediary_result.append(d)
            else:
                break
        if not data or got_everything:
            break
        else:
            page += 1
    if end_date:
        for d in intermediary_result:
            created = dateutil.parser.parse(d[date_key])
            if created.timestamp() < end_date.timestamp():
                results.append(d)
    else:
        results = intermediary_result
    return results


def post_remote_communautary_resource(
    api_key: str,
    dataset_id: str,
    title: str,
    format: str,
    remote_url: str,
    organisation_publication_id: str,
    filesize: int,
    type: str = "main",
    schema: dict = {},
    description: str = ""
):
    """Post a remote communautary resource on data.gouv.fr

    Args:
        api_key (str): API key from data.gouv.fr
        dataset_id (str): id of the dataset
        title (str): resource title
        format (str): resource format
        remote_url (str): resource distant URL
        organisation_publication_id (str): organization with which to publish
        filesize (int): resource size (bytes)
        type (str): type of resource
        schema (str): schema of the resource (if relevant)
        description (str): resource description

    Returns:
       json: return API result in a dictionnary containing metadatas
    """
    headers = {
        "X-API-KEY": api_key,
    }
    community_resource_url = f"{DATAGOUV_URL}/api/1/datasets/community_resources"
    dataset_link = f"{DATAGOUV_URL}/fr/datasets/{dataset_id}/#/community-resources"

    # Check if resource already exists
    data = requests.get(
        community_resource_url,
        {"dataset": dataset_id}
    ).json()["data"]
    resource_exists = remote_url in [d.get('url', '') for d in data]
    payload = {
        "dataset": {
            "id": dataset_id
        },
        "description": description,
        "filetype": "remote",
        "filesize": filesize,
        "format": format,
        "organization": {
            "id": organisation_publication_id
        },
        "schema": schema,
        "title": title,
        "type": type,
        "url": remote_url
    }
    print("Payload content:\n", payload)
    if resource_exists:
        print(f"Updating resource at {dataset_link} from {remote_url}")
        # Update resource
        idx = np.argwhere(
            np.array([d.get('url', '') for d in data]) == remote_url
        )[0][0]
        resource_id = data[idx]['id']
        refined_url = community_resource_url + f"/{resource_id}"

        r = requests.put(
            refined_url,
            json=payload,
            headers=headers
        )

    else:
        print(f"Creating resource at {dataset_link} from {remote_url}")
        # Create resource
        r = requests.post(
            community_resource_url,
            json=payload,
            headers=headers
        )
    r.raise_for_status()
    return r.json()


def get_all_from_api_query(base_query):
    # /!\ only for paginated endpoints
    all_you_want = []
    r = requests.get(base_query)
    r.raise_for_status()
    data = r.json()
    all_you_want += data["data"]
    while data["next_page"]:
        r = requests.get(data["next_page"])
        r.raise_for_status()
        data = r.json()
        all_you_want += data["data"]
    return all_you_want
