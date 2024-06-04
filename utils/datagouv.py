import dateutil
from typing import List, Optional, TypedDict
import requests
import os
import numpy as np
from datetime import datetime
from datagouvfr_data_pipelines.config import (
    AIRFLOW_ENV,
    DATAGOUV_SECRET_API_KEY,
)

if AIRFLOW_ENV == "dev":
    DATAGOUV_URL = "https://demo.data.gouv.fr"
    ORGA_REFERENCE = "63e3ae4082ddaa6c806b8417"
if AIRFLOW_ENV == "prod":
    DATAGOUV_URL = "https://www.data.gouv.fr"
    ORGA_REFERENCE = "646b7187b50b2a93b1ae3d45"


SPAM_WORDS = [
    'free',
    'gratuit',
    'allah',
    'jesus'
    'call',
    'promo',
    'argent',
    'reduction',
    'economisez',
    'urgent',
    'recompense',
    'discount',
    'money',
    'gagner',
    'libido',
    'sex',
    'viagra',
    'bitcoin',
    'cash',
    'satisfied',
    'miracle',
    'weight loss',
    'voyance',
    'streaming',
    'benefits',
    'escort',
    'kbis',
    'hack',
    'fuck',
    'macron',
    'acte de',
    'siret',
    'casier judiciaire',
    'officiel',
    'annuaire',
    'carte grise',
    'passeport',
    'administratif',
    "répertoire d'entreprises",
    'documents professionnels',
    'immatriculation',
    'greffe',
    'juridique',
    'seo',
    'démarche',
    'B2B',
    'documents légaux',
    'entrepeneur',
]

datagouv_session = requests.Session()
datagouv_session.headers.update({"X-API-KEY": DATAGOUV_SECRET_API_KEY})


class File(TypedDict):
    dest_path: str
    dest_name: str


def create_dataset(
    payload: TypedDict,
):
    """Create a dataset in data.gouv.fr

    Args:
        payload (TypedDict): payload for dataset containing at minimum title

    Returns:
        json: return API result in a dictionnary
    """
    r = datagouv_session.post(
        f"{DATAGOUV_URL}/api/1/datasets/", json=payload
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
    with datagouv_session.get(
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
    file_to_upload: File,
    dataset_id: str,
    resource_id: Optional[str] = None,
    resource_payload: Optional[dict] = None,
):
    """Upload a resource in data.gouv.fr

    Args:
        file_to_upload (File): Dictionnary containing `dest_path` and
        `dest_name` where resource to upload is stored
        dataset_id (str): ID of the dataset where to store resource
        resource_id (Optional[str], optional): ID of the resource where
        to upload file. If it is a new resource, let it to None.
        Defaults to None.
        resource_payload (Optional[dict], optional): payload to update the resource's metadata
        Defaults to None (then the id is retrieved when the resource is created)

    Returns:
        json: return API result in a dictionnary
    """
    if not file_to_upload['dest_path'].endswith('/'):
        file_to_upload['dest_path'] += '/'
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
    r = datagouv_session.post(url, files=files)
    r.raise_for_status()
    if not resource_id:
        resource_id = r.json()['id']
        print("Resource was given this id:", resource_id)
        url = f"{DATAGOUV_URL}/api/1/datasets/{dataset_id}/resources/{resource_id}/upload/"
    if resource_id and resource_payload:
        r_put = datagouv_session.put(url.replace('upload/', ''), json=resource_payload)
        r_put.raise_for_status()
    return r


def post_remote_resource(
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
        r = datagouv_session.put(
            url,
            json=payload,
        )
    else:
        url = f"{DATAGOUV_URL}/api/1/datasets/{dataset_id}/resources/"
        print(f"Posting '{title}' at {url}")
        r = datagouv_session.post(
            url,
            json=payload,
        )
    r.raise_for_status()
    return r.json()


def delete_dataset_or_resource(
    dataset_id: str,
    resource_id: Optional[str] = None,
):
    """Delete a dataset or a resource in data.gouv.fr

    Args:
        dataset_id (str): ID of the dataset
        resource_id (Optional[str], optional): ID of the resource.
        If resource is None, the dataset will be deleted. Else only the resource.
        Defaults to None.

    Returns:
        json: return API result in a dictionnary
    """
    if resource_id:
        url = f"{DATAGOUV_URL}/api/1/datasets/{dataset_id}/resources/{resource_id}/"
    else:
        url = f"{DATAGOUV_URL}/api/1/datasets/{dataset_id}/"

    r = datagouv_session.delete(url)
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
    r = datagouv_session.get(url)
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
    r = datagouv_session.get(url)
    r.raise_for_status()
    return r.json()['dataset_id']


def update_dataset_or_resource_metadata(
    payload: TypedDict,
    dataset_id: str,
    resource_id: Optional[str] = None,
):
    """Update metadata to dataset or resource in data.gouv.fr

    Args:
        payload (TypedDict): metadata to upload.
        dataset_id (str): ID of the dataset
        resource_id (Optional[str], optional): ID of the resource.
        If resource_id is None, it will be dataset metadata which will be
        updated. Else it will be resouce_id's ones. Defaults to None.

    Returns:
       json: return API result in a dictionnary containing metadatas
    """
    if resource_id:
        url = f"{DATAGOUV_URL}/api/1/datasets/{dataset_id}/resources/{resource_id}/"
    else:
        url = f"{DATAGOUV_URL}/api/1/datasets/{dataset_id}/"

    r = datagouv_session.put(url, json=payload)
    r.raise_for_status()
    return r


def update_dataset_or_resource_extras(
    payload: TypedDict,
    dataset_id: str,
    resource_id: Optional[str] = None,
):
    """Update specific extras to a dataset or resource in data.gouv.fr

    Args:
        payload (TypedDict): Payload contaning extra and its value
        dataset_id (str): ID of the dataset.
        resource_id (Optional[str], optional): ID of the resource.
        If resource_id is None, it will be dataset extras which will be
        updated. Else it will be resouce_id's ones. Defaults to None.

    Returns:
       json: return API result in a dictionnary containing metadatas
    """
    if resource_id:
        url = f"{DATAGOUV_URL}/api/2/datasets/{dataset_id}/resources/{resource_id}/extras/"
    else:
        url = f"{DATAGOUV_URL}/api/2/datasets/{dataset_id}/extras/"
    r = datagouv_session.put(url, json=payload)
    r.raise_for_status()
    return r


def delete_dataset_or_resource_extras(
    extras: List,
    dataset_id: str,
    resource_id: Optional[str] = None,
):
    """Delete extras from a dataset or resoruce in data.gouv.fr

    Args:
        extras (List): List of extras to delete.
        dataset_id (str): ID of the dataset.
        resource_id (Optional[str], optional): ID of the resource.
        If resource_id is None, it will be dataset extras which will be
        deleted. Else it will be resouce_id's ones. Defaults to None.

    Returns:
       json: return API result in a dictionnary containing metadatas
    """
    if resource_id:
        url = f"{DATAGOUV_URL}/api/2/datasets/{dataset_id}/resources/{resource_id}/extras/"
    else:
        url = f"{DATAGOUV_URL}/api/2/datasets/{dataset_id}/extras/"
    r = datagouv_session.delete(url, json=extras)
    if r.status_code == 204:
        return {"message": "ok"}
    else:
        return r.json()


def create_post(
    name: str,
    headline: str,
    content: str,
    body_type: str,
    tags: Optional[List] = [],
):
    """Create a post in data.gouv.fr

    Args:
        name (str): name of post.
        headline (str): headline of post
        content (str) : content of post
        body_type (str) : body type of post (html or markdown)
        tags: Option list of tags for post

    Returns:
       json: return API result in a dictionnary containing metadatas
    """

    r = datagouv_session.post(
        f"{DATAGOUV_URL}/api/1/posts/",
        json={
            'name': name,
            'headline': headline,
            'content': content,
            'body_type': body_type,
            'tags': tags
        },
    )
    assert r.status_code == 201
    return r.json()


def get_created_date(data, date_key):
    # Helper to get created date based on a date_key that could be nested, using . as a separator
    for key in date_key.split('.'):
        data = data.get(key)
    created = dateutil.parser.parse(data)
    return created


def get_last_items(endpoint, start_date, end_date=None, date_key='created_at', sort_key='-created'):
    results = []
    data = get_all_from_api_query(
        f"https://www.data.gouv.fr/api/1/{endpoint}/?sort={sort_key}",
        # this WILL fail locally  for users because of token mismath (demo/prod)
        auth=endpoint == "users",
    )
    for d in data:
        created = get_created_date(d, date_key)
        if end_date and created.timestamp() > end_date.timestamp():
            continue
        elif created.timestamp() < start_date.timestamp():
            break
        results.append(d)
    return results


def get_latest_comments(start_date, end_date=None):
    """
    Get latest comments posted on discussions, stored as a list of tuples:
    ("discussion_id:comment_timestamp", subject, comment)
    """
    results = []
    data = get_all_from_api_query(
        "https://www.data.gouv.fr/api/1/discussions/?sort=-discussion.posted_on"
    )
    for d in data:
        latest_comment = datetime.fromisoformat(d['discussion'][-1]['posted_on'])
        if latest_comment.timestamp() < start_date.timestamp():
            break
        # going up from the latest comment
        for comment in d['discussion'][::-1]:
            posted_ts = datetime.fromisoformat(comment['posted_on']).timestamp()
            if end_date and posted_ts > end_date.timestamp():
                continue
            elif posted_ts < start_date.timestamp():
                break
            results.append({
                "comment_id": f"{d['id']}|{comment['posted_on']}",
                "discussion_subject": d['subject'],
                "discussion_title": d['title'],
                "discussion_created": d['created'],
                "comment": comment,
            })
    return results


def post_remote_communautary_resource(
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
    community_resource_url = f"{DATAGOUV_URL}/api/1/datasets/community_resources"
    dataset_link = f"{DATAGOUV_URL}/fr/datasets/{dataset_id}/#/community-resources"

    # Check if resource already exists
    data = datagouv_session.get(
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

        r = datagouv_session.put(
            refined_url,
            json=payload,
        )

    else:
        print(f"Creating resource at {dataset_link} from {remote_url}")
        # Create resource
        r = datagouv_session.post(
            community_resource_url,
            json=payload,
        )
    r.raise_for_status()
    return r.json()


def get_all_from_api_query(
    base_query,
    next_page='next_page',
    ignore_errors=False,
    mask=None,
    auth=False,
):
    """/!\ only for paginated endpoints"""
    def get_link_next_page(elem, separated_keys):
        result = elem
        for k in separated_keys.split('.'):
            result = result[k]
        return result
    # certain endpoints require authentification but otherwise we're not using it
    # when running locally this can trigger 401 (if you use your dev/demo token in prod)
    headers = {"X-API-KEY": DATAGOUV_SECRET_API_KEY} if auth else {}
    if mask is not None:
        headers["X-fields"] = mask + f",{next_page}"
    r = requests.get(base_query, headers=headers)
    if not ignore_errors:
        r.raise_for_status()
    for elem in r.json()["data"]:
        yield elem
    while get_link_next_page(r.json(), next_page):
        r = requests.get(get_link_next_page(r.json(), next_page), headers=headers)
        if not ignore_errors:
            r.raise_for_status()
        for data in r.json()['data']:
            yield data


# Function to post a comment on a dataset
def post_comment_on_dataset(dataset_id, title, comment):
    post_object = {
        "title": title,
        "comment": comment,
        "subject": {"class": "Dataset", "id": dataset_id},
    }
    r = datagouv_session.post(
        f"{DATAGOUV_URL}/fr/datasets/{dataset_id}/discussions/",
        json=post_object
    )
    r.raise_for_status()
    return r


def get_awaiting_spam_comments():
    r = datagouv_session.get("https://www.data.gouv.fr/api/1/spam/")
    r.raise_for_status()
    return r.json()
