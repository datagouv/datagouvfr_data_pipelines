import dateutil
from typing import TypedDict, Iterator, Optional
import requests
from datetime import datetime
import logging
import re

from datagouvfr_data_pipelines.utils.filesystem import File
from datagouvfr_data_pipelines.utils.retry import simple_connection_retry, RequestRetry
from datagouvfr_data_pipelines.config import (
    AIRFLOW_ENV,
    DATAGOUV_SECRET_API_KEY,
    DEMO_DATAGOUV_URL,
    DEMO_DATAGOUV_SECRET_API_KEY,
)

if AIRFLOW_ENV == "dev":
    DATAGOUV_URL = DEMO_DATAGOUV_URL
    ORGA_REFERENCE = "63e3ae4082ddaa6c806b8417"
if AIRFLOW_ENV == "prod":
    DATAGOUV_URL = "https://www.data.gouv.fr"
    ORGA_REFERENCE = "646b7187b50b2a93b1ae3d45"
VALIDATA_BASE_URL = "https://api.validata.etalab.studio/"
DATAGOUV_MATOMO_ID = 109

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
    "repertoire d'entreprises",
    'documents professionnels',
    'immatriculation',
    'greffe',
    'juridique',
    'seo',
    'demarche',
    'B2B',
    'documents legaux',
    'entrepreneur',
    'visa',
]

datagouv_session = requests.Session()
datagouv_session.headers.update({"X-API-KEY": DATAGOUV_SECRET_API_KEY})


class BaseObject:

    def __init__(self, id: str):
        self.id = id

    @simple_connection_retry
    def get_metadata(self) -> dict:
        r = datagouv_session.get(self.url)
        r.raise_for_status()
        return r.json()

    @simple_connection_retry
    def update_metadata(self, payload: dict) -> requests.Response:
        logging.info(f"🔁 Putting {self.url} with {payload}")
        r = datagouv_session.put(self.url, json=payload)
        r.raise_for_status()
        return r

    @simple_connection_retry
    def delete(self) -> requests.Response:
        logging.info(f"🚮 Deleting {self.url}")
        r = datagouv_session.delete(self.url)
        r.raise_for_status()
        return r

    @simple_connection_retry
    def update_extras(self, payload: dict) -> requests.Response:
        logging.info(f"🔁 Putting {self.url} with extras {payload}")
        r = datagouv_session.put(self.url + "extras/", json=payload)
        r.raise_for_status()
        return r

    @simple_connection_retry
    def delete_extras(self, payload: dict) -> requests.Response:
        logging.info(f"🚮 Deleting extras {payload} for {self.url}")
        r = datagouv_session.delete(self.url + "extras/", json=payload)
        r.raise_for_status()
        return r


class Dataset(BaseObject):
    def __init__(self, id: str, on_demo: bool = False):
        super().__init__(id)
        datagouv_url = "https://demo.data.gouv.fr" if on_demo or AIRFLOW_ENV == "dev" else DATAGOUV_URL
        self.url = f"{datagouv_url}/api/1/datasets/{self.id}/"

    @staticmethod
    @simple_connection_retry
    def create(payload: dict, on_demo: bool = False) -> requests.Response:
        logging.info(f"Creating dataset '{payload['title']}'")
        datagouv_url = "https://demo.data.gouv.fr" if on_demo or AIRFLOW_ENV == "dev" else DATAGOUV_URL
        r = datagouv_session.post(f"{datagouv_url}/api/1/datasets/", json=payload)
        r.raise_for_status()
        return r


class Resource(BaseObject):
    def __init__(
        self,
        id: str,
        dataset_id: str | None,
        is_communautary: bool = False,
        on_demo: bool = False,
    ):
        super().__init__(id)
        self.dataset_id = dataset_id or self.get_dataset_id(id)
        datagouv_url = "https://demo.data.gouv.fr" if on_demo or AIRFLOW_ENV == "dev" else DATAGOUV_URL
        # datagouv_session.headers.update({"X-API-KEY": DEMO_DATAGOUV_SECRET_API_KEY})
        self.url = (
            f"{datagouv_url}/api/1/datasets/{self.dataset_id}/resources/{self.id}/"
            if not is_communautary
            else f"{datagouv_url}/api/1/datasets/community_resources/{self.id}"
        )

    @staticmethod
    @simple_connection_retry
    def create_remote(
        dataset_id: str,
        payload: dict,
        on_demo: bool = False,
        is_communautary: bool = False,
    ) -> requests.Response:
        datagouv_url = "https://demo.data.gouv.fr" if on_demo or AIRFLOW_ENV == "dev" else DATAGOUV_URL
        if is_communautary:
            url = f"{datagouv_url}/api/1/datasets/community_resources"
            payload["dataset"] = {"class": "Dataset", "id": dataset_id}
        else:
            url = f"{datagouv_url}/api/1/datasets/{dataset_id}/resources/"
        logging.info(f"Creating '{payload['title']}' at {url}")
        if "filetype" not in payload:
            payload.update({"filetype": "remote"})
        r = datagouv_session.post(url, json=payload)
        r.raise_for_status()
        return r

    @staticmethod
    @simple_connection_retry
    def create_static(
        file_to_upload: File,
        dataset_id: str,
        payload: dict,
        on_demo: bool = False,
        is_communautary: bool = False,
    ) -> requests.Response:
        datagouv_url = "https://demo.data.gouv.fr" if on_demo or AIRFLOW_ENV == "dev" else DATAGOUV_URL
        if is_communautary:
            url = f"{datagouv_url}/api/1/datasets/community_resources"
            payload["dataset"] = {"class": "Dataset", "id": dataset_id}
        else:
            url = f"{datagouv_url}/api/1/datasets/{dataset_id}/upload/"
        r = datagouv_session.post(url, files={
            "file": open(
                f"{file_to_upload['source_path']}{file_to_upload['source_name']}",
                "rb",
            )
        })
        r.raise_for_status()
        resource_id = r.json()['id']
        logging.info(f"Resource was given this id: {resource_id}")
        r = Resource(resource_id=resource_id, dataset_id=dataset_id).update_metadata(payload=payload)
        return r

    @staticmethod
    @simple_connection_retry
    def get_dataset_id(resource_id: str) -> str:
        url = f"{DATAGOUV_URL}/api/2/datasets/resources/{resource_id}/"
        r = datagouv_session.get(url)
        r.raise_for_status()
        return r.json()['dataset_id']

    def check_if_more_recent_update(
        self,
        dataset_id: str,
        on_demo: bool = False,
    ) -> bool:
        """
        Checks whether any resource of the specified dataset has been updated more recently
        than the specified resource
        """
        prefix = "demo" if on_demo else "www"
        resources = datagouv_session.get(
            f"https://{prefix}.data.gouv.fr/api/1/datasets/{dataset_id}/",
            headers={"X-fields": "resources{internal{last_modified_internal}}"}
        ).json()['resources']
        latest_update = datagouv_session.get(
            f"https://{prefix}.data.gouv.fr/api/2/datasets/resources/{self.id}/",
            headers={"X-fields": "resource{internal{last_modified_internal}}"}
        ).json()["resource"]["internal"]["last_modified_internal"]
        return any(
            r["internal"]["last_modified_internal"] > latest_update for r in resources
        )


@simple_connection_retry
def create_post(
    name: str,
    headline: str,
    content: str,
    body_type: str,
    tags: Optional[list] = [],
) -> dict:
    """Create a post in data.gouv.fr

    Args:
        name: name of post.
        headline: headline of post
        content: content of post
        body_type: body type of post (html or markdown)
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


def get_created_date(data: dict, date_key: str) -> datetime:
    # Helper to get created date based on a date_key that could be nested, using . as a separator
    for key in date_key.split('.'):
        data = data.get(key)
    created = dateutil.parser.parse(data)
    return created


@simple_connection_retry
def get_last_items(
    endpoint: str,
    start_date: datetime,
    end_date=None,
    date_key='created_at',
    sort_key='-created',
) -> list:
    results = []
    data = get_all_from_api_query(
        f"https://www.data.gouv.fr/api/1/{endpoint}/?sort={sort_key}",
        # this WILL fail locally for users because of token mismath (demo/prod)
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


@simple_connection_retry
def get_latest_comments(start_date: datetime, end_date: datetime = None) -> list:
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


def get_all_from_api_query(
    base_query: str,
    next_page: str = 'next_page',
    ignore_errors: bool = False,
    mask: Optional[str] = None,
    auth: bool = False,
) -> Iterator[dict]:
    """/!\ only for paginated endpoints"""
    def get_link_next_page(elem: dict, separated_keys: str):
        result = elem
        for k in separated_keys.split('.'):
            result = result[k]
        return result
    # certain endpoints require authentification but otherwise we're not using it
    # when running locally this can trigger 401 (if you use your dev/demo token in prod)
    # to prevent, overwrite the API key with a valid prod key down here
    # DATAGOUV_SECRET_API_KEY = ""
    headers = {"X-API-KEY": DATAGOUV_SECRET_API_KEY} if auth else {}
    if mask is not None:
        headers["X-fields"] = mask + f",{next_page}"
    r = RequestRetry.get(base_query, headers=headers)
    if not ignore_errors:
        r.raise_for_status()
    for elem in r.json()["data"]:
        yield elem
    while get_link_next_page(r.json(), next_page):
        r = RequestRetry.get(get_link_next_page(r.json(), next_page), headers=headers)
        if not ignore_errors:
            r.raise_for_status()
        for data in r.json()['data']:
            yield data


@simple_connection_retry
# Function to post a comment on a dataset
def post_comment_on_dataset(dataset_id: str, title: str, comment: str) -> requests.Response:
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


@simple_connection_retry
def get_awaiting_spam_comments() -> dict:
    r = datagouv_session.get("https://www.data.gouv.fr/api/1/spam/")
    r.raise_for_status()
    return r.json()


@simple_connection_retry
def check_duplicated_orga(slug: str) -> tuple[bool, Optional[str]]:
    duplicate_slug_pattern = r'-\d+$'
    if re.search(duplicate_slug_pattern, slug) is not None:
        suffix = re.findall(duplicate_slug_pattern, slug)[0]
        original_orga = slug[:-len(suffix)]
        url_dup = f"https://data.gouv.fr/api/1/organizations/{original_orga}/"
        test_orga = requests.get(url_dup)
        # only considering a duplicate if the original slug is taken (not not found or deleted)
        if test_orga.status_code not in [404, 410]:
            return True, url_dup
    return False, None
