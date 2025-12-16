import dateutil
import requests
from datetime import datetime
import re

from datagouv import Client

from datagouvfr_data_pipelines.utils.retry import simple_connection_retry
from datagouvfr_data_pipelines.config import (
    AIRFLOW_ENV,
    DATAGOUV_SECRET_API_KEY,
    DEMO_DATAGOUV_SECRET_API_KEY,
)

if AIRFLOW_ENV == "dev":
    ORGA_REFERENCE = "63e3ae4082ddaa6c806b8417"
if AIRFLOW_ENV == "prod":
    ORGA_REFERENCE = "646b7187b50b2a93b1ae3d45"
VALIDATA_BASE_URL = "https://api.validata.etalab.studio/"
DATAGOUV_MATOMO_ID = 109

SPAM_WORDS = [
    "free",
    "gratuit",
    "allah",
    "jesuscall",
    "promo",
    "argent",
    "reduction",
    "economisez",
    "urgent",
    "recompense",
    "discount",
    "money",
    "gagner",
    "libido",
    "sex",
    "viagra",
    "bitcoin",
    "cash",
    "satisfied",
    "miracle",
    "weight loss",
    "voyance",
    "streaming",
    "benefits",
    "escort",
    "kbis",
    "hack",
    "fuck",
    "macron",
    "acte de",
    "siret",
    "casier judiciaire",
    "officiel",
    "annuaire",
    "carte grise",
    "passeport",
    "administratif",
    "repertoire d'entreprises",
    "documents professionnels",
    "immatriculation",
    "greffe",
    "juridique",
    "seo",
    "demarche",
    "B2B",
    "documents legaux",
    "entrepreneur",
    "visa",
    "abortion",
    "pills",
]

prod_client = Client(api_key=DATAGOUV_SECRET_API_KEY, timeout=30)
demo_client = Client(
    environment="demo", api_key=DEMO_DATAGOUV_SECRET_API_KEY, timeout=30
)
local_client = prod_client if AIRFLOW_ENV == "prod" else demo_client

datagouv_session = requests.Session()
datagouv_session.headers.update({"X-API-KEY": DATAGOUV_SECRET_API_KEY})


@simple_connection_retry
def create_post(
    name: str,
    headline: str,
    content: str,
    body_type: str,
    tags: list | None = [],
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
        f"{local_client.base_url}/api/1/posts/",
        json={
            "name": name,
            "headline": headline,
            "content": content,
            "body_type": body_type,
            "tags": tags,
        },
    )
    assert r.status_code == 201
    return r.json()


def get_created_date(data: dict, date_key: str) -> datetime:
    # Helper to get created date based on a date_key that could be nested, using . as a separator
    for key in date_key.split("."):
        data = data.get(key)
    created = dateutil.parser.parse(data)
    return created


@simple_connection_retry
def get_last_items(
    endpoint: str,
    start_date: datetime,
    end_date=None,
    date_key="created_at",
    sort_key="-created",
) -> list:
    results = []
    data = local_client.get_all_from_api_query(f"api/1/{endpoint}/?sort={sort_key}")
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
    data = local_client.get_all_from_api_query(
        "api/1/discussions/?sort=-discussion.posted_on"
    )
    for d in data:
        latest_comment = datetime.fromisoformat(d["discussion"][-1]["posted_on"])
        if latest_comment.timestamp() < start_date.timestamp():
            break
        # going up from the latest comment
        for comment in d["discussion"][::-1]:
            posted_ts = datetime.fromisoformat(comment["posted_on"]).timestamp()
            if end_date and posted_ts > end_date.timestamp():
                continue
            elif posted_ts < start_date.timestamp():
                break
            results.append(
                {
                    "comment_id": f"{d['id']}|{comment['posted_on']}",
                    "discussion_subject": d["subject"],
                    "discussion_title": d["title"],
                    "discussion_created": d["created"],
                    "comment": comment,
                }
            )
    return results


@simple_connection_retry
def post_comment_on_dataset(
    dataset_id: str, title: str, comment: str
) -> requests.Response:
    post_object = {
        "title": title,
        "comment": comment,
        "subject": {"class": "Dataset", "id": dataset_id},
    }
    r = datagouv_session.post(
        f"{local_client.base_url}/datasets/{dataset_id}/discussions/",
        json=post_object,
    )
    r.raise_for_status()
    return r


@simple_connection_retry
def get_awaiting_spam_comments() -> dict:
    r = datagouv_session.get("https://www.data.gouv.fr/api/1/spam/")
    r.raise_for_status()
    return r.json()


@simple_connection_retry
def check_duplicated_orga(slug: str) -> tuple[bool, str | None]:
    duplicate_slug_pattern = r"-\d+$"
    if re.search(duplicate_slug_pattern, slug) is not None:
        suffix = re.findall(duplicate_slug_pattern, slug)[0]
        original_orga = slug[: -len(suffix)]
        url_dup = f"https://data.gouv.fr/api/1/organizations/{original_orga}/"
        test_orga = requests.get(url_dup)
        # only considering a duplicate if the original slug is taken (not not found or deleted)
        if test_orga.status_code not in [404, 410]:
            return True, url_dup
    return False, None
