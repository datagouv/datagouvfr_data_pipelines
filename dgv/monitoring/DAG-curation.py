from datetime import timedelta

import aiohttp
import asyncio
from airflow.models import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.dates import days_ago
from datagouvfr_data_pipelines.config import (
    DATAGOUV_URL,
    MATTERMOST_DATAGOUV_DATAENG_TEST  # TODO: replace with MATTERMOST_DATAGOUV_ACTIVITES
)
from datagouvfr_data_pipelines.utils.mattermost import send_message
from datagouvfr_data_pipelines.utils.datagouv import get_all_from_api_query

DAG_NAME = "dgv_curation"


async def url_error(url, session):
    try:
        async with session.head(url, timeout=10) as r:
            r.raise_for_status()
        return False
    except (
        aiohttp.ClientError,
        AssertionError,
        asyncio.exceptions.TimeoutError
    ) as e:
        return str(e)


async def crawl_reuses(reuses):
    async with aiohttp.ClientSession() as session:
        url_errors = await asyncio.gather(*[url_error(reuse["url"], session) for reuse in reuses])

    unavailable_reuses = []
    for reuse, error in zip(reuses, url_errors):
        if error:
            unavailable_reuses.append((reuse, error))
    return unavailable_reuses


def get_unavailable_reuses(ti):
    print(f"Fetching and checking reuses from {DATAGOUV_URL}/api/1/reuses/")
    reuses = get_all_from_api_query(f"{DATAGOUV_URL}/api/1/reuses/?page_size=100")
    unavailable_reuses = asyncio.run(crawl_reuses(reuses))
    ti.xcom_push(key="reuses", value=unavailable_reuses)
    return True


def publish_mattermost(ti):
    print("Publishing on mattermost")
    reuses = ti.xcom_pull(key="reuses", task_ids="get_unavailable_reuses")
    message = (
        f":octagonal_sign: Voici la liste des {len(reuses)} réutilisations non disponibles"
    )
    for reuse, error in reuses:
        message += f"\n* [{reuse['title']}]({reuse['page']}): {error}"
    print(message)
    send_message(message, MATTERMOST_DATAGOUV_DATAENG_TEST)


default_args = {"email": ["geoffrey.aldebert@data.gouv.fr"], "email_on_failure": True}

with DAG(
    dag_id=DAG_NAME,
    schedule_interval="0 0 1 * *",
    start_date=days_ago(0, hour=1),
    dagrun_timeout=timedelta(minutes=60),
    tags=["curation", "monthly", "datagouv"],
    default_args=default_args,
    catchup=False,
) as dag:
    get_unavailable_reuses = ShortCircuitOperator(
        task_id="get_unavailable_reuses",
        python_callable=get_unavailable_reuses
    )

    publish_mattermost = PythonOperator(
        task_id="publish_mattermost",
        python_callable=publish_mattermost,
    )

    get_unavailable_reuses >> publish_mattermost
