import logging

import requests

from datagouvfr_data_pipelines.config import (
    DATAGOUV_SECRET_API_KEY,
    GRIST_API_URL,
    SECRET_GRIST_API_KEY,
)
from datagouvfr_data_pipelines.utils.datagouv import get_all_from_api_query, local_client

dgv_headers = {"X-API-KEY": DATAGOUV_SECRET_API_KEY}


def get_and_format_grist_data(ti):
    doc_id = "c5pt7QVcKWWe"
    table_id = "SIMPLIFIONS_cas_usages"
    r = requests.get(
        GRIST_API_URL + f"docs/{doc_id}/tables/{table_id}/records",
        headers={
            'Authorization': 'Bearer ' + SECRET_GRIST_API_KEY,
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        },
    )
    r.raise_for_status()
    rows = [row["fields"] for row in r.json()["records"]]

    grist_topics = {
        row["slug"]: {
            key: (
                row[key][1:]
                # cleaning multiple-choice cells
                if isinstance(row[key], list) and row[key][0] == "L"
                else row[key]
            )
            for key in row.keys()
        }
        for row in rows
        if row["slug"]
    }
    ti.xcom_push(key="grist_topics", value=grist_topics)


def update_topics(ti):
    grist_topics: dict = ti.xcom_pull(key="grist_topics", task_ids="get_and_format_grist_data")

    # this will need adapting when we add need objects
    simplifions_tags = [
        "simplifions-data-gouv-fr",
        "simplifions-cas-d-usage",
    ]
    current_topics = {
        topic["extras"]["slug"]: topic["id"]
        for topic in get_all_from_api_query(
            (
                f"{local_client.base_url}/api/1/topics/?"
                + "&".join([f"tag={tag}" for tag in simplifions_tags])
            ),
        )
    }

    for slug in grist_topics.keys():
        if slug in current_topics.keys():
            # updating existing topics
            url = f"{local_client.base_url}/api/1/topics/{current_topics[slug]}/"
            method = "put"
        else:
            # creating a new topic
            url = f"{local_client.base_url}/api/1/topics/"
            method = "post"
        logging.info(
            f"{method} topic '{slug}' at {local_client.base_url}/api/1/topics/{current_topics[slug]}/"
        )
        r = getattr(requests, method)(
            url,
            headers=dgv_headers,
            json={
                "name": grist_topics[slug]["Titre"],
                # description cannot be empty
                "description": grist_topics[slug]["Description_courte"] or "-",
                "tags": simplifions_tags,
                "extras": {
                    key: value or False for key, value in grist_topics[slug].items()
                    if key not in ["Titre", "Description_courte"]
                },
            },
        )
        r.raise_for_status()

    # deleting topics that are not in the table anymore
    for slug in current_topics:
        if slug not in grist_topics:
            logging.info(
                f"Deleting topic '{slug}' at {local_client.base_url}/api/1/topics/{current_topics[slug]}/"
            )
            r = requests.delete(
                f"{local_client.base_url}/api/1/topics/{current_topics[slug]}/",
                headers=dgv_headers,
            )
            r.raise_for_status()
