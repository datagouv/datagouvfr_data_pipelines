import json
import logging

import requests

from datagouvfr_data_pipelines.config import (
    DATAGOUV_SECRET_API_KEY,
    GRIST_API_URL,
    SECRET_GRIST_API_KEY,
)
from datagouvfr_data_pipelines.utils.datagouv import get_all_from_api_query, local_client

dgv_headers = {"X-API-KEY": DATAGOUV_SECRET_API_KEY}

GRIST_DOC_ID = "c5pt7QVcKWWe"
MAIN_TABLE_ID = "SIMPLIFIONS_cas_usages"
SUBDATA_TABLE_IDS = {
    "reco_solutions": "SIMPLIFIONS_reco_solutions_cas_usages",
}

ATTRIBUTES_FOR_TAGS = ['fournisseurs_de_service', 'target_users', 'budget', 'types_de_simplification']

def request_grist_table(table_id: str, filter: str = None):
    r = requests.get(
        GRIST_API_URL + f"docs/{GRIST_DOC_ID}/tables/{table_id}/records",
        headers={
            'Authorization': 'Bearer ' + SECRET_GRIST_API_KEY,
            'Content-Type': 'application/json',
            'Accept': 'application/json',
        },
        params={"filter": filter} if filter else None,
    )
    r.raise_for_status()
    return [row["fields"] for row in r.json()["records"]]

def clean_row(row):
    cleaned_row = {}
    for key, value in row.items():
        if isinstance(value, list) and value and value[0] == "L":
            # Remove the "L" prefix and keep the rest of the list
            cleaned_row[key] = value[1:]
        else:
            cleaned_row[key] = value
    return cleaned_row
    
def get_subdata(key, value):
    if key in SUBDATA_TABLE_IDS.keys():
        filter = json.dumps({ "id": value }) if value else None
        return request_grist_table(SUBDATA_TABLE_IDS[key], filter=filter)
    else:
        return value

def cleaned_row_with_subdata(row):
    cleaned_row = clean_row(row)
    formatted_row = {
        key: get_subdata(key, cleaned_row[key])
        for key in cleaned_row.keys()
    }
    return formatted_row

def generated_search_tags(topic):
    return [
        f"simplifions-{attribute}-{value}"
        for attribute in ATTRIBUTES_FOR_TAGS
        for value in topic[attribute]
    ]


# 👇 Methods used by the DAG 👇

def get_and_format_grist_data(ti):
    rows = request_grist_table(MAIN_TABLE_ID)
    
    grist_topics = {
        row["slug"]: cleaned_row_with_subdata(row)
        for row in rows
        if row["slug"]
    }
    ti.xcom_push(key="grist_topics", value=grist_topics)

def update_topics(ti):
    grist_topics: dict = ti.xcom_pull(key="grist_topics", task_ids="get_and_format_grist_data")

    # this will need adapting when we add need objects
    simplifions_tags = [
        "simplifions",
        "simplifions-cas-d-usages",
        "simplifions-dag-generated",
    ]
    
    extras_nested_key = "simplifions-cas-usages"

    current_topics = {
        topic["extras"][extras_nested_key]["slug"]: topic["id"]
        for topic in get_all_from_api_query(
            (
                f"{local_client.base_url}/api/1/topics/?"
                + "&".join([f"tag={tag}" for tag in simplifions_tags])
            ),
        )
        if extras_nested_key in topic["extras"]
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
            f"{method} topic '{slug}'"
            + (f" at {url}" if method == "put" else "")
        )

        tags = simplifions_tags + generated_search_tags(grist_topics[slug])

        r = getattr(requests, method)(
            url,
            headers=dgv_headers,
            json={
                "name": grist_topics[slug]["Titre"],
                # description cannot be empty
                "description": grist_topics[slug]["Description_courte"] or "-",
                "organization": {
                    "class": "Organization",
                    "id": "57fe2a35c751df21e179df72",
                },
                "tags": tags,
                "extras": { extras_nested_key: grist_topics[slug] or False },
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
