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
    # "Solutions_publiques_recommandees": "SIMPLIFIONS_produitspublics",
    "reco_solutions": "SIMPLIFIONS_reco_solutions_cas_usages",
    "usagers": "TYPE_usagers",
    "A_destination_de_": "TYPE_fournisseur_service",
}

def get_and_format_grist_data(ti):
    rows = request_grist_table(MAIN_TABLE_ID)
    
    grist_topics = {
        row["slug"]: get_and_format_grist_subdata(row)
        for row in rows
        if row["slug"]
    }
    ti.xcom_push(key="grist_topics", value=grist_topics)

def get_and_format_grist_subdata(row):
    formatted_row = {
        key: handle_subdata_list(key, row[key])
        for key in row.keys()
    }
    return formatted_row
    
def handle_subdata_list(key, value):
    if key in SUBDATA_TABLE_IDS.keys():
        if isinstance(value, list) and value[0] == "L":
            filter = json.dumps({ "id": value[1:] })
            print(filter)
            return request_grist_table(SUBDATA_TABLE_IDS[key], filter=filter)
        else:
            return value
    else:
        return value

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

def update_topics(ti):
    grist_topics: dict = ti.xcom_pull(key="grist_topics", task_ids="get_and_format_grist_data")

    # this will need adapting when we add need objects
    simplifions_tags = [
        "simplifions",
        "cas-d-usages",
        "simplifions-dag-generated",
    ]
    extras_nested_key = "cas-d-usages"
    current_topics = {
        topic["extras"][extras_nested_key]["slug"]: topic["id"]
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
            f"{method} topic '{slug}'"
            + (f" at {url}" if method == "put" else "")
        )
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
                "tags": simplifions_tags,
                "extras": {extras_nested_key: {
                    key: value or False for key, value in grist_topics[slug].items()
                    if key not in ["Titre", "Description_courte"]
                },},
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
