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
TAGS_AND_TABLES = {
    "simplifions-cas-d-usages": {
        "table_id": "SIMPLIFIONS_cas_usages",
        "title_column": "Titre"
    },
    "simplifions-solutions": {
      "table_id": "SIMPLIFIONS_produitspublics",
      "title_column": "Ref_Nom_de_la_solution"
    }
}
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
        subdata = request_grist_table(SUBDATA_TABLE_IDS[key], filter=filter)
        return [ clean_row(item) for item in subdata ]
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
    tags = []
    for attribute in ATTRIBUTES_FOR_TAGS:
        if attribute in topic and topic[attribute]:
            if isinstance(topic[attribute], list):
                for value in topic[attribute]:
                    tags.append(f'simplifions-{attribute}-{value}')
            else:
                tags.append(f'simplifions-{attribute}-{topic[attribute]}')
        
    return tags


# 👇 Methods used by the DAG 👇

def get_and_format_grist_data(ti):
    tag_and_grist_topics = {}

    for tag, table_info in TAGS_AND_TABLES.items():
        rows = request_grist_table(table_info["table_id"])
        
        tag_and_grist_topics[tag] = {
            row["slug"]: cleaned_row_with_subdata(row)
            for row in rows
            if row["slug"]
        }
    
    ti.xcom_push(key="tag_and_grist_topics", value=tag_and_grist_topics)

def update_topics(ti):
    tag_and_grist_topics: dict = ti.xcom_pull(key="tag_and_grist_topics", task_ids="get_and_format_grist_data")

    simplifions_tags = [
        "simplifions",
        "simplifions-dag-generated",
    ]

    for tag, grist_topics in tag_and_grist_topics.items():
        logging.info(f"\n\nUpdating topics for tag: {tag}")
        
        extras_nested_key = tag
        topic_tags = simplifions_tags + [tag]

        current_topics = {
            topic["extras"][extras_nested_key]["slug"]: topic["id"]
            for topic in get_all_from_api_query(
                (
                    f"{local_client.base_url}/api/1/topics/?"
                    + "&".join([f"tag={tag}" for tag in topic_tags])
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

            r = getattr(requests, method)(
                url,
                headers=dgv_headers,
                json={
                    "name": grist_topics[slug][TAGS_AND_TABLES[tag]["title_column"]],
                    # description cannot be empty
                    "description": grist_topics[slug]["Description_courte"] or "-",
                    "organization": {
                        "class": "Organization",
                        "id": "57fe2a35c751df21e179df72",
                    },
                    "tags": topic_tags + generated_search_tags(grist_topics[slug]),
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
