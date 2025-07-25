import json
import logging

import requests

from datagouvfr_data_pipelines.config import (
    DATAGOUV_SECRET_API_KEY,
    GRIST_API_URL,
    SECRET_GRIST_API_KEY,
)
from datagouvfr_data_pipelines.utils.datagouv import (
    get_all_from_api_query,
    # for now targetting demo
    # local_client,
    demo_client,
)

dgv_headers = {"X-API-KEY": DATAGOUV_SECRET_API_KEY}

GRIST_DOC_ID = "c5pt7QVcKWWe"
TAGS_AND_TABLES = {
    "simplifions-cas-d-usages": {
        "table_id": "SIMPLIFIONS_cas_usages",
        "title_column": "Titre",
        "sub_tables": {
            "reco_solutions": "SIMPLIFIONS_reco_solutions_cas_usages",
            "API_et_donnees_utiles": "Apidata"
        }
    },
    "simplifions-solutions": {
      "table_id": "SIMPLIFIONS_produitspublics",
      "title_column": "Ref_Nom_de_la_solution",
      "sub_tables": {
        #   "Cas_d_usages": "SIMPLIFIONS_cas_usages",
      }
    }
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


def get_subdata(key, value, table_info):
    if not value:
        return value
    elif table_info["sub_tables"] and key in table_info["sub_tables"].keys():
        value_as_list = value if isinstance(value, list) else [value]
        filter = json.dumps({"id": value_as_list})
        subdata = request_grist_table(table_info["sub_tables"][key], filter=filter)
        cleaned_subdata = [clean_row(item) for item in subdata if item.get("Visible_sur_simplifions", True)]
        return cleaned_subdata if isinstance(value, list) else cleaned_subdata[0]
    else:
        return value


def cleaned_row_with_subdata(row, table_info):
    cleaned_row = clean_row(row)
    formatted_row = {
        key: get_subdata(key, cleaned_row[key], table_info)
        for key in cleaned_row.keys()
    }
    return formatted_row


def generated_search_tags(topic):
    tags = []
    for attribute in ATTRIBUTES_FOR_TAGS:
        if topic.get(attribute):
            if isinstance(topic[attribute], list):
                for value in topic[attribute]:
                    tags.append(f'simplifions-{attribute}-{value}')
            else:
                tags.append(f'simplifions-{attribute}-{topic[attribute]}')
    return tags

def update_extras_of_topic(topic, new_extras):
    url = f"{demo_client.base_url}/api/1/topics/{topic['id']}/"
    r = requests.put(
        url,
        headers=dgv_headers,
        json={
            "tags": topic["tags"],
            "extras": new_extras,
        },
    )
    if r.status_code != 200:
        logging.error(f"Failed to update topic references for {topic['id']}: {r.status_code} - {r.text}")
    r.raise_for_status()
    logging.info(f"Updated topic references at {url}")


# 👇 Methods used by the DAG 👇
def get_and_format_grist_data(ti):
    tag_and_grist_topics = {}

    for tag, table_info in TAGS_AND_TABLES.items():
        rows = request_grist_table(table_info["table_id"])

        tag_and_grist_topics[tag] = {
            row["slug"]: cleaned_row_with_subdata(row, table_info)
            for row in rows
            if row["slug"]
        }

    ti.xcom_push(key="tag_and_grist_topics", value=tag_and_grist_topics)


def update_topics(ti):
    tag_and_grist_topics: dict = ti.xcom_pull(
        key="tag_and_grist_topics", task_ids="get_and_format_grist_data"
    )

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
                    f"{demo_client.base_url}/api/1/topics/?"
                    + "&".join([f"tag={tag}" for tag in topic_tags])
                ),
            )
            if extras_nested_key in topic["extras"]
        }

        for slug in grist_topics.keys():
            if slug in current_topics.keys():
                # updating existing topics
                url = f"{demo_client.base_url}/api/1/topics/{current_topics[slug]}/"
                method = "put"
            else:
                # creating a new topic
                url = f"{demo_client.base_url}/api/1/topics/"
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
                    "extras": {extras_nested_key: grist_topics[slug] or False},
                    "private": not grist_topics[slug]["Visible_sur_simplifions"],
                },
            )
            r.raise_for_status()

        # deleting topics that are not in the table anymore
        for slug in current_topics:
            if slug not in grist_topics:
                logging.info(
                    f"Deleting topic '{slug}' at {demo_client.base_url}/api/1/topics/{current_topics[slug]}/"
                )
                r = requests.delete(
                    f"{demo_client.base_url}/api/1/topics/{current_topics[slug]}/",
                    headers=dgv_headers,
                )
                r.raise_for_status()


def update_topics_references(ti):
    all_topics = {}
    for tag in TAGS_AND_TABLES.keys():
        all_topics[tag] = [
            topic for topic in get_all_from_api_query(
                f"{demo_client.base_url}/api/2/topics/?tag={tag}&private=false"
            )
        ]
        logging.info(f"Found {len(all_topics[tag])} topics for tag {tag}")

    solutions_topics = all_topics["simplifions-solutions"]
    cas_usages_topics = all_topics["simplifions-cas-d-usages"]
    
    # Update solutions_topics with references to cas_usages_topics
    for solution_topic in solutions_topics:
        cas_d_usages_slugs = solution_topic["extras"]["simplifions-solutions"]["cas_d_usages_slugs"]
        matching_topics = [
            topic for topic in cas_usages_topics
            if "simplifions-cas-d-usages" in topic["extras"]
            and topic["extras"]["simplifions-cas-d-usages"]["slug"] in cas_d_usages_slugs
        ]
        solution_topic["extras"]["simplifions-solutions"]["cas_d_usages_topics_ids"] = [
            topic["id"] for topic in matching_topics
        ]
        # update the solution topic with the new extras
        update_extras_of_topic(solution_topic, solution_topic["extras"])

    # Update cas_usages_topics with references to solutions_topics
    for cas_usage_topic in cas_usages_topics:
        for reco in cas_usage_topic["extras"]["simplifions-cas-d-usages"]["reco_solutions"]: 
            matching_topic = next(
                (
                    topic
                    for topic in solutions_topics
                    if "simplifions-solutions" in topic["extras"]
                    and topic["extras"]["simplifions-solutions"]["slug"] == reco["solution_slug"]
                ),
                None
            )
            if matching_topic:
                reco["solution_topic_id"] = matching_topic["id"]
        # update the cas_usage topic with the new extras
        update_extras_of_topic(cas_usage_topic, cas_usage_topic["extras"])
