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
GRIST_TABLES_AND_TAGS = {
    "SIMPLIFIONS_cas_usages": {
        "tag": "simplifions-cas-d-usages",
        "sub_tables": {
            "reco_solutions": "SIMPLIFIONS_reco_solutions_cas_usages",
            "API_et_donnees_utiles": "Apidata",
            "descriptions_api_et_donnees_utiles": "SIMPLIFIONS_description_apidata_cas_usages",
        }
    },
    "SIMPLIFIONS_produitspublics": {
        "tag": "simplifions-solutions",
        "sub_tables": {
            "API_et_data_disponibles": "Apidata"
        }
    },
    "SIMPLIFIONS_solutions_editeurs": {
        "tag": "simplifions-solutions",
        "sub_tables": {
            "API_et_data_disponibles": "Apidata"
        }

    }
}

TAG_AND_TITLES_COLUMNS = {
    "simplifions-solutions": "Ref_Nom_de_la_solution",
    "simplifions-cas-d-usages": "Titre",
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

def get_all_topics_for_tag(tag):
    return get_all_from_api_query(
        f"{demo_client.base_url}/api/1/topics/?tag={tag}&include_private=true",
        auth=True,
    )

# 👇 Methods used by the DAG 👇

def get_and_format_grist_data(ti):
    tag_and_grist_topics = {}

    for table_id, table_info in GRIST_TABLES_AND_TAGS.items():
        tag = table_info["tag"]
        rows = request_grist_table(table_id)

        if tag not in tag_and_grist_topics:
            tag_and_grist_topics[tag] = {}

        tag_and_grist_topics[tag].update({
            row["slug"]: cleaned_row_with_subdata(row, table_info)
            for row in rows
            if row["slug"]
        })

    logging_str = "\n".join([f"{tag}: {len(grist_topics)} topics" for tag, grist_topics in tag_and_grist_topics.items()])
    total_length = sum([len(grist_topics) for grist_topics in tag_and_grist_topics.values()])
    logging.info(f"Found {total_length} items in grist: \n{logging_str}")

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
        logging.info(f"\n\n\nUpdating {len(grist_topics)} topics for tag: {tag}")

        title_column = TAG_AND_TITLES_COLUMNS[tag]
        extras_nested_key = tag
        topic_tags = simplifions_tags + [tag]

        current_topics = {
            topic["extras"][extras_nested_key]["slug"]: topic["id"]
            for topic in get_all_topics_for_tag(tag)
            if extras_nested_key in topic["extras"]
        }

        logging.info(f"Found {len(current_topics)} existing topics in datagouv for tag {tag}")

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
                    "name": grist_topics[slug][title_column],
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
    all_tags = ["simplifions-solutions", "simplifions-cas-d-usages"]

    for tag in all_tags:
        all_topics[tag] = list(get_all_topics_for_tag(tag))
        logging.info(f"Found {len(all_topics[tag])} topics for tag {tag}")

    solutions_topics = all_topics["simplifions-solutions"]
    cas_usages_topics = all_topics["simplifions-cas-d-usages"]

    visible_solutions_topics = [topic for topic in solutions_topics if topic["private"] == False]
    visible_cas_usages_topics = [topic for topic in cas_usages_topics if topic["private"] == False]
    
    # Update solutions_topics with references to cas_usages_topics
    for solution_topic in solutions_topics:
        cas_d_usages_slugs = solution_topic["extras"]["simplifions-solutions"]["cas_d_usages_slugs"]
        matching_topics = [
            topic for topic in visible_cas_usages_topics
            if "simplifions-cas-d-usages" in topic["extras"]
            and topic["extras"]["simplifions-cas-d-usages"]["slug"] in cas_d_usages_slugs
        ]
        solution_topic["extras"]["simplifions-solutions"]["cas_d_usages_topics_ids"] = [
            topic["id"] for topic in matching_topics
        ]
        # update the solution topic with the new extras
        update_extras_of_topic(solution_topic, solution_topic["extras"])

    # Update cas_usages_topics with references to solution_topic_id and solutions_editeurs_topics
    for cas_usage_topic in cas_usages_topics:
        for reco in cas_usage_topic["extras"]["simplifions-cas-d-usages"]["reco_solutions"]: 
            matching_solution_topic = next(
                (
                    topic
                    for topic in visible_solutions_topics
                    if "simplifions-solutions" in topic["extras"] 
                    and topic["extras"]["simplifions-solutions"]["slug"] == reco["solution_slug"]
                ),
                None
            )
            if matching_solution_topic:
                reco["solution_topic_id"] = matching_solution_topic["id"]
            
            matching_editeur_topics = [
                topic for topic in visible_solutions_topics
                if "simplifions-solutions" in topic["extras"]
                and not topic["extras"]["simplifions-solutions"]["is_public"]
                and topic["extras"]["simplifions-solutions"]["slug"] in reco["solution_editeurs_slugs"]
            ]
            reco["solutions_editeurs_topics"] = [
                { 
                    "topic_id": topic["id"],
                    "solution_name": topic["name"],
                    "editeur_name": topic["extras"]["simplifions-solutions"]["operateur_nom"],
                } for topic in matching_editeur_topics
            ]

        # update the cas_usage topic with the new extras
        update_extras_of_topic(cas_usage_topic, cas_usage_topic["extras"])
