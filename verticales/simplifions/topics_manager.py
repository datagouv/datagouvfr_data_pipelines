import logging

from datagouv import Client
import requests

from datagouvfr_data_pipelines.utils.datagouv import (
    get_all_from_api_query,
)

# These columns are used as the title of the topics
TAG_AND_TITLES_COLUMNS = {
    "simplifions-solutions": "Ref_Nom_de_la_solution",
    "simplifions-cas-d-usages": "Titre",
}

# These attributes are used to generate tags for the topics filters
ATTRIBUTES_FOR_TAGS = [
    "fournisseurs_de_service",
    "target_users",
    "budget",
    "types_de_simplification",
]


class TopicsManager:
    def __init__(self, client: Client):
        if not client:
            raise ValueError("client is required")

        self.client = client
        # We need to provide the api key in the headers of our requests
        # because the client doesn't have built-in api key management
        # for the topics endpoints for now
        self.dgv_headers = client.session.headers

    @staticmethod
    def generated_search_tags(topic: dict) -> list[str]:
        tags = []
        for attribute in ATTRIBUTES_FOR_TAGS:
            if topic.get(attribute):
                if isinstance(topic[attribute], list):
                    for value in topic[attribute]:
                        tags.append(f"simplifions-{attribute}-{value}")
                else:
                    tags.append(f"simplifions-{attribute}-{topic[attribute]}")
        return tags

    def update_extras_of_topic(self, topic: dict, new_extras: dict):
        url = f"{self.client.base_url}/api/1/topics/{topic['id']}/"
        r = requests.put(
            url,
            headers=self.dgv_headers,
            json={
                "tags": topic["tags"],
                "extras": new_extras,
            },
        )
        if r.status_code != 200:
            logging.error(
                f"Failed to update topic references for {topic['id']}: {r.status_code} - {r.text}"
            )
        r.raise_for_status()
        logging.info(f"Updated topic references at {url}")

    def get_all_topics_for_tag(self, tag: str) -> list[dict]:
        return get_all_from_api_query(
            f"{self.client.base_url}/api/1/topics/?tag={tag}&include_private=true",
            auth=True,
        )

    def update_topics(self, ti):
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
                for topic in self.get_all_topics_for_tag(tag)
                if extras_nested_key in topic["extras"]
            }

            logging.info(
                f"Found {len(current_topics)} existing topics in datagouv for tag {tag}"
            )

            for slug in grist_topics.keys():
                if slug in current_topics.keys():
                    # updating existing topics
                    url = f"{self.client.base_url}/api/1/topics/{current_topics[slug]}/"
                    method = "put"
                else:
                    # creating a new topic
                    url = f"{self.client.base_url}/api/1/topics/"
                    method = "post"
                logging.info(
                    f"{method} topic '{slug}'"
                    + (f" at {url}" if method == "put" else "")
                )

                r = getattr(requests, method)(
                    url,
                    headers=self.dgv_headers,
                    json={
                        "name": grist_topics[slug][title_column],
                        # description cannot be empty
                        "description": grist_topics[slug]["Description_courte"] or "-",
                        "organization": {
                            "class": "Organization",
                            "id": "57fe2a35c751df21e179df72",
                        },
                        "tags": topic_tags
                        + self.generated_search_tags(grist_topics[slug]),
                        "extras": {extras_nested_key: grist_topics[slug] or False},
                        "private": not grist_topics[slug]["Visible_sur_simplifions"],
                    },
                )
                r.raise_for_status()

            # deleting topics that are not in the table anymore
            for slug in current_topics:
                if slug not in grist_topics:
                    logging.info(
                        f"Deleting topic '{slug}' at {self.client.base_url}/api/1/topics/{current_topics[slug]}/"
                    )
                    r = requests.delete(
                        f"{self.client.base_url}/api/1/topics/{current_topics[slug]}/",
                        headers=self.dgv_headers,
                    )
                    r.raise_for_status()

    def update_topics_references(self, ti):
        all_topics = {}
        all_tags = ["simplifions-solutions", "simplifions-cas-d-usages"]

        for tag in all_tags:
            all_topics[tag] = list(self.get_all_topics_for_tag(tag))
            logging.info(f"Found {len(all_topics[tag])} topics for tag {tag}")

        solutions_topics = all_topics["simplifions-solutions"]
        cas_usages_topics = all_topics["simplifions-cas-d-usages"]

        visible_solutions_topics = [
            topic for topic in solutions_topics if topic["private"] is False
        ]
        visible_cas_usages_topics = [
            topic for topic in cas_usages_topics if topic["private"] is False
        ]

        # Update solutions_topics with references to cas_usages_topics
        for solution_topic in solutions_topics:
            cas_d_usages_slugs = solution_topic["extras"]["simplifions-solutions"][
                "cas_d_usages_slugs"
            ]
            matching_topics = [
                topic
                for topic in visible_cas_usages_topics
                if "simplifions-cas-d-usages" in topic["extras"]
                and topic["extras"]["simplifions-cas-d-usages"]["slug"]
                in cas_d_usages_slugs
            ]
            solution_topic["extras"]["simplifions-solutions"][
                "cas_d_usages_topics_ids"
            ] = [topic["id"] for topic in matching_topics]
            # update the solution topic with the new extras
            self.update_extras_of_topic(solution_topic, solution_topic["extras"])

        # Update cas_usages_topics with references to solution_topic_id and solutions_editeurs_topics
        for cas_usage_topic in cas_usages_topics:
            for reco in cas_usage_topic["extras"]["simplifions-cas-d-usages"][
                "reco_solutions"
            ]:
                matching_solution_topic = next(
                    (
                        topic
                        for topic in visible_solutions_topics
                        if "simplifions-solutions" in topic["extras"]
                        and topic["extras"]["simplifions-solutions"]["slug"]
                        == reco["solution_slug"]
                    ),
                    None,
                )
                if matching_solution_topic:
                    reco["solution_topic_id"] = matching_solution_topic["id"]

                matching_editeur_topics = [
                    topic
                    for topic in visible_solutions_topics
                    if "simplifions-solutions" in topic["extras"]
                    and not topic["extras"]["simplifions-solutions"]["is_public"]
                    and topic["extras"]["simplifions-solutions"]["slug"]
                    in reco["solution_editeurs_slugs"]
                ]
                reco["solutions_editeurs_topics"] = [
                    {
                        "topic_id": topic["id"],
                        "solution_name": topic["name"],
                        "editeur_name": topic["extras"]["simplifions-solutions"][
                            "operateur_nom"
                        ],
                    }
                    for topic in matching_editeur_topics
                ]

            # update the cas_usage topic with the new extras
            self.update_extras_of_topic(cas_usage_topic, cas_usage_topic["extras"])
