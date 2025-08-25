import logging
from copy import deepcopy
from slugify import slugify

from datagouv import Client
from datagouvfr_data_pipelines.verticales.simplifions.topics_api import TopicsAPI

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

# These attributes are not used when comparing topics for update
TOPICS_REFERENCES_ATTRIBUTES = {
    "simplifions-solutions": [
        "simplifions-solutions.cas_d_usages_topics_ids",
    ],
    "simplifions-cas-d-usages": [
        "simplifions-cas-d-usages.reco_solutions[].solution_topic_id",
        "simplifions-cas-d-usages.reco_solutions[].solutions_editeurs_topics",
    ],
}


class TopicsManager:
    def __init__(self, client: Client):
        self.topics_api = TopicsAPI(client)

    @staticmethod
    def _slugify_tag(string: str) -> str:
        return slugify(string.lower())

    @staticmethod
    def _generated_search_tags(topic: dict) -> list[str]:
        tags = []
        for attribute in ATTRIBUTES_FOR_TAGS:
            if topic.get(attribute):
                if isinstance(topic[attribute], list):
                    for value in topic[attribute]:
                        tags.append(
                            TopicsManager._slugify_tag(
                                f"simplifions-{attribute}-{value}"
                            )
                        )
                else:
                    tags.append(
                        TopicsManager._slugify_tag(
                            f"simplifions-{attribute}-{topic[attribute]}"
                        )
                    )
        return tags

    @staticmethod
    def _nested_pop(data, key_chain):
        """
        Recursively remove a key from nested dictionaries and arrays.

        Args:
            data: The dictionary or list to modify
            key_chain: Dot-separated key path. Use '[]' to indicate array processing.
                      Example: 'level1.array_key[].target_key'
        """
        keys = key_chain.split(".")
        return TopicsManager._nested_pop_recursive(data, keys, key_chain)

    @staticmethod
    def _nested_pop_recursive(current, keys, original_key_chain):
        """
        Recursive helper function for _nested_pop.

        Args:
            current: Current level of the data structure
            keys: Remaining keys to process
            original_key_chain: Original key chain for error messages
        """
        if not keys:
            return

        key = keys[0]
        remaining_keys = keys[1:]

        # Handle array notation: key[]
        if key.endswith("[]"):
            array_key = key[:-2]

            if not isinstance(current, dict) or array_key not in current:
                raise KeyError(
                    f"Key {array_key} of {original_key_chain} not found in : {current}"
                )

            if not isinstance(current[array_key], list):
                raise TypeError(
                    f"Expected list for key {array_key}, got {type(current[array_key])}"
                )

            # Apply recursively to each dict item in the array
            for item in current[array_key]:
                if isinstance(item, dict):
                    TopicsManager._nested_pop_recursive(
                        item, remaining_keys, original_key_chain
                    )
            return

        # Validate current is a dict
        if not isinstance(current, dict):
            raise KeyError(
                f"Key {key} of {original_key_chain} not found in : {current}"
            )

        # If this is the final key, pop it (if it exists); otherwise, continue recursion
        if not remaining_keys:
            current.pop(key, None)  # Use pop with default to avoid KeyError
        else:
            # For intermediate keys, we still need to raise an error if they don't exist
            if key not in current:
                raise KeyError(
                    f"Key {key} of {original_key_chain} not found in : {current}"
                )
            TopicsManager._nested_pop_recursive(
                current[key], remaining_keys, original_key_chain
            )

    def _topics_are_similar_so_we_can_skip_update(
        self, old_topic: dict, new_topic: dict, ignore_topics_references: bool = True
    ):
        for key in ["name", "description", "tags", "private"]:
            if old_topic[key] != new_topic[key]:
                logging.info(
                    f"{key} is different : {old_topic[key]} != {new_topic[key]}"
                )
                return False

        if old_topic["organization"]["id"] != new_topic["organization"]["id"]:
            logging.info(
                f"organization is different : {old_topic['organization']['id']} != {new_topic['organization']['id']}"
            )
            return False

        if ignore_topics_references:
            # Don't compare the keys of the old_topic that will be updated later by update_topics_references()
            # We need to deepcopy the extras because the nested_pop() method modifies pointers in the extras dictionaries
            old_topic_extras = deepcopy(old_topic["extras"])
            new_topic_extras = deepcopy(new_topic["extras"])
            extra_key = list(old_topic["extras"].keys())[0]

            for keychain in TOPICS_REFERENCES_ATTRIBUTES[extra_key]:
                self._nested_pop(old_topic_extras, keychain)
                self._nested_pop(new_topic_extras, keychain)
        else:
            old_topic_extras = old_topic["extras"]
            new_topic_extras = new_topic["extras"]

        return old_topic_extras == new_topic_extras

    def _update_topic_if_needed(
        self, topic: dict, topic_data: dict, ignore_topics_references: bool = True
    ):
        if self._topics_are_similar_so_we_can_skip_update(
            topic, topic_data, ignore_topics_references
        ):
            logging.info(
                f"Topic hasn't changed, skipping update of slug: {topic['slug']}"
            )
            return
        return self.topics_api.update_topic_by_id(topic["id"], topic_data)

    # We need to send the tags when updating the extras otherwise it fails
    def _update_extras_of_topic_if_needed(self, topic: dict, new_extras: dict):
        # no need to use the complicated _topics_are_similar_so_we_can_skip_update() method here
        # because this method is called in update_topics_references()
        # so the extras are complete and can be compared simply
        if topic["extras"] == new_extras:
            logging.info(
                f"Topic hasn't changed, skipping update of slug: {topic['slug']}"
            )
            return

        return self.topics_api.update_topic_by_id(
            topic["id"],
            {
                "tags": topic["tags"],
                "extras": new_extras,
            },
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

            current_topics_by_simplifions_slug = {
                topic["extras"][extras_nested_key]["slug"]: topic
                for topic in self.topics_api.get_all_topics_for_tag(tag)
            }

            logging.info(
                f"Found {len(current_topics_by_simplifions_slug)} existing topics in datagouv for tag {tag}"
            )

            for simplifions_slug in grist_topics:
                topic_data = {
                    "name": grist_topics[simplifions_slug][title_column],
                    # description cannot be empty
                    "description": grist_topics[simplifions_slug]["Description_courte"]
                    or "-",
                    "organization": {
                        "class": "Organization",
                        "id": "57fe2a35c751df21e179df72",
                    },
                    "tags": topic_tags
                    + self._generated_search_tags(grist_topics[simplifions_slug]),
                    "extras": {
                        extras_nested_key: grist_topics[simplifions_slug] or False
                    },
                    "private": not grist_topics[simplifions_slug][
                        "Visible_sur_simplifions"
                    ],
                }

                # Create or update the topic
                if simplifions_slug in current_topics_by_simplifions_slug:
                    old_topic = current_topics_by_simplifions_slug[simplifions_slug]
                    self._update_topic_if_needed(
                        old_topic, topic_data, ignore_topics_references=True
                    )
                else:
                    logging.info(
                        f"Creating topic {simplifions_slug} : {topic_data['name']}"
                    )
                    self.topics_api.create_topic(topic_data)

            # deleting topics that are not in the table anymore
            for simplifions_slug in current_topics_by_simplifions_slug:
                if simplifions_slug not in grist_topics:
                    old_topic = current_topics_by_simplifions_slug[simplifions_slug]
                    logging.info(
                        f"Deleting topic {simplifions_slug} : {old_topic['id']}"
                    )
                    self.topics_api.delete_topic(old_topic["id"])

    def update_topics_references(self, ti):
        all_topics = {}
        all_tags = ["simplifions-solutions", "simplifions-cas-d-usages"]

        for tag in all_tags:
            all_topics[tag] = list(self.topics_api.get_all_topics_for_tag(tag))
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

            new_extras = deepcopy(solution_topic["extras"])
            new_extras["simplifions-solutions"]["cas_d_usages_topics_ids"] = [
                topic["id"] for topic in matching_topics
            ]
            # update the solution topic with the new extras
            self._update_extras_of_topic_if_needed(solution_topic, new_extras)

        # Update cas_usages_topics with references to solution_topic_id and solutions_editeurs_topics
        for cas_usage_topic in cas_usages_topics:
            new_extras = deepcopy(cas_usage_topic["extras"])
            for reco in new_extras["simplifions-cas-d-usages"]["reco_solutions"]:
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
            self._update_extras_of_topic_if_needed(cas_usage_topic, new_extras)
