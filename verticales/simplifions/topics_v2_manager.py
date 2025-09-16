import logging

from datagouv import Client
from datagouvfr_data_pipelines.verticales.simplifions.topics_api import TopicsAPI

# These attributes are used to generate tags for the topics filters
ATTRIBUTES_FOR_TAGS = {
    "Budget_requis": {"table_id": "Budgets_de_mise_en_oeuvre", "filter_slug": "budget"},
    "Types_de_simplification": {
        "table_id": "Types_de_simplification",
        "filter_slug": "types-de-simplification",
    },
    "A_destination_de": {
        "table_id": "Fournisseurs_de_services",
        "filter_slug": "fournisseurs-de-service",
    },
    "Pour_simplifier_les_demarches_de": {
        "table_id": "Usagers",
        "filter_slug": "target-users",
    },
}


class TopicsV2Manager:
    def __init__(self, client: Client):
        self.topics_api = TopicsAPI(client)

    def _generated_search_tags(
        self, grist_row: dict, grist_tables_for_filters: dict
    ) -> list[str]:
        tags = []
        for attribute, table_info in ATTRIBUTES_FOR_TAGS.items():
            values = grist_row["fields"].get(attribute)
            if values:
                table_values = grist_tables_for_filters[table_info["table_id"]]
                filter_slug = table_info["filter_slug"]

                if not isinstance(values, list):
                    values = [values]

                for value in values:
                    table_value = next(
                        (x for x in table_values if x["id"] == value), None
                    )
                    if not table_value:
                        raise ValueError(
                            f"Value '{value}' not found in table {table_info['table_id']}"
                        )
                    value_slug = table_value["fields"]["slug"]
                    tags.append(f"simplifions-v2-{filter_slug}-{value_slug}")
        return tags

    def _topics_are_similar_so_we_can_skip_update(
        self, old_topic: dict, new_topic: dict
    ) -> bool:
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

        return old_topic["extras"] == new_topic["extras"]

    def _update_topic_if_needed(self, topic: dict, topic_data: dict):
        if self._topics_are_similar_so_we_can_skip_update(topic, topic_data):
            logging.info(
                f"Topic hasn't changed, skipping update of topic with slug: {topic['slug']}"
            )
            return
        return self.topics_api.update_topic_by_id(topic["id"], topic_data)

    def _topic_name(self, grist_row: dict) -> str:
        icon = grist_row["fields"].get("Icone_du_titre")
        if icon:
            return f"{icon} {grist_row['fields']['Nom']}"
        return grist_row["fields"]["Nom"]

    def _add_attribute_if_it_exists(
        self, source_dict: dict, target_dict: dict, attribute_name: str
    ):
        attribute_value = source_dict.get(attribute_name)
        if attribute_value:
            target_dict[attribute_name] = attribute_value

    def _topic_extras(self, grist_row: dict) -> dict:
        extras = {
            "id": grist_row["id"],
        }
        self._add_attribute_if_it_exists(grist_row["fields"], extras, "Image")
        self._add_attribute_if_it_exists(grist_row["fields"], extras, "Public_ou_prive")
        self._add_attribute_if_it_exists(
            grist_row["fields"], extras, "Nom_de_l_operateur"
        )

        return extras

    def update_topics(self, ti):
        tag_and_grist_rows: dict = ti.xcom_pull(
            key="tag_and_grist_rows_v2", task_ids="get_and_format_grist_v2_data"
        )

        grist_tables_for_filters: dict = ti.xcom_pull(
            key="grist_tables_for_filters", task_ids="get_and_format_grist_v2_data"
        )

        simplifions_tags = [
            "simplifions-v2",
            "simplifions-v2-dag-generated",
        ]

        for tag, grist_rows in tag_and_grist_rows.items():
            logging.info(f"\n\n\nUpdating {len(grist_rows)} topics for tag: {tag}")

            extras_nested_key = tag
            topic_tags = simplifions_tags + [tag]

            current_topics_by_grist_id = {
                topic["extras"][extras_nested_key]["id"]: topic
                for topic in self.topics_api.get_all_topics_for_tag(tag)
            }

            logging.info(
                f"Found {len(current_topics_by_grist_id)} existing topics in datagouv for tag {tag}"
            )

            for grist_id_str in grist_rows:
                grist_row = grist_rows[grist_id_str]
                grist_id = int(grist_id_str)
                all_tags = (
                    topic_tags
                    + [f"{tag}-{grist_id_str}"]
                    + self._generated_search_tags(grist_row, grist_tables_for_filters)
                )

                topic_data = {
                    "name": self._topic_name(grist_row),
                    "description": grist_row["fields"]["Description_courte"] or "-",
                    "organization": {
                        "class": "Organization",
                        "id": "57fe2a35c751df21e179df72",
                    },
                    "tags": all_tags,
                    "extras": {extras_nested_key: self._topic_extras(grist_row)},
                    "private": not grist_row["fields"]["Visible_sur_simplifions"],
                }

                # Create or update the topic
                if grist_id in current_topics_by_grist_id:
                    old_topic = current_topics_by_grist_id[grist_id]
                    self._update_topic_if_needed(old_topic, topic_data)
                else:
                    logging.info(
                        f"Creating topic grist_id: {grist_id}, name: {topic_data['name']}"
                    )
                    self.topics_api.create_topic(topic_data)

            # deleting topics that are not in the table anymore
            for grist_id in current_topics_by_grist_id:
                grist_id_str = str(grist_id)
                if grist_id_str not in grist_rows:
                    old_topic = current_topics_by_grist_id[grist_id]
                    logging.info(
                        f"Deleting topic grist_id: {grist_id}, slug: {old_topic['slug']}"
                    )
                    self.topics_api.delete_topic(old_topic["id"])
