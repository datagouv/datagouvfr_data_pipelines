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
                    # handle a list of `slugs` or a single `slug`
                    value_slugs_multiple = table_value["fields"].get("slugs")
                    value_slug_single = table_value["fields"].get("slug")
                    if value_slugs_multiple:
                        assert isinstance(value_slugs_multiple, list), "`slugs` should be a list"
                        for value_slug in value_slugs_multiple:
                            tags.append(f"simplifions-v2-{filter_slug}-{value_slug}")
                    elif value_slug_single:
                        tags.append(f"simplifions-v2-{filter_slug}-{value_slug_single}")

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
