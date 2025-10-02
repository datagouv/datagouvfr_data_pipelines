import logging
from collections import defaultdict
from datetime import datetime, timedelta

from datagouvfr_data_pipelines.utils.mattermost import send_message
from datagouvfr_data_pipelines.verticales.simplifions.grist_v2_manager import (
    GristV2Manager,
)
from datagouvfr_data_pipelines.verticales.simplifions.topics_v2_manager import (
    TopicsV2Manager,
)
from datagouvfr_data_pipelines.verticales.simplifions.topics_api import TopicsAPI
from datagouvfr_data_pipelines.config import (
    MATTERMOST_SIMPLIFIONS_WEBHOOK_URL,
)

# Grist configuration
GRIST_TABLES_AND_TAGS = {
    "Cas_d_usages": {
        "tag": "simplifions-v2-cas-d-usages",
    },
    "Solutions": {
        "tag": "simplifions-v2-solutions",
    },
}

GRIST_TABLES_FOR_FILTERS = [
    "Fournisseurs_de_services",
    "Types_de_simplification",
    "Usagers",
    "Budgets_de_mise_en_oeuvre",
]


def get_and_format_grist_v2_data(ti, client=None):
    tag_and_grist_topics = defaultdict(dict)

    for table_id, table_info in GRIST_TABLES_AND_TAGS.items():
        tag = table_info["tag"]
        rows = GristV2Manager._request_grist_table(table_id)

        tag_and_grist_topics[tag].update(
            {row["id"]: GristV2Manager._clean_row(row) for row in rows}
        )

    logging_str = "\n".join(
        [
            f"{tag}: {len(grist_topics)} topics"
            for tag, grist_topics in tag_and_grist_topics.items()
        ]
    )
    total_length = sum(
        [len(grist_topics) for grist_topics in tag_and_grist_topics.values()]
    )
    logging.info(f"Found {total_length} items in grist: \n{logging_str}")

    ti.xcom_push(key="tag_and_grist_rows_v2", value=tag_and_grist_topics)

    logging.info("Get grist_tables_for_filters...")

    grist_tables_for_filters = {}
    for table_id in GRIST_TABLES_FOR_FILTERS:
        rows = GristV2Manager._request_grist_table(table_id)
        grist_tables_for_filters[table_id] = rows

    logging.info("grist_tables_for_filters done.")

    ti.xcom_push(key="grist_tables_for_filters", value=grist_tables_for_filters)


def update_topics_v2(ti, client=None):
    topics_manager = TopicsV2Manager(client)
    topics_api = TopicsAPI(client)

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

        # Normalize grist_rows keys to integers (handles both string keys from JSON and int keys from tests)
        grist_rows = {int(k): v for k, v in grist_rows.items()}

        extras_nested_key = tag
        topic_tags = simplifions_tags + [tag]

        current_topics_by_grist_id = {}
        for topic in topics_api.get_all_topics_for_tag(tag):
            # Handle both old and new extras structure
            if extras_nested_key in topic["extras"]:
                grist_id = topic["extras"][extras_nested_key]["id"]
            else:
                # Fallback for old structure
                grist_id = topic["extras"]["id"]
            current_topics_by_grist_id[grist_id] = topic

        logging.info(
            f"Found {len(current_topics_by_grist_id)} existing topics in datagouv for tag {tag}"
        )

        for grist_id_str in grist_rows:
            grist_row = grist_rows[grist_id_str]
            grist_id = int(grist_id_str)
            all_tags = (
                topic_tags
                + [f"{tag}-{grist_id_str}"]
                + topics_manager._generated_search_tags(
                    grist_row, grist_tables_for_filters
                )
            )

            topic_data = {
                "name": topics_manager._topic_name(grist_row),
                "description": grist_row["fields"]["Description_courte"] or "-",
                "organization": {
                    "class": "Organization",
                    "id": "57fe2a35c751df21e179df72",
                },
                "tags": all_tags,
                "extras": {extras_nested_key: topics_manager._topic_extras(grist_row)},
                "private": not grist_row["fields"]["Visible_sur_simplifions"],
            }

            # Create or update the topic
            if grist_id in current_topics_by_grist_id:
                old_topic = current_topics_by_grist_id[grist_id]
                topics_manager._update_topic_if_needed(old_topic, topic_data)
            else:
                logging.info(
                    f"Creating topic grist_id: {grist_id}, name: {topic_data['name']}"
                )
                topics_api.create_topic(topic_data)

        # deleting topics that are not in the table anymore
        for grist_id in current_topics_by_grist_id:
            if grist_id not in grist_rows:
                old_topic = current_topics_by_grist_id[grist_id]
                logging.info(
                    f"Deleting topic grist_id: {grist_id}, slug: {old_topic['slug']}"
                )
                topics_api.delete_topic(old_topic["id"])


def watch_grist_data(ti):
    """
    Watch Grist data for changes and log the current state.
    This method monitors the Grist tables and provides information about data changes.
    """
    logging.info("Grist data watch started")
    all_tables = GristV2Manager._request_all_tables()
    time_delta = 24  # in hours
    cutoff_timestamp = (datetime.now() - timedelta(hours=time_delta)).timestamp()
    tables_with_modified_rows = {}

    # Get all tables data

    for table in all_tables:
        logging.info("\n")
        logging.info(f"Table: {table['id']}")
        table_columns = GristV2Manager._request_table_columns(table["id"])

        if "Modifie_le" not in [column["id"] for column in table_columns]:
            logging.info("> Does not have a 'Modifie_le' column. Skipping.")
            continue

        all_rows = GristV2Manager._request_grist_table(table["id"])
        logging.info(f"> Found {len(all_rows)} rows total")

        modified_rows = [
            row
            for row in all_rows
            if row["fields"].get("Modifie_le")
            and row["fields"]["Modifie_le"] > cutoff_timestamp
        ]

        if modified_rows:
            logging.info(
                f"> Found {len(modified_rows)} modified rows since {time_delta} hours ago"
            )
            tables_with_modified_rows[table["id"]] = modified_rows
        else:
            logging.info(f"> No modified rows found for table {table['id']}")

    # Format the message

    tables_messages = []
    message = "# Modifications du grist Simplifions\n"
    message += f"Les données suivantes ont reçu des modifications pendant les {time_delta} dernières heures:\n\n\n"

    for table_id, modified_rows in tables_with_modified_rows.items():
        modified_rows_grouped_by_modifier = defaultdict(list)
        for row in modified_rows:
            modified_rows_grouped_by_modifier[row["fields"]["Modifie_par"]].append(row)

        table_message = f"### **{table_id}**\n"

        for modifier, rows in modified_rows_grouped_by_modifier.items():
            table_message += f"- **{modifier}**\n"
            for row in rows:
                table_message += f"  - {GristV2Manager._boldify_last_section(row['fields']['technical_title'])}\n"

        tables_messages.append(table_message + "\n")

    message += "\n".join(tables_messages)
    logging.info(message)

    # Send the message

    send_message(
        text=message,
        endpoint_url=MATTERMOST_SIMPLIFIONS_WEBHOOK_URL,
    )
