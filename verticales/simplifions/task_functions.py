from collections import defaultdict
from datetime import datetime, timedelta
import logging

from airflow.decorators import task

from datagouvfr_data_pipelines.utils.mattermost import send_message
from datagouvfr_data_pipelines.verticales.simplifions.grist_v2_manager import (
    GristV2Manager,
)
from datagouvfr_data_pipelines.verticales.simplifions.topics_v2_manager import (
    TopicsV2Manager,
)
from datagouvfr_data_pipelines.verticales.simplifions.diff_manager import (
    DiffManager,
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
    "Categories_de_solution",
]


@task()
def get_and_format_grist_v2_data(**context):
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

    context["ti"].xcom_push(key="tag_and_grist_rows_v2", value=tag_and_grist_topics)

    logging.info("Get grist_tables_for_filters...")

    grist_tables_for_filters = {}
    for table_id in GRIST_TABLES_FOR_FILTERS:
        rows = GristV2Manager._request_grist_table(table_id)
        grist_tables_for_filters[table_id] = [
            GristV2Manager._clean_row(row) for row in rows
        ]

    logging.info("grist_tables_for_filters done.")

    context["ti"].xcom_push(
        key="grist_tables_for_filters", value=grist_tables_for_filters
    )


@task()
def update_topics_v2(client=None, **context):
    topics_api = TopicsAPI(client)

    tag_and_grist_rows: dict = context["ti"].xcom_pull(
        key="tag_and_grist_rows_v2", task_ids="get_and_format_grist_v2_data"
    )

    grist_tables_for_filters: dict = context["ti"].xcom_pull(
        key="grist_tables_for_filters", task_ids="get_and_format_grist_v2_data"
    )
    topics_manager = TopicsV2Manager(client, grist_tables_for_filters)

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

            # Skip if the name is empty
            if not grist_row["fields"]["Nom"]:
                logging.info(
                    f"⚠️ Skipping topic {tag} with grist_id: {grist_id} because of empty name ⚠️"
                )
                continue

            all_tags = (
                topic_tags
                + [f"{tag}-{grist_id_str}"]
                + topics_manager._generated_search_tags(grist_row)
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
                    f"Creating topic {tag} with grist_id: {grist_id}, name: {topic_data['name']}"
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


@task()
def watch_grist_data():
    """
    Watch Grist data for changes and log the current state.
    This method monitors the Grist tables and provides information about data changes.
    """
    logging.info("Grist data watch started")
    all_tables = GristV2Manager._request_all_tables()
    time_delta = 24  # in hours
    cutoff_timestamp = (datetime.now() - timedelta(hours=time_delta)).timestamp()
    tables_with_modified_rows = {}
    tables_with_deleted_rows = {}

    # Get the last backup for diffing
    last_backup = DiffManager._get_last_simplifions_backup()
    last_backup_id = last_backup["id"]
    logging.info(f"Using last backup: {last_backup_id}, name: {last_backup['name']}")

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

        # Check for deleted rows
        all_current_ids = {row["id"] for row in all_rows}
        all_backup_rows = GristV2Manager._request_table_records(
            table["id"], document_id=last_backup_id
        )
        deleted_rows = [
            row
            for row in all_backup_rows
            if row["id"] not in all_current_ids
            and row["fields"].get("Modifie_le")
            and row["fields"]["Modifie_le"] > cutoff_timestamp
        ]
        if deleted_rows:
            logging.info(f"> Found {len(deleted_rows)} deleted rows")
            tables_with_deleted_rows[table["id"]] = deleted_rows

    if not tables_with_modified_rows and not tables_with_deleted_rows:
        logging.info("No modified or deleted rows found")
        return

    # Format the message

    tables_messages = []
    message = "# Modifications du grist Simplifions\n"
    message += f"Les données suivantes ont reçu des modifications pendant les {time_delta} dernières heures:\n"
    message += f"Diff effectué avec le backup [{last_backup['name']}](https://grist.numerique.gouv.fr/o/circulation/{last_backup['id']}) (GMT)\n\n\n"

    logging.info("\n")
    logging.info("Processing diffs...")

    # Get all tables that need to be processed (either modified or deleted rows)
    all_table_ids = set(tables_with_modified_rows.keys()) | set(
        tables_with_deleted_rows.keys()
    )

    for table_id in all_table_ids:
        modified_rows = tables_with_modified_rows.get(table_id, [])
        deleted_rows = tables_with_deleted_rows.get(table_id, [])

        logging.info(
            f"Processing table {table_id} with {len(modified_rows)} modified rows "
            f"and {len(deleted_rows)} deleted rows"
        )

        table_message = f"### **{table_id}**\n"

        # Process modified rows if any
        if modified_rows:
            # Get all backup rows (used for diff comparison)
            all_backup_rows = GristV2Manager._request_table_records(
                table_id, document_id=last_backup_id
            )

            modified_rows_grouped_by_modifier = defaultdict(list)
            for row in modified_rows:
                modified_rows_grouped_by_modifier[row["fields"]["Modifie_par"]].append(
                    row
                )

            for modifier, rows in modified_rows_grouped_by_modifier.items():
                table_message += f"- **{modifier}**\n"
                sorted_rows = sorted(
                    rows, key=lambda row: row["fields"]["technical_title"]
                )

                for row in sorted_rows:
                    # Find the corresponding backup row from all backup rows
                    backup_row = next(
                        (r for r in all_backup_rows if r["id"] == row["id"]), None
                    )
                    new_line_prefix = "" if backup_row else "(Nouvelle ligne) "
                    table_message += (
                        f"  - {new_line_prefix}{DiffManager.format_row_link(row)}\n"
                    )

                    if not backup_row:
                        continue

                    diff = DiffManager.get_diff(row["fields"], backup_row["fields"])
                    if diff:
                        for key, value in diff.items():
                            table_message += f"    - `{key}`:\n"
                            table_message += f"      - :heavy_minus_sign: {DiffManager.format_diff_value(value['old'])}\n"
                            table_message += f"      - :heavy_plus_sign: {DiffManager.format_diff_value(value['new'])}\n"
                    else:
                        table_message += "    - _(Aucune modification détectée)_\n"

        # Add deleted rows section (not grouped by modifier)
        if deleted_rows:
            table_message += (
                "\n- **Lignes supprimées** (liens vers le grist de backup)\n"
            )
            sorted_deleted_rows = sorted(
                deleted_rows, key=lambda row: row["fields"]["technical_title"]
            )
            for row in sorted_deleted_rows:
                table_message += f"  - ~~{DiffManager.format_row_link(row)}~~\n"

        tables_messages.append(table_message + "\n")

    message += "\n".join(tables_messages)
    logging.info(message)

    # Send the message

    send_message(
        text=message,
        endpoint_url=MATTERMOST_SIMPLIFIONS_WEBHOOK_URL,
    )


@task()
def clone_grist_document():
    logging.info("Cloning grist document")
    new_doc_info = DiffManager._create_simplifions_backup()
    logging.info(
        f"Grist document cloned. Id: {new_doc_info['id']}, name: {new_doc_info['name']}"
    )

    logging.info("Cleaning old backups")
    number_of_backups = 7
    backups = DiffManager._list_simplifions_backups()
    logging.info(f"Found {len(backups)} backups")

    sorted_backups = sorted(backups, key=lambda x: x["name"], reverse=True)
    obsolete_backups = sorted_backups[number_of_backups:]
    for backup in obsolete_backups:
        logging.info(f"Deleting backup: {backup['id']}, name: {backup['name']}")
        GristV2Manager._delete_document(backup["id"])
    logging.info(f"{len(obsolete_backups)} obsolete backups deleted.")
