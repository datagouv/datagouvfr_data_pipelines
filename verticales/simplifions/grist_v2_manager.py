import logging
import requests
from collections import defaultdict
from datetime import datetime, timedelta
from datagouvfr_data_pipelines.utils.mattermost import send_message

from datagouvfr_data_pipelines.config import (
    GRIST_API_URL,
    SECRET_GRIST_API_KEY,
    MATTERMOST_SIMPLIFIONS_WEBHOOK_URL,
)

GRIST_DOC_ID = "ofSVjCSAnMb6"
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


class GristV2Manager:
    def __init__(self):
        pass

    @staticmethod
    def _request_grist_table(table_id: str, filter: str = None) -> list[dict]:
        r = requests.get(
            GRIST_API_URL + f"docs/{GRIST_DOC_ID}/tables/{table_id}/records",
            headers={
                "Authorization": "Bearer " + SECRET_GRIST_API_KEY,
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            params={"filter": filter} if filter else None,
        )
        r.raise_for_status()
        return r.json()["records"]

    @staticmethod
    def _request_all_tables() -> dict:
        r = requests.get(
            GRIST_API_URL + f"docs/{GRIST_DOC_ID}/tables",
            headers={
                "Authorization": "Bearer " + SECRET_GRIST_API_KEY,
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        )
        r.raise_for_status()
        return r.json()["tables"]

    @staticmethod
    def _request_table_columns(table_id: str) -> dict:
        r = requests.get(
            GRIST_API_URL + f"docs/{GRIST_DOC_ID}/tables/{table_id}/columns",
            headers={
                "Authorization": "Bearer " + SECRET_GRIST_API_KEY,
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        )
        r.raise_for_status()
        return r.json()["columns"]

    @staticmethod
    def _clean_row(row: dict) -> dict:
        cleaned_fields = {}
        for key, value in row["fields"].items():
            if isinstance(value, list) and value and value[0] == "L":
                # Remove the "L" prefix and keep the rest of the list
                cleaned_fields[key] = value[1:]
            else:
                cleaned_fields[key] = value
        row["fields"] = cleaned_fields
        return row

    def get_and_format_grist_v2_data(self, ti):
        tag_and_grist_topics = defaultdict(dict)

        for table_id, table_info in GRIST_TABLES_AND_TAGS.items():
            tag = table_info["tag"]
            rows = GristV2Manager._request_grist_table(table_id)

            tag_and_grist_topics[tag].update(
                {row["id"]: self._clean_row(row) for row in rows}
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

    def watch_grist_data(self, ti):
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
                modified_rows_grouped_by_modifier[row["fields"]["Modifie_par"]].append(
                    row
                )

            table_message = f"### **{table_id}**\n"

            for modifier, rows in modified_rows_grouped_by_modifier.items():
                table_message += f"- **{modifier}**\n"
                for row in rows:
                    table_message += f"  - {row['fields']['technical_title']}\n"

            tables_messages.append(table_message + "\n")

        message += "\n".join(tables_messages)
        logging.info(message)

        # Send the message

        send_message(
            text=message,
            endpoint_url=MATTERMOST_SIMPLIFIONS_WEBHOOK_URL,
        )
