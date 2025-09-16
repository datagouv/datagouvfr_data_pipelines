import logging
import requests
from collections import defaultdict

from datagouvfr_data_pipelines.config import (
    GRIST_API_URL,
    SECRET_GRIST_API_KEY,
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

    def _request_grist_table(self, table_id: str, filter: str = None) -> list[dict]:
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
            rows = self._request_grist_table(table_id)

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
            rows = self._request_grist_table(table_id)
            grist_tables_for_filters[table_id] = rows

        logging.info("grist_tables_for_filters done.")

        ti.xcom_push(key="grist_tables_for_filters", value=grist_tables_for_filters)
