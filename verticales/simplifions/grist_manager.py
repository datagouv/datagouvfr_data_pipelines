import json
import logging

import requests

from datagouvfr_data_pipelines.config import (
    GRIST_API_URL,
    SECRET_GRIST_API_KEY,
)

GRIST_DOC_ID = "c5pt7QVcKWWe"
GRIST_TABLES_AND_TAGS = {
    "SIMPLIFIONS_cas_usages": {
        "tag": "simplifions-cas-d-usages",
        "sub_tables": {
            "reco_solutions": "SIMPLIFIONS_reco_solutions_cas_usages",
            "API_et_donnees_utiles": "Apidata",
            "descriptions_api_et_donnees_utiles": "SIMPLIFIONS_description_apidata_cas_usages",
        },
    },
    "SIMPLIFIONS_produitspublics": {
        "tag": "simplifions-solutions",
        "sub_tables": {"API_et_data_disponibles": "Apidata"},
    },
    "SIMPLIFIONS_solutions_editeurs": {
        "tag": "simplifions-solutions",
        "sub_tables": {"API_et_data_disponibles": "Apidata"},
    },
}


class GristManager:
    def __init__(self):
        pass

    @staticmethod
    def request_grist_table(table_id: str, filter: str = None) -> list[dict]:
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
        return [row["fields"] for row in r.json()["records"]]

    @staticmethod
    def clean_row(row: dict) -> dict:
        cleaned_row = {}
        for key, value in row.items():
            if isinstance(value, list) and value and value[0] == "L":
                # Remove the "L" prefix and keep the rest of the list
                cleaned_row[key] = value[1:]
            else:
                cleaned_row[key] = value
        return cleaned_row

    def get_subdata(self, key: str, value: list | str, table_info: dict) -> list | str:
        if not value:
            return value
        elif table_info["sub_tables"] and key in table_info["sub_tables"].keys():
            value_as_list = value if isinstance(value, list) else [value]
            filter = json.dumps({"id": value_as_list})
            subdata = self.request_grist_table(
                table_info["sub_tables"][key], filter=filter
            )
            cleaned_subdata = [
                self.clean_row(item)
                for item in subdata
                if item.get("Visible_sur_simplifions", True)
            ]
            return cleaned_subdata if isinstance(value, list) else cleaned_subdata[0]
        else:
            return value

    def cleaned_row_with_subdata(self, row: dict, table_info: dict) -> dict:
        cleaned_row = self.clean_row(row)
        formatted_row = {
            key: self.get_subdata(key, cleaned_row[key], table_info)
            for key in cleaned_row.keys()
        }
        return formatted_row

    def get_and_format_grist_data(self, ti):
        tag_and_grist_topics = {}

        for table_id, table_info in GRIST_TABLES_AND_TAGS.items():
            tag = table_info["tag"]
            rows = self.request_grist_table(table_id)

            if tag not in tag_and_grist_topics:
                tag_and_grist_topics[tag] = {}

            tag_and_grist_topics[tag].update(
                {
                    row["slug"]: self.cleaned_row_with_subdata(row, table_info)
                    for row in rows
                    if row["slug"]
                }
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

        ti.xcom_push(key="tag_and_grist_topics", value=tag_and_grist_topics)
