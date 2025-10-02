import requests

from datagouvfr_data_pipelines.config import (
    GRIST_API_URL,
    SECRET_GRIST_API_KEY,
)

GRIST_DOC_ID = "ofSVjCSAnMb6"


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

    @staticmethod
    def _boldify_last_section(description: str) -> str:
        if " > " not in description:
            return description
        first_section = description.split(" > ")[0]
        last_section = description.split(" > ")[-1]
        return f"{first_section} > **{last_section}**"
