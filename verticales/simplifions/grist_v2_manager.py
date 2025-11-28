import requests

from datagouvfr_data_pipelines.config import (
    GRIST_API_URL,
    SECRET_GRIST_API_KEY,
)

GRIST_DOC_ID = "ofSVjCSAnMb6SGZSb7GGrv"
GRIST_WORKSPACE_ID = 51287


class GristV2Manager:
    def __init__(self):
        pass

    @staticmethod
    def _request_grist_table(table_id: str, filter: str | None = None) -> list[dict]:
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
    def _request_table_records(
        table_id: str, filter: str | None = None, document_id: str = GRIST_DOC_ID
    ) -> list[dict]:
        r = requests.get(
            GRIST_API_URL + f"docs/{document_id}/tables/{table_id}/records",
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
    def _copy_document(document_name: str, as_template: bool = False) -> dict:
        r = requests.post(
            GRIST_API_URL + f"docs/{GRIST_DOC_ID}/copy",
            headers={
                "Authorization": "Bearer " + SECRET_GRIST_API_KEY,
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            json={
                "workspaceId": GRIST_WORKSPACE_ID,
                "documentName": document_name,
                "asTemplate": as_template,
            },
        )
        r.raise_for_status()
        return r.json()

    @staticmethod
    def _list_workspace_documents() -> dict:
        r = requests.get(
            GRIST_API_URL + f"workspaces/{GRIST_WORKSPACE_ID}",
            headers={
                "Authorization": "Bearer " + SECRET_GRIST_API_KEY,
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        )
        r.raise_for_status()
        return r.json()["docs"]

    @staticmethod
    def _delete_document(document_id: str) -> dict:
        r = requests.delete(
            GRIST_API_URL + f"docs/{document_id}",
            headers={
                "Authorization": "Bearer " + SECRET_GRIST_API_KEY,
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        )
        r.raise_for_status()
        return r.json()

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
