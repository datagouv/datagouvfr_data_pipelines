import json
import re

import requests_mock

from .external_resources_mock import ExternalResourcesMock


class GristMock(ExternalResourcesMock):
    def match_resource_list_url(self, resource_name: str):
        return re.compile(
            rf"https://grist\.example\.com/api/docs/[^/]+/tables/{re.escape(resource_name)}/records"
        )

    def match_resource_creation_url(self, resource_name: str):
        pass

    def match_resource_url(self, resource_name: str):
        pass

    def build_filtered_response_for_records(
        self, records: list[dict], request: requests_mock.Mocker
    ) -> dict:
        filtered_records = self._filter_records(records, request)
        return self._create_response_for_records(filtered_records)

    def _filter_records(
        self, records: list[dict], request: requests_mock.Mocker
    ) -> list[dict]:
        """Filter records based on the filter parameter"""
        filter_param = request.qs.get("filter", [None])[0]

        if not filter_param:
            return records

        try:
            filter_data = json.loads(filter_param)
            target_ids = filter_data.get("id", [])

            # Normalize to list
            if not isinstance(target_ids, list):
                target_ids = [target_ids]

            # Return records matching the IDs (ID = index + 1)
            return [records[id - 1] for id in target_ids if 1 <= id <= len(records)]

        except (json.JSONDecodeError, TypeError):
            return records

    def _create_response_for_records(self, records: list[dict]) -> dict:
        records_with_ids = [
            {"id": index + 1, "fields": record} for index, record in enumerate(records)
        ]
        return {"records": records_with_ids}

    def mock_table_metadata(self):
        """Mock the table metadata endpoints used by watch_grist_data"""

        # Mock the /tables endpoint to list all tables
        tables_response = {
            "tables": [
                {"id": "Cas_d_usages"},
                {"id": "Solutions"},
            ]
        }

        ExternalResourcesMock._data_mocker.register_uri(
            "GET",
            re.compile(r"https://grist\.example\.com/api/docs/[^/]+/tables$"),
            json=tables_response,
            status_code=200,
        )

        # Mock the /tables/{table_id}/columns endpoint to always return these columns
        columns_response = {
            "columns": [
                {"id": "technical_title", "type": "Text"},
                {"id": "Modifie_le", "type": "Numeric"},
                {"id": "Modifie_par", "type": "Text"},
            ]
        }

        ExternalResourcesMock._data_mocker.register_uri(
            "GET",
            re.compile(
                r"https://grist\.example\.com/api/docs/[^/]+/tables/[^/]+/columns$"
            ),
            json=columns_response,
            status_code=200,
        )

    def mock_workspace(self):
        workspace_response = {
            "id": "51287",
            "name": "Simplifions",
            "docs": [
                {
                    "id": "backup_1",
                    "name": "Simplifions Copy - 2025-10-16 10:00:00",
                },
                {
                    "id": "backup_2",
                    "name": "Simplifions Copy - 2025-10-17 10:00:00",
                },
            ],
        }

        ExternalResourcesMock._data_mocker.register_uri(
            "GET",
            re.compile(r"https://grist\.example\.com/api/workspaces/\d+$"),
            json=workspace_response,
            status_code=200,
        )
