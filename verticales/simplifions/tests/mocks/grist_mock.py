from unittest.mock import Mock
from .external_resources_mock import ExternalResourcesMock
import requests_mock
import re
import json


class GristMock(ExternalResourcesMock):
    def mock_environment_variables(self, config_mock: Mock):
        config_mock.GRIST_API_URL = "https://grist.example.com/api/"
        config_mock.SECRET_GRIST_API_KEY = "test-api-key"

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
