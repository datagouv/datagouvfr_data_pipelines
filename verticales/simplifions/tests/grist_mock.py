from unittest.mock import Mock, patch
import requests_mock
import json

# Import the shared mocker from topics_mock
from topics_mock import get_shared_mocker

class GristMock:
    def __init__(self):
        self._config_mocker = None

    def mock_config(self):
        self.stop_config_mocks()
        config_mock = Mock()
        config_mock.GRIST_API_URL = "https://grist.example.com/api/"
        config_mock.SECRET_GRIST_API_KEY = "test-api-key"

        # Create specific mock modules with limited specs to avoid pytest issues
        mock_datagouvfr_root = Mock(spec=[])  # Empty spec means no dynamic attributes

        self._config_mocker = patch.dict(
            "sys.modules",
            {
                "datagouvfr_data_pipelines": mock_datagouvfr_root,
                "datagouvfr_data_pipelines.config": config_mock,
            },
        )
        self._config_mocker.start()

    def stop_config_mocks(self):
        if self._config_mocker:
            self._config_mocker.stop()
            self._config_mocker = None

    def mock_table_with_data(self, table_id: str, records: list[dict]):
        # Use the shared mocker
        shared_mocker = get_shared_mocker()

        # Create a callback function to handle filtering
        def response_callback(request, context):
            filter_param = request.qs.get("filter", [None])[0]
            filtered_records = self._filter_records(records, filter_param)
            response_data = self._create_grist_response_for_records(filtered_records)
            return json.dumps(response_data)

        import re

        url_pattern = re.compile(
            rf"https://grist\.example\.com/api/docs/[^/]+/tables/{re.escape(table_id)}/records"
        )

        # Register the callback for this table
        shared_mocker.register_uri("GET", url_pattern, text=response_callback)

    def stop_data_mocks(self):
        # Don't stop the shared mocker since other mocks might be using it
        pass

    def _filter_records(self, records: list[dict], filter_param: str) -> list[dict]:
        """Filter records based on the filter parameter"""
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

    def _create_grist_response_for_records(self, records: list[dict]) -> dict:
        records_with_ids = [
            {"id": index + 1, "fields": record} for index, record in enumerate(records)
        ]
        return {"records": records_with_ids}
