import re
from .external_resources_mock import ExternalResourcesMock
import requests_mock


class DatagouvfrMock(ExternalResourcesMock):
    def match_resource_list_url(self, resource_name: str):
        return f"https://demo.data.gouv.fr/api/1/{resource_name}/"

    def match_resource_creation_url(self, resource_name: str):
        return f"https://demo.data.gouv.fr/api/1/{resource_name}/"

    def match_resource_url(self, resource_name: str):
        return re.compile(rf"https://demo\.data\.gouv\.fr/api/1/{resource_name}/.+")

    def build_filtered_response_for_records(
        self, records: list[dict], request: requests_mock.Mocker
    ):
        filtered_records = self._filter_records(records, request)
        return self._create_response_for_records(filtered_records)

    def _filter_records(self, records: list[dict], request: requests_mock.Mocker):
        filter_param = request.qs.get("tag", [None])[0]
        if not filter_param:
            return records
        return [record for record in records if filter_param in record["tags"]]

    def _create_response_for_records(self, records: list[dict]):
        return {
            "data": records,
            "page": 1,
            "page_size": len(records),
            "total": len(records),
            "next_page": None,
        }
