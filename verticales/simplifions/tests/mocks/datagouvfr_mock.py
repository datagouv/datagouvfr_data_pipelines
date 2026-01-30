import re
from urllib.parse import urlparse, parse_qs

import httpx
import respx
import requests_mock

from .external_resources_mock import ExternalResourcesMock


class DatagouvfrMock(ExternalResourcesMock):
    _respx_mock = None

    def __init__(self):
        super().__init__()
        if DatagouvfrMock._respx_mock is None:
            DatagouvfrMock._respx_mock = respx.mock(assert_all_called=False)
            DatagouvfrMock._respx_mock.start()

    def match_resource_list_url(self, resource_name: str):
        return f"https://demo.data.gouv.fr/api/2/{resource_name}"

    def match_resource_creation_url(self, resource_name: str):
        return f"https://demo.data.gouv.fr/api/2/{resource_name}"

    def match_resource_url(self, resource_name: str):
        return re.compile(rf"https://demo\.data\.gouv\.fr/api/2/{resource_name}/.+")

    def build_filtered_response_for_records(
        self, records: list[dict], request: requests_mock.Mocker
    ):
        filtered_records = self._filter_records(records, request.qs)
        return self._create_response_for_records(filtered_records)

    def _filter_records(self, records: list[dict], qs: dict):
        filter_param = qs.get("tag", [None])[0]
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

    def stop_data_mocks(self):
        super().stop_data_mocks()
        if DatagouvfrMock._respx_mock:
            DatagouvfrMock._respx_mock.stop()
            DatagouvfrMock._respx_mock = None

    def mock_resource_list(self, resource_name: str, records: list[dict]):
        super().mock_resource_list(resource_name, records)

        base_url = self.match_resource_list_url(resource_name)

        def httpx_side_effect(request):
            qs = parse_qs(str(request.url).split("?", 1)[-1]) if "?" in str(request.url) else {}
            filtered_records = self._filter_records(records, qs)
            return httpx.Response(200, json=self._create_response_for_records(filtered_records))

        DatagouvfrMock._respx_mock.get(url=re.compile(rf"^{re.escape(base_url)}")).mock(
            side_effect=httpx_side_effect
        )
