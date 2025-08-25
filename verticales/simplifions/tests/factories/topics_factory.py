from mocks.datagouvfr_mock import DatagouvfrMock
from factories.external_resources_factory import ExternalResourcesFactory
from copy import deepcopy

datagouvfr_mock = DatagouvfrMock()


class TopicsFactory(ExternalResourcesFactory):
    def _initialize_resources_mocks(self):
        """Initialize the single 'topics' resource in the mock instead of separate resources per fixture"""
        mock = self.resource_mock()
        mock.initialize_resource_records("topics")
        mock.mock_resource_list("topics", mock.get_records("topics"))
        mock.mock_resource_creation("topics")
        mock.mock_resource_update("topics")
        mock.mock_resource_delete("topics")

    def fixture_folder(self):
        return "topics"

    def resource_mock(self):
        return datagouvfr_mock

    def build_record(self, resource_name: str, data: dict = None) -> dict:
        """
        Override to handle all topic resources as a single 'topics' resource.
        The resource_name is used to determine the appropriate fixture.
        """
        if resource_name not in self.fixtures_per_resource:
            raise ValueError(f"Unknown resource: {resource_name}")

        record = deepcopy(self.fixtures_per_resource[resource_name])
        if data:
            self._deep_update(record, data)
        return record

    def create_record(self, resource_name: str, data: dict = None) -> dict:
        """
        Override to store all records under the 'topics' resource regardless of their original type.
        """
        record = self.build_record(resource_name, data)
        mock = self.resource_mock()
        mock.add_record("topics", record)
        return record
