import json
from copy import deepcopy
from pathlib import Path


class ExternalResourcesFactory:
    def __init__(self):
        self.fixtures_per_resource = self._load_all_fixtures()
        self._initialize_resources_mocks()

    def fixture_folder(self):
        raise NotImplementedError("Subclasses must implement this method")

    def resource_mock(self):
        raise NotImplementedError("Subclasses must implement this method")

    def build_record(self, resource_name: str, data: dict = None) -> dict:
        # Create a deep copy to avoid modifying the original fixture
        record = deepcopy(self.fixtures_per_resource[resource_name])
        if data:
            self._deep_update(record, data)
        return record

    def build_records(
        self, resource_name: str, count: int, data: dict = None
    ) -> list[dict]:
        return [self.build_record(resource_name, data) for _ in range(count)]

    def create_record(self, resource_name: str, data: dict = None) -> dict:
        record = self.build_record(resource_name, data)
        # Add record to the mock's storage
        mock = self.resource_mock()
        mock.add_record(resource_name, record)
        return record

    def create_records(
        self, resource_name: str, count: int, data: dict = None
    ) -> list[dict]:
        records = []
        for _ in range(count):
            record = self.create_record(resource_name, data)
            records.append(record)
        return records

    def clear_resource(self, resource_name: str):
        """Clear all records from a specific resource"""
        self.resource_mock().clear_resource_records(resource_name)

    def clear_all_resources(self):
        """Clear all records from all resources"""
        self.resource_mock().clear_all_records()

    def get_record_by_id(self, resource_name: str, record_id: str) -> dict:
        """Get a specific record by its ID"""
        return self.resource_mock().get_record_by_id(resource_name, record_id)

    def get_records(self, resource_name: str) -> list[dict]:
        """List all records for a resource"""
        return self.resource_mock().get_records(resource_name)

    def _initialize_resources_mocks(self):
        # Initialize empty record lists in the mock for each resource
        mock = self.resource_mock()
        for resource_name in self.fixtures_per_resource.keys():
            mock.initialize_resource_records(resource_name)
            mock.mock_resource_list(resource_name, mock.get_records(resource_name))
            mock.mock_resource_creation(resource_name)
            mock.mock_resource_update(resource_name)
            mock.mock_resource_delete(resource_name)

    def _load_all_fixtures(self):
        fixtures_per_resource = {}
        fixtures_folder = (
            Path(__file__).parent.parent / "fixtures" / self.fixture_folder()
        )
        for file in fixtures_folder.glob("*.json"):
            fixture_data = self._load_fixture(file.stem)
            fixtures_per_resource[fixture_data["resource_name"]] = fixture_data[
                "sample_record"
            ]
        return fixtures_per_resource

    def _load_fixture(self, fixture_name: str) -> dict:
        fixture_path = (
            Path(__file__).parent.parent
            / "fixtures"
            / self.fixture_folder()
            / f"{fixture_name}.json"
        )
        with open(fixture_path) as f:
            return json.load(f)

    def _deep_update(self, base_dict: dict, update_dict: dict):
        """Recursively update a dictionary with another dictionary"""
        for key, value in update_dict.items():
            if (
                isinstance(value, dict)
                and key in base_dict
                and isinstance(base_dict[key], dict)
            ):
                self._deep_update(base_dict[key], value)
            else:
                base_dict[key] = value
