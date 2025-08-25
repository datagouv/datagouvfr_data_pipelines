import requests_mock
import json


class ExternalResourcesMock:
    # Global data mocker shared across all instances
    _data_mocker = None

    def __init__(self):
        self.records = {}  # Store records per resource
        self.initialize_data_mocker()

    def match_resource_list_url(self, resource_name: str):
        raise NotImplementedError("Not implemented")

    def match_resource_creation_url(self, resource_name: str):
        raise NotImplementedError("Not implemented")

    def match_resource_url(self, resource_name: str):
        raise NotImplementedError("Not implemented")

    def resource_id_column(self):
        return "id"

    def build_filtered_response_for_records(
        self, records: list[dict], request: requests_mock.Mocker
    ) -> dict:
        raise NotImplementedError("Not implemented")

    def build_response_for_record(self, record: dict):
        """Default response for a creation request. You can override this method to return a different response."""
        return json.dumps(record)

    def extract_record_id_from_url(self, url: str) -> str:
        """Default extract record ID from URL. Override this method if needed for specific URL patterns."""
        # Default implementation assumes ID is the last part of the path
        return url.rstrip("/").split("/")[-1]

    def initialize_resource_records(self, resource_name: str):
        """Initialize an empty list of records for a resource if it doesn't exist"""
        if resource_name not in self.records:
            self.records[resource_name] = []

    def add_record(self, resource_name: str, record: dict):
        """Add a record to the mock's storage and update the list endpoint"""
        self.initialize_resource_records(resource_name)
        self.records[resource_name].append(record)
        self.mock_resource_list(resource_name, self.records[resource_name])
        return record

    def update_record(self, resource_name: str, record_id: str, updated_data: dict):
        """Update a record in the mock's storage and refresh the list endpoint"""
        self.initialize_resource_records(resource_name)
        id_column = self.resource_id_column()

        for i, record in enumerate(self.records[resource_name]):
            if record.get(id_column) == record_id:
                self.records[resource_name][i].update(updated_data)
                self.mock_resource_list(resource_name, self.records[resource_name])
                return self.records[resource_name][i]
        # If record not found, add it
        updated_data[id_column] = record_id
        return self.add_record(resource_name, updated_data)

    def delete_record(self, resource_name: str, record_id: str):
        """Delete a record from the mock's storage and refresh the list endpoint"""
        self.initialize_resource_records(resource_name)
        id_column = self.resource_id_column()
        original_count = len(self.records[resource_name])

        self.records[resource_name] = [
            record
            for record in self.records[resource_name]
            if record.get(id_column) != record_id
        ]

        if len(self.records[resource_name]) < original_count:
            self.mock_resource_list(resource_name, self.records[resource_name])
            return True
        return False

    def clear_resource_records(self, resource_name: str):
        """Clear all records for a specific resource"""
        self.initialize_resource_records(resource_name)
        self.records[resource_name] = []
        self.mock_resource_list(resource_name, self.records[resource_name])

    def clear_all_records(self):
        """Clear all records from all resources"""
        for resource_name in list(self.records.keys()):
            self.clear_resource_records(resource_name)

    def get_records(self, resource_name: str) -> list[dict]:
        """Get all records for a resource"""
        self.initialize_resource_records(resource_name)
        return self.records[resource_name]

    def get_record_by_id(self, resource_name: str, record_id: str) -> dict:
        """Get a specific record by its ID"""
        self.initialize_resource_records(resource_name)
        id_column = self.resource_id_column()

        for record in self.records[resource_name]:
            if record.get(id_column) == record_id:
                return record

        raise ValueError(
            f"Record with ID {record_id} not found in resource {resource_name}"
        )

    def initialize_data_mocker(self):
        if ExternalResourcesMock._data_mocker is None:
            ExternalResourcesMock._data_mocker = requests_mock.Mocker()
            ExternalResourcesMock._data_mocker.start()

    def stop_data_mocks(self):
        if ExternalResourcesMock._data_mocker:
            ExternalResourcesMock._data_mocker.stop()
            ExternalResourcesMock._data_mocker = None

    def mock_resource_list(self, resource_name: str, records: list[dict]):
        def response_callback(request, context):
            response_data = self.build_filtered_response_for_records(records, request)
            return json.dumps(response_data)

        url_pattern = self.match_resource_list_url(resource_name)

        ExternalResourcesMock._data_mocker.register_uri(
            "GET", url_pattern, text=response_callback, status_code=200
        )

    def mock_resource_creation(self, resource_name: str):
        def response_callback(request, context):
            record_data = request.json()
            created_record = self.add_record(resource_name, record_data)
            return self.build_response_for_record(created_record)

        url_pattern = self.match_resource_creation_url(resource_name)

        ExternalResourcesMock._data_mocker.register_uri(
            "POST", url_pattern, text=response_callback, status_code=201
        )

    def mock_resource_update(self, resource_name: str):
        def response_callback(request, context):
            record_data = request.json()
            record_id = self.extract_record_id_from_url(request.url)
            updated_record = self.update_record(resource_name, record_id, record_data)
            return self.build_response_for_record(updated_record)

        url_pattern = self.match_resource_url(resource_name)

        ExternalResourcesMock._data_mocker.register_uri(
            "PUT", url_pattern, text=response_callback, status_code=204
        )

    def mock_resource_delete(self, resource_name: str):
        def response_callback(request, context):
            record_id = self.extract_record_id_from_url(request.url)
            # Delete the record from our storage
            self.delete_record(resource_name, record_id)
            return ""

        url_pattern = self.match_resource_url(resource_name)

        ExternalResourcesMock._data_mocker.register_uri(
            "DELETE", url_pattern, text=response_callback, status_code=204
        )
