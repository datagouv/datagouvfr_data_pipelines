import json
from pathlib import Path
from copy import deepcopy

from grist_mock import GristMock

grist_mock = GristMock()
grist_mock.mock_config()


class GristFactory:
    def __init__(self):
        self.fixtures_per_table = self._load_all_fixtures()
        self._initialize_table_mocks()

    def build_record(self, table_id: str, data: dict = None) -> dict:
        # Create a deep copy to avoid modifying the original fixture
        record = deepcopy(self.fixtures_per_table[table_id])
        if data:
            record.update(data)
        return record

    def build_records(self, table_id: str, count: int, data: dict = None) -> list[dict]:
        return [self.build_record(table_id, data) for _ in range(count)]

    def create_records(
        self, table_id: str, count: int, data: dict = None
    ) -> list[dict]:
        records = self.build_records(table_id, count, data)
        self.records[table_id].extend(records)
        grist_mock.mock_table_with_data(table_id, self.records[table_id])
        return records

    def create_record(self, table_id: str, data: dict = None) -> dict:
        records = self.create_records(table_id, 1, data)
        return records[0]

    def clear_table(self, table_id: str):
        """Clear all records from a specific table"""
        if table_id in self.records:
            self.records[table_id] = []
            grist_mock.mock_table_with_data(table_id, self.records[table_id])

    def clear_all_tables(self):
        """Clear all records from all tables"""
        for table_id in self.records.keys():
            self.clear_table(table_id)

    def _initialize_table_mocks(self):
        self.records = {key: [] for key in self.fixtures_per_table.keys()}
        for table_id in self.records.keys():
            grist_mock.mock_table_with_data(table_id, self.records[table_id])

    def _load_all_fixtures(self):
        fixtures_per_table = {}
        fixtures_folder = Path(__file__).parent / "fixtures" / "grist"
        for file in fixtures_folder.glob("*.json"):
            fixture_data = self._load_fixture(file.stem)
            fixtures_per_table[fixture_data["table_id"]] = fixture_data["sample_record"]
        return fixtures_per_table

    def _load_fixture(self, fixture_name: str) -> dict:
        fixture_path = Path(__file__).parent / "fixtures" / "grist" / f"{fixture_name}.json"
        with open(fixture_path) as f:
            return json.load(f)
