import json
from pathlib import Path
from copy import deepcopy

from topics_mock import TopicsMock

topics_mock = TopicsMock()
topics_mock.mock_config()


class TopicsFactory:
    def __init__(self):
        self.fixtures_per_tag = self._load_all_fixtures()
        self._initialize_topics_mocks()

    def build_topic(self, tag: str, data: dict = None) -> dict:
        """Build a single topic record based on the fixture for the given tag"""
        # Create a deep copy to avoid modifying the original fixture
        topic = deepcopy(self.fixtures_per_tag[tag])
        if data:
            # Allow overriding any field in the topic
            self._deep_update(topic, data)
        return topic

    def build_topics(self, tag: str, count: int, data: dict = None) -> list[dict]:
        """Build multiple topic records based on the fixture for the given tag"""
        return [self.build_topic(tag, data) for _ in range(count)]

    def create_topics(self, tag: str, count: int, data: dict = None) -> list[dict]:
        """Create and mock multiple topic records for the given tag"""
        topics = self.build_topics(tag, count, data)
        self.records[tag].extend(topics)
        topics_mock.mock_topics_api_with_data(tag, self.records[tag])
        return topics

    def create_topic(self, tag: str, data: dict = None) -> dict:
        """Create and mock a single topic record for the given tag"""
        topics = self.create_topics(tag, 1, data)
        return topics[0]

    def clear_tag(self, tag: str):
        """Clear all topics for a specific tag"""
        if tag in self.records:
            self.records[tag] = []
            topics_mock.mock_topics_api_with_data(tag, self.records[tag])

    def clear_all_tags(self):
        """Clear all topics for all tags"""
        for tag in self.records.keys():
            self.clear_tag(tag)

    def _initialize_topics_mocks(self):
        """Initialize empty topics storage for each tag"""
        self.records = {tag: [] for tag in self.fixtures_per_tag.keys()}
        for tag in self.records.keys():
            topics_mock.mock_topics_api_with_data(tag, self.records[tag])

    def _load_all_fixtures(self):
        """Load all topic fixtures from the fixtures/topics directory"""
        fixtures_per_tag = {}
        fixtures_folder = Path(__file__).parent / "fixtures" / "topics"
        for file in fixtures_folder.glob("*.json"):
            fixture_data = self._load_fixture(file.stem)
            fixtures_per_tag[fixture_data["tag"]] = fixture_data["sample_record"]
        return fixtures_per_tag

    def _load_fixture(self, fixture_name: str) -> dict:
        """Load a specific fixture file"""
        fixture_path = Path(__file__).parent / "fixtures" / "topics" / f"{fixture_name}.json"
        with open(fixture_path) as f:
            return json.load(f)

    def _deep_update(self, base_dict: dict, update_dict: dict):
        """Recursively update a dictionary with another dictionary"""
        for key, value in update_dict.items():
            if isinstance(value, dict) and key in base_dict and isinstance(base_dict[key], dict):
                self._deep_update(base_dict[key], value)
            else:
                base_dict[key] = value
