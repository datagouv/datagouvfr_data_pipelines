from unittest.mock import Mock, patch
import re


class TopicsMock:
    def __init__(self):
        self._data_mocker = None
        self._config_mocker = None

    def mock_config(self):
        self.stop_config_mocks()
        config_mock = Mock()
        config_mock.AIRFLOW_ENV = "dev"
        config_mock.DATAGOUV_SECRET_API_KEY = "test-key"
        config_mock.DEMO_DATAGOUV_SECRET_API_KEY = "test-demo-key"

        # Mock the retry module
        mock_retry = Mock()
        mock_retry.simple_connection_retry = lambda func: func
        mock_retry.RequestRetry = Mock()

        # Mock the utils module with our custom get_all_from_api_query
        mock_utils = Mock()
        mock_utils.get_all_from_api_query = self._mock_get_all_from_api_query

        self._config_mocker = patch.dict(
            "sys.modules",
            {
                "datagouvfr_data_pipelines": Mock(),
                "datagouvfr_data_pipelines.config": config_mock,
                "datagouvfr_data_pipelines.utils": mock_utils,
                "datagouvfr_data_pipelines.utils.datagouv": mock_utils,
                "datagouvfr_data_pipelines.utils.retry": mock_retry,
            },
        )
        self._config_mocker.start()

    def stop_config_mocks(self):
        if self._config_mocker:
            self._config_mocker.stop()
            self._config_mocker = None

    def mock_topics_api_with_data(self, tag: str, topics: list[dict]):
        """Store topics data for the given tag to be returned by the mocked get_all_from_api_query"""
        if not hasattr(self, '_topics_storage'):
            self._topics_storage = {}
        self._topics_storage[tag] = topics

    def stop_data_mocks(self):
        if self._data_mocker:
            self._data_mocker.stop()
            self._data_mocker = None

    def _mock_get_all_from_api_query(self, url: str, auth: bool = True):
        """Mock implementation of get_all_from_api_query that returns stored topics"""
        # Extract tag from URL
        if "tag=" in url and "include_private=true" in url:
            # Extract the tag parameter from the URL  
            tag_match = re.search(r'tag=([^&]+)', url)
            if tag_match:
                tag = tag_match.group(1)
                return self._topics_storage.get(tag, [])
        return []


