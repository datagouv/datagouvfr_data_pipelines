from unittest.mock import Mock, patch
import requests_mock
import json
import re


# Shared requests_mock instance across all mocks
_shared_requests_mocker = None

def get_shared_mocker():
    global _shared_requests_mocker
    if _shared_requests_mocker is None:
        _shared_requests_mocker = requests_mock.Mocker()
        _shared_requests_mocker.start()
    return _shared_requests_mocker

class TopicsMock:
    def __init__(self):
        self._config_mocker = None
        self._topics_storage = {}
        self._callback_registered = False

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

        # Mock the utils module - we need to provide get_all_from_api_query
        # but since we're mocking HTTP requests, we can let the real function run
        import requests
        
        def mock_get_all_from_api_query(url, auth=True):
            """Let the function make real requests which will be intercepted by our requests_mock"""
            # This will be intercepted by our requests_mock setup
            response = requests.get(url)
            return response.json()
        
        mock_utils = Mock()
        mock_utils.get_all_from_api_query = mock_get_all_from_api_query
        mock_utils.pytest_plugins = None  # Explicitly set to None to avoid pytest issues
        
        # Create a mock local_client that the test can import
        from datagouv import Client
        mock_local_client = Client(environment="demo", api_key="test-demo-key")
        mock_utils.local_client = mock_local_client
        
        # Create mock datagouv module for direct import by tests
        mock_datagouv_direct = Mock()
        mock_datagouv_direct.local_client = mock_local_client
        mock_datagouv_direct.get_all_from_api_query = mock_get_all_from_api_query
        mock_datagouv_direct.pytest_plugins = None  # Explicitly set to None to avoid pytest issues

        # Create specific mock modules with limited specs to avoid pytest issues
        mock_datagouvfr_root = Mock(spec=[])  # Empty spec means no dynamic attributes
        mock_utils_root = Mock(spec=[])
        
        self._config_mocker = patch.dict(
            "sys.modules",
            {
                "datagouvfr_data_pipelines": mock_datagouvfr_root,
                "datagouvfr_data_pipelines.config": config_mock,
                "datagouvfr_data_pipelines.utils": mock_utils,
                "datagouvfr_data_pipelines.utils.datagouv": mock_utils,
                "datagouvfr_data_pipelines.utils.retry": mock_retry,
                # Mock the direct utils.datagouv import that tests use
                "utils": mock_utils_root,
                "utils.datagouv": mock_datagouv_direct,
            },
        )
        self._config_mocker.start()

    def stop_config_mocks(self):
        if self._config_mocker:
            self._config_mocker.stop()
            self._config_mocker = None

    def mock_topics_api_with_data(self, tag: str, topics: list[dict]):
        """Mock the topics API to return specific topics for a given tag"""
        # Store the topics for this tag
        self._topics_storage[tag] = topics
        
        # Use the shared mocker
        shared_mocker = get_shared_mocker()
        
        # Register callback only once
        if not self._callback_registered:
            # Create a single callback function that handles all tags
            def response_callback(request, context):
                # Extract tag from the URL query parameters
                parsed_tag = request.qs.get('tag', [None])[0]
                return json.dumps(self._topics_storage.get(parsed_tag, []))

            # Pattern to match demo.data.gouv.fr topics API calls
            url_pattern = re.compile(
                r"https://demo\.data\.gouv\.fr/api/1/topics/\?.*tag=.*"
            )

            # Register the callback for this URL pattern (only once)
            shared_mocker.register_uri("GET", url_pattern, text=response_callback)
            self._callback_registered = True

    def clear_topics_for_tag(self, tag: str):
        """Clear topics for a specific tag"""
        if tag in self._topics_storage:
            self._topics_storage[tag] = []
    
    def clear_all_topics(self):
        """Clear all topics storage"""
        self._topics_storage = {}

    def stop_data_mocks(self):
        # Don't stop the shared mocker since other mocks might be using it
        # Just clear our callback registration flag
        self._callback_registered = False


