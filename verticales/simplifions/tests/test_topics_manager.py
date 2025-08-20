import sys
from pathlib import Path
from unittest.mock import Mock, patch
import requests_mock

# Add the parent directory to the path so we can import the module
sys.path.append(str(Path(__file__).parent.parent))
# Add the root directory to the path so we can import utils
sys.path.append(str(Path(__file__).parent.parent.parent.parent))

# Mock the datagouvfr_data_pipelines modules before importing anything
mock_utils = Mock()
mock_utils.get_all_from_api_query = Mock()

# Mock the config module
mock_config = Mock()
mock_config.AIRFLOW_ENV = "dev"
mock_config.DATAGOUV_SECRET_API_KEY = "test-key"
mock_config.DEMO_DATAGOUV_SECRET_API_KEY = "test-demo-key"

# Mock the retry module
mock_retry = Mock()
mock_retry.simple_connection_retry = lambda func: func
mock_retry.RequestRetry = Mock()

with patch.dict(
    "sys.modules",
    {
        "datagouvfr_data_pipelines": Mock(),
        "datagouvfr_data_pipelines.config": mock_config,
        "datagouvfr_data_pipelines.utils": mock_utils,
        "datagouvfr_data_pipelines.utils.datagouv": mock_utils,
        "datagouvfr_data_pipelines.utils.retry": mock_retry,
    },
):
    from utils.datagouv import local_client
    from topics_manager import TopicsManager

topics_manager = TopicsManager(local_client)

def test__generated_search_tags():
    topic = {
        "target_users": "particuliers",
        "budget": ["aucun-developpement-ni-budget", "avec-des-moyens-techniques"],
        "other_attribute": "some_value",
    }
    assert topics_manager._generated_search_tags(topic) == ["simplifions-target_users-particuliers", "simplifions-budget-aucun-developpement-ni-budget", "simplifions-budget-avec-des-moyens-techniques"]


def test_get_all_topics_for_tag_with_mocked_api():
    """Example of how to mock the topics API requests while using the real local_client"""
    
    # Mock response for the topics API
    mock_topics_response = {
        "data": [
            {
                "id": "topic-1",
                "slug": "test-topic-1",
                "name": "Test Topic 1",
                "tags": ["simplifions", "simplifions-solutions"],
                "extras": {
                    "simplifions-solutions": {
                        "slug": "test-solution-1",
                        "Ref_Nom_de_la_solution": "Test Solution 1"
                    }
                }
            },
            {
                "id": "topic-2", 
                "slug": "test-topic-2",
                "name": "Test Topic 2",
                "tags": ["simplifions", "simplifions-solutions"],
                "extras": {
                    "simplifions-solutions": {
                        "slug": "test-solution-2",
                        "Ref_Nom_de_la_solution": "Test Solution 2"
                    }
                }
            }
        ],
        "page": 1,
        "page_size": 20,
        "total": 2
    }
    
    with requests_mock.Mocker() as m:
        # Mock the topics API endpoint 
        # The URL pattern matches what _get_all_topics_for_tag calls
        m.get(
            f"{local_client.base_url}/api/1/topics/?tag=simplifions-solutions&include_private=true",
            json=mock_topics_response
        )
        
        # Mock the get_all_from_api_query dependency that we already mocked during import
        mock_utils.get_all_from_api_query.return_value = mock_topics_response["data"]
        
        # Call the method
        result = topics_manager._get_all_topics_for_tag("simplifions-solutions")
        
        # Verify the result
        assert len(result) == 2
        assert result[0]["id"] == "topic-1"
        assert result[1]["id"] == "topic-2"
        
        # Verify the API was called with correct URL
        mock_utils.get_all_from_api_query.assert_called_once_with(
            f"{local_client.base_url}/api/1/topics/?tag=simplifions-solutions&include_private=true",
            auth=True,
        )


def test_create_topic_with_mocked_api():
    """Example of how to mock direct HTTP requests (POST, PUT, DELETE) to topics API"""
    
    topic_data = {
        "name": "Test Topic",
        "description": "Test Description",
        "organization": {"class": "Organization", "id": "57fe2a35c751df21e179df72"},
        "tags": ["simplifions", "simplifions-solutions"],
        "extras": {"simplifions-solutions": {"slug": "test-topic"}},
        "private": False
    }
    
    with requests_mock.Mocker() as m:
        # Mock the POST request to create a topic
        m.post(
            f"{local_client.base_url}/api/1/topics/",
            json={"id": "new-topic-id", "slug": "test-topic", **topic_data},
            status_code=201
        )
        
        # Call the method - this should not raise an exception
        topics_manager._create_topic(topic_data)
        
        # Verify the request was made with correct data
        assert m.call_count == 1
        assert m.last_request.json() == topic_data


def test_update_topic_with_mocked_api():
    """Example of how to mock PUT requests to update topics"""
    
    topic_id = "test-topic-id"
    topic_data = {
        "name": "Updated Topic",
        "description": "Updated Description", 
        "tags": ["simplifions", "simplifions-solutions"],
        "extras": {"simplifions-solutions": {"slug": "updated-topic"}},
    }
    
    with requests_mock.Mocker() as m:
        # Mock the PUT request to update a topic
        m.put(
            f"{local_client.base_url}/api/1/topics/{topic_id}/",
            json={"id": topic_id, **topic_data},
            status_code=200
        )
        
        # Call the method
        topics_manager._update_topic_by_id(topic_id, topic_data)
        
        # Verify the request was made with correct data
        assert m.call_count == 1
        assert m.last_request.json() == topic_data


def test_delete_topic_with_mocked_api():
    """Example of how to mock DELETE requests"""
    
    topic_id = "test-topic-id"
    
    with requests_mock.Mocker() as m:
        # Mock the DELETE request
        m.delete(
            f"{local_client.base_url}/api/1/topics/{topic_id}/",
            status_code=204
        )
        
        # Call the method
        topics_manager._delete_topic(topic_id)
        
        # Verify the request was made
        assert m.call_count == 1

