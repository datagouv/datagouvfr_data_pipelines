import sys
from pathlib import Path
import pytest

# Add the parent directory to the path so we can import the module
sys.path.append(str(Path(__file__).parent.parent))
# Add the root directory to the path so we can import utils
sys.path.append(str(Path(__file__).parent.parent.parent.parent))

# The factory must be imported before the manager because it initializes the mocks
from topics_factory import TopicsFactory
from utils.datagouv import local_client
from topics_manager import TopicsManager

topics_manager = TopicsManager(local_client)
topics_factory = TopicsFactory()

def test__generated_search_tags():
    topic = {
        "target_users": "particuliers",
        "budget": ["aucun-developpement-ni-budget", "avec-des-moyens-techniques"],
        "other_attribute": "some_value",
    }
    assert topics_manager._generated_search_tags(topic) == ["simplifions-target_users-particuliers", "simplifions-budget-aucun-developpement-ni-budget", "simplifions-budget-avec-des-moyens-techniques"]


def test_get_all_topics_for_tag_with_mocked_api():
    """Example of how to mock the topics API requests using TopicsFactory"""
    
    # Clear any existing topics first
    topics_factory.clear_tag("simplifions-solutions")
    
    # Create topics with custom data for testing
    topic1_data = {
        "id": "topic-1",
        "slug": "test-topic-1", 
        "name": "Test Topic 1",
        "extras": {
            "simplifions-solutions": {
                "slug": "test-solution-1",
                "Ref_Nom_de_la_solution": "Test Solution 1"
            }
        }
    }
    
    topic2_data = {
        "id": "topic-2",
        "slug": "test-topic-2",
        "name": "Test Topic 2", 
        "extras": {
            "simplifions-solutions": {
                "slug": "test-solution-2",
                "Ref_Nom_de_la_solution": "Test Solution 2"
            }
        }
    }
    
    # Create the topics using the factory
    topics_factory.create_topic("simplifions-solutions", topic1_data)
    topics_factory.create_topic("simplifions-solutions", topic2_data)
    
    # Call the method
    result = topics_manager._get_all_topics_for_tag("simplifions-solutions")
    
    # Verify the result
    assert len(result) == 2
    assert result[0]["id"] == "topic-1"
    assert result[1]["id"] == "topic-2"


# def test_create_topic_with_mocked_api():
#     """Example of how to mock direct HTTP requests (POST, PUT, DELETE) to topics API"""
    
#     topic_data = {
#         "name": "Test Topic",
#         "description": "Test Description",
#         "organization": {"class": "Organization", "id": "57fe2a35c751df21e179df72"},
#         "tags": ["simplifions", "simplifions-solutions"],
#         "extras": {"simplifions-solutions": {"slug": "test-topic"}},
#         "private": False
#     }
    
#     with requests_mock.Mocker() as m:
#         # Mock the POST request to create a topic
#         m.post(
#             f"{local_client.base_url}/api/1/topics/",
#             json={"id": "new-topic-id", "slug": "test-topic", **topic_data},
#             status_code=201
#         )
        
#         # Call the method - this should not raise an exception
#         topics_manager._create_topic(topic_data)
        
#         # Verify the request was made with correct data
#         assert m.call_count == 1
#         assert m.last_request.json() == topic_data


# def test_update_topic_with_mocked_api():
#     """Example of how to mock PUT requests to update topics"""
    
#     topic_id = "test-topic-id"
#     topic_data = {
#         "name": "Updated Topic",
#         "description": "Updated Description", 
#         "tags": ["simplifions", "simplifions-solutions"],
#         "extras": {"simplifions-solutions": {"slug": "updated-topic"}},
#     }
    
#     with requests_mock.Mocker() as m:
#         # Mock the PUT request to update a topic
#         m.put(
#             f"{local_client.base_url}/api/1/topics/{topic_id}/",
#             json={"id": topic_id, **topic_data},
#             status_code=200
#         )
        
#         # Call the method
#         topics_manager._update_topic_by_id(topic_id, topic_data)
        
#         # Verify the request was made with correct data
#         assert m.call_count == 1
#         assert m.last_request.json() == topic_data


# def test_delete_topic_with_mocked_api():
#     """Example of how to mock DELETE requests"""
    
#     topic_id = "test-topic-id"
    
#     with requests_mock.Mocker() as m:
#         # Mock the DELETE request
#         m.delete(
#             f"{local_client.base_url}/api/1/topics/{topic_id}/",
#             status_code=204
#         )
        
#         # Call the method
#         topics_manager._delete_topic(topic_id)
        
#         # Verify the request was made
#         assert m.call_count == 1

