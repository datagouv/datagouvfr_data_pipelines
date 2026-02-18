# The factory must be imported before the manager because it initializes the mocks
from datagouvfr_data_pipelines.utils.datagouv import local_client
from factories.topics_factory import TopicsFactory
from topics_api import TopicsAPI

topics_factory = TopicsFactory()
topics_api = TopicsAPI(local_client)


def test_get_all_topics_for_tag():
    """Example of how to mock the topics API requests using TopicsFactory"""

    topics_factory.clear_all_resources()
    topics_factory.create_record("simplifions-v2-solutions", {"id": "topic-1"})
    topics_factory.create_record("simplifions-v2-solutions", {"id": "topic-2"})
    topics_factory.create_record("simplifions-v2-cas-d-usages", {"id": "topic-3"})

    solutions = list(topics_api.get_all_topics_for_tag("simplifions-v2-solutions"))
    cas_usages = list(topics_api.get_all_topics_for_tag("simplifions-v2-cas-d-usages"))

    assert len(solutions) == 2
    assert solutions[0]["id"] == "topic-1"
    assert solutions[1]["id"] == "topic-2"
    assert len(cas_usages) == 1
    assert cas_usages[0]["id"] == "topic-3"


def test_create_topic():
    topic_data = topics_factory.build_record("simplifions-v2-solutions")
    response = topics_api.create_topic(topic_data)
    assert response.status_code == 201
    assert response.json()["name"] == topic_data["name"]


def test_update_topic():
    topic_data = topics_factory.build_record(
        "simplifions-v2-solutions", {"name": "new name"}
    )
    response = topics_api.update_topic_by_id(topic_data["id"], topic_data)
    assert response.status_code == 204
    assert response.json()["name"] == "new name"


def test_delete_topic():
    response = topics_api.delete_topic("some-id")
    assert response.status_code == 204


def test_clear_all_resources():
    topics_factory.create_record("simplifions-v2-solutions")
    topics_factory.create_record("simplifions-v2-cas-d-usages")
    topics_factory.clear_all_resources()
    assert topics_factory.get_records("topics") == []


def test_clear_resource():
    topics_factory.create_record("simplifions-v2-solutions")
    topics_factory.create_record("simplifions-v2-cas-d-usages")
    topics_factory.clear_resource("topics")
    assert topics_factory.get_records("topics") == []


def test_get_all_topics_for_tag_with_v2_tags():
    """Test that v2 tags work correctly with the API"""
    topics_factory.clear_all_resources()

    # Create topics with v2-specific tags
    topics_factory.create_record(
        "simplifions-v2-solutions",
        {
            "id": "solution-1",
            "tags": [
                "simplifions-v2",
                "simplifions-v2-dag-generated",
                "simplifions-v2-solutions",
            ],
        },
    )
    topics_factory.create_record(
        "simplifions-v2-cas-d-usages",
        {
            "id": "cas-usage-1",
            "tags": [
                "simplifions-v2",
                "simplifions-v2-dag-generated",
                "simplifions-v2-cas-d-usages",
            ],
        },
    )

    # Test filtering by main v2 tag
    all_v2_topics = list(topics_api.get_all_topics_for_tag("simplifions-v2"))
    assert len(all_v2_topics) == 2

    # Test filtering by specific resource type
    solutions = list(topics_api.get_all_topics_for_tag("simplifions-v2-solutions"))
    cas_usages = list(topics_api.get_all_topics_for_tag("simplifions-v2-cas-d-usages"))

    assert len(solutions) == 1
    assert len(cas_usages) == 1
    assert solutions[0]["id"] == "solution-1"
    assert cas_usages[0]["id"] == "cas-usage-1"
