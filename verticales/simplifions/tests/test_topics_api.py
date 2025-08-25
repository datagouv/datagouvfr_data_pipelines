# The factory must be imported before the manager because it initializes the mocks
from factories.topics_factory import TopicsFactory
from datagouvfr_data_pipelines.utils.datagouv import local_client
from topics_api import TopicsAPI

topics_factory = TopicsFactory()
topics_api = TopicsAPI(local_client)


def test_get_all_topics_for_tag():
    """Example of how to mock the topics API requests using TopicsFactory"""

    topics_factory.clear_all_resources()
    topics_factory.create_record("simplifions-solutions", {"id": "topic-1"})
    topics_factory.create_record("simplifions-solutions", {"id": "topic-2"})
    topics_factory.create_record("simplifions-cas-d-usages", {"id": "topic-3"})

    solutions = list(topics_api.get_all_topics_for_tag("simplifions-solutions"))
    cas_usages = list(topics_api.get_all_topics_for_tag("simplifions-cas-d-usages"))

    assert len(solutions) == 2
    assert solutions[0]["id"] == "topic-1"
    assert solutions[1]["id"] == "topic-2"
    assert len(cas_usages) == 1
    assert cas_usages[0]["id"] == "topic-3"


def test_create_topic():
    topic_data = topics_factory.build_record("simplifions-solutions")
    response = topics_api.create_topic(topic_data)
    assert response.status_code == 201
    assert response.json()["name"] == topic_data["name"]


def test_update_topic():
    topic_data = topics_factory.build_record(
        "simplifions-solutions", {"name": "new name"}
    )
    response = topics_api.update_topic_by_id(topic_data["id"], topic_data)
    assert response.status_code == 204
    assert response.json()["name"] == "new name"


def test_delete_topic():
    response = topics_api.delete_topic("some-id")
    assert response.status_code == 204


def test_clear_all_resources():
    topics_factory.create_record("simplifions-solutions")
    topics_factory.create_record("simplifions-cas-d-usages")
    topics_factory.clear_all_resources()
    assert topics_factory.get_records("topics") == []


def test_clear_resource():
    topics_factory.create_record("simplifions-solutions")
    topics_factory.create_record("simplifions-cas-d-usages")
    topics_factory.clear_resource("topics")
    assert topics_factory.get_records("topics") == []
