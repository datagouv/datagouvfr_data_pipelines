import pytest

# The factory must be imported before the manager because it initializes the mocks
from factories.topics_factory import TopicsFactory
from factories.task_instance_factory import TaskInstanceFactory
from topics_v2_manager import TopicsV2Manager
from datagouvfr_data_pipelines.utils.datagouv import local_client

topics_manager = TopicsV2Manager(local_client)
topics_factory = TopicsFactory()
task_instance_factory = TaskInstanceFactory()


@pytest.fixture
def grist_tables_for_filters():
    return {
        "Budgets_de_mise_en_oeuvre": [
            {"id": 1, "fields": {"Label": "Budget 1", "slug": "b1"}},
            {"id": 2, "fields": {"Label": "Budget 2", "slug": "b2"}},
        ],
        "Types_de_simplification": [
            {"id": 1, "fields": {"Label": "Type 1", "slug": "t1"}},
            {"id": 2, "fields": {"Label": "Type 2", "slug": "t2"}},
        ],
        "Usagers": [
            {"id": 1, "fields": {"Label": "Usager 1", "slug": "u1"}},
            {"id": 2, "fields": {"Label": "Usager 2", "slug": "u2"}},
        ],
        "Fournisseurs_de_services": [
            {"id": 1, "fields": {"Label": "Fournisseur 1", "slug": "f1"}},
            {"id": 2, "fields": {"Label": "Fournisseur 2", "slug": "f2"}},
        ],
    }


def test_generated_search_tags(grist_tables_for_filters):
    grist_row = {
        "id": 123,
        "fields": {
            "Budget_requis": 1,
            "Types_de_simplification": 2,
            "A_destination_de": 1,
            "Pour_simplifier_les_demarches_de": 2,
        },
    }

    tags = topics_manager._generated_search_tags(grist_row, grist_tables_for_filters)
    assert sorted(tags) == sorted([
        "simplifions-v2-budget-b1",
        "simplifions-v2-types-de-simplification-t2",
        "simplifions-v2-fournisseurs-de-service-f1",
        "simplifions-v2-target-users-u2",
    ])


class TestTopicsAreSimilarSoWeCanSkipUpdate:
    def test_equal_cas_usages(self):
        topic1 = topics_factory.build_record("simplifions-v2-cas-d-usages")
        topic2 = topics_factory.build_record("simplifions-v2-cas-d-usages")
        assert topics_manager._topics_are_similar_so_we_can_skip_update(topic1, topic2)

    def test_equal_solutions(self):
        topic1 = topics_factory.build_record("simplifions-v2-solutions")
        topic2 = topics_factory.build_record("simplifions-v2-solutions")
        assert topics_manager._topics_are_similar_so_we_can_skip_update(topic1, topic2)

    def test_different_topics_names(self):
        topic1 = topics_factory.build_record("simplifions-v2-cas-d-usages")
        topic2 = topics_factory.build_record(
            "simplifions-v2-cas-d-usages", {"name": "Different Name"}
        )
        assert not topics_manager._topics_are_similar_so_we_can_skip_update(
            topic1, topic2
        )

    def test_different_topics_tags(self):
        topic1 = topics_factory.build_record("simplifions-v2-cas-d-usages")
        topic2 = topics_factory.build_record(
            "simplifions-v2-cas-d-usages",
            {"tags": ["simplifions-v2-cas-d-usages", "another-tag"]},
        )
        assert not topics_manager._topics_are_similar_so_we_can_skip_update(
            topic1, topic2
        )

    def test_different_descriptions(self):
        topic1 = topics_factory.build_record("simplifions-v2-cas-d-usages")
        topic2 = topics_factory.build_record(
            "simplifions-v2-cas-d-usages", {"description": "Different Description"}
        )
        assert not topics_manager._topics_are_similar_so_we_can_skip_update(
            topic1, topic2
        )

    def test_different_organizations(self):
        topic1 = topics_factory.build_record("simplifions-v2-cas-d-usages")
        topic2 = topics_factory.build_record(
            "simplifions-v2-cas-d-usages",
            {"organization": {"id": "different-organization"}},
        )
        assert not topics_manager._topics_are_similar_so_we_can_skip_update(
            topic1, topic2
        )

    def test_different_extras(self):
        topic1 = topics_factory.build_record("simplifions-v2-cas-d-usages")
        topic2 = topics_factory.build_record(
            "simplifions-v2-cas-d-usages",
            {"extras": {"simplifions-v2-cas-d-usages": {"id": "different-id"}}},
        )
        assert not topics_manager._topics_are_similar_so_we_can_skip_update(
            topic1, topic2
        )

    def test_different_private(self):
        topic1 = topics_factory.build_record("simplifions-v2-cas-d-usages")
        topic2 = topics_factory.build_record(
            "simplifions-v2-cas-d-usages", {"private": True}
        )
        assert not topics_manager._topics_are_similar_so_we_can_skip_update(
            topic1, topic2
        )


# Tests for moved update_topics method are now in test_task_functions.py
