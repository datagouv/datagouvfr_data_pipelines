import pytest
from datagouvfr_data_pipelines.utils.datagouv import local_client
from factories.task_instance_factory import TaskInstanceFactory

# The factory must be imported before the manager because it initializes the mocks
from factories.topics_factory import TopicsFactory
from topics_v2_manager import TopicsV2Manager

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
        "Categories_de_solution": [
            {
                "id": 1,
                "fields": {"Nom": "Brique technique", "slug": "brique-technique"},
            },
            {"id": 2, "fields": {"Nom": "Logiciel métier", "slug": "logiciel-metier"}},
        ],
    }


@pytest.fixture
def topics_manager(grist_tables_for_filters):
    return TopicsV2Manager(local_client, grist_tables_for_filters)


def test_generated_search_tags(topics_manager):
    grist_row = {
        "id": 123,
        "fields": {
            "Budget_requis": 1,
            "Types_de_simplification": 2,
            "A_destination_de": 1,
            "Pour_simplifier_les_demarches_de": 2,
            "Categorie_de_solution": 1,
        },
    }

    tags = topics_manager._generated_search_tags(grist_row)
    assert sorted(tags) == sorted(
        [
            "simplifions-v2-budget-b1",
            "simplifions-v2-types-de-simplification-t2",
            "simplifions-v2-fournisseurs-de-service-f1",
            "simplifions-v2-target-users-u2",
            "simplifions-v2-categorie-de-solution-brique-technique",
        ]
    )


class TestTopicsAreSimilarSoWeCanSkipUpdate:
    def test_equal_cas_usages(self, topics_manager):
        topic1 = topics_factory.build_record("simplifions-v2-cas-d-usages")
        topic2 = topics_factory.build_record("simplifions-v2-cas-d-usages")
        assert topics_manager._topics_are_similar_so_we_can_skip_update(topic1, topic2)

    def test_equal_solutions(self, topics_manager):
        topic1 = topics_factory.build_record("simplifions-v2-solutions")
        topic2 = topics_factory.build_record("simplifions-v2-solutions")
        assert topics_manager._topics_are_similar_so_we_can_skip_update(topic1, topic2)

    def test_different_topics_names(self, topics_manager):
        topic1 = topics_factory.build_record("simplifions-v2-cas-d-usages")
        topic2 = topics_factory.build_record(
            "simplifions-v2-cas-d-usages", {"name": "Different Name"}
        )
        assert not topics_manager._topics_are_similar_so_we_can_skip_update(
            topic1, topic2
        )

    def test_different_topics_tags(self, topics_manager):
        topic1 = topics_factory.build_record("simplifions-v2-cas-d-usages")
        topic2 = topics_factory.build_record(
            "simplifions-v2-cas-d-usages",
            {"tags": ["simplifions-v2-cas-d-usages", "another-tag"]},
        )
        assert not topics_manager._topics_are_similar_so_we_can_skip_update(
            topic1, topic2
        )

    def test_different_descriptions(self, topics_manager):
        topic1 = topics_factory.build_record("simplifions-v2-cas-d-usages")
        topic2 = topics_factory.build_record(
            "simplifions-v2-cas-d-usages", {"description": "Different Description"}
        )
        assert not topics_manager._topics_are_similar_so_we_can_skip_update(
            topic1, topic2
        )

    def test_different_organizations(self, topics_manager):
        topic1 = topics_factory.build_record("simplifions-v2-cas-d-usages")
        topic2 = topics_factory.build_record(
            "simplifions-v2-cas-d-usages",
            {"organization": {"id": "different-organization"}},
        )
        assert not topics_manager._topics_are_similar_so_we_can_skip_update(
            topic1, topic2
        )

    def test_different_extras(self, topics_manager):
        topic1 = topics_factory.build_record("simplifions-v2-cas-d-usages")
        topic2 = topics_factory.build_record(
            "simplifions-v2-cas-d-usages",
            {"extras": {"simplifions-v2-cas-d-usages": {"id": "different-id"}}},
        )
        assert not topics_manager._topics_are_similar_so_we_can_skip_update(
            topic1, topic2
        )

    def test_different_private(self, topics_manager):
        topic1 = topics_factory.build_record("simplifions-v2-cas-d-usages")
        topic2 = topics_factory.build_record(
            "simplifions-v2-cas-d-usages", {"private": True}
        )
        assert not topics_manager._topics_are_similar_so_we_can_skip_update(
            topic1, topic2
        )


class TestKeywordTags:
    def test_with_keywords(self, topics_manager):
        grist_row = {"id": 1, "fields": {"Mots_clefs": ["api", "identité"]}}
        assert topics_manager._keyword_tags(grist_row) == ["api", "identité"]

    def test_with_single_keyword(self, topics_manager):
        grist_row = {"id": 1, "fields": {"Mots_clefs": "api"}}
        assert topics_manager._keyword_tags(grist_row) == ["api"]

    def test_with_empty_list(self, topics_manager):
        grist_row = {"id": 1, "fields": {"Mots_clefs": []}}
        assert topics_manager._keyword_tags(grist_row) == []

    def test_with_none(self, topics_manager):
        grist_row = {"id": 1, "fields": {"Mots_clefs": None}}
        assert topics_manager._keyword_tags(grist_row) == []

    def test_with_missing_field(self, topics_manager):
        grist_row = {"id": 1, "fields": {}}
        assert topics_manager._keyword_tags(grist_row) == []


# Tests for moved update_topics method are now in test_task_functions.py
