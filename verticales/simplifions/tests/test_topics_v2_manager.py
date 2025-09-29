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
    assert tags == [
        "simplifions-v2-budget-b1",
        "simplifions-v2-types-de-simplification-t2",
        "simplifions-v2-fournisseurs-de-service-f1",
        "simplifions-v2-target-users-u2",
    ]


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


class TestUpdateTopics:
    def setup_method(self):
        topics_factory.clear_all_resources()

    def test_with_one_new_solution(self, grist_tables_for_filters):
        solutions_data = {
            1: {
                "id": 1,
                "fields": {
                    "Nom": "Solution 1",
                    "Description_courte": "Blabla",
                    "Visible_sur_simplifions": True,
                    "Budget_requis": 1,
                    "Types_de_simplification": 2,
                    "A_destination_de": 1,
                    "Pour_simplifier_les_demarches_de": 2,
                    "Image": ["https://example.com/image.png"],
                    "Public_ou_prive": "Public",
                    "Nom_de_l_operateur": "Operateur 1",
                },
            },
        }
        mock_ti = task_instance_factory.build_ti(
            {
                "tag_and_grist_rows_v2": {
                    "simplifions-v2-solutions": solutions_data,
                },
                "grist_tables_for_filters": grist_tables_for_filters,
            }
        )

        topics_manager.update_topics(mock_ti)

        topics = topics_factory.get_records("topics")
        assert len(topics) == 1
        assert topics[0]["name"] == "Solution 1"
        assert topics[0]["description"] == "Blabla"
        assert not topics[0]["private"]
        assert topics[0]["tags"] == [
            "simplifions-v2",
            "simplifions-v2-dag-generated",
            "simplifions-v2-solutions",
            "simplifions-v2-solutions-1",
            "simplifions-v2-budget-b1",
            "simplifions-v2-types-de-simplification-t2",
            "simplifions-v2-fournisseurs-de-service-f1",
            "simplifions-v2-target-users-u2",
        ]
        assert topics[0]["extras"]["simplifions-v2-solutions"]["id"] == 1
        assert topics[0]["extras"]["simplifions-v2-solutions"]["Image"] == [
            "https://example.com/image.png"
        ]
        assert (
            topics[0]["extras"]["simplifions-v2-solutions"]["Public_ou_prive"]
            == "Public"
        )
        assert (
            topics[0]["extras"]["simplifions-v2-solutions"]["Nom_de_l_operateur"]
            == "Operateur 1"
        )

    def test_with_one_new_cas_d_usage(self, grist_tables_for_filters):
        cas_usages_data = {
            1: {
                "id": 1,
                "fields": {
                    "Nom": "Cas usage 1",
                    "Description_courte": "Blabla",
                    "Visible_sur_simplifions": True,
                    "Budget_requis": 1,
                    "Types_de_simplification": 2,
                    "A_destination_de": 1,
                    "Pour_simplifier_les_demarches_de": 2,
                    "Icone_du_titre": "👋",
                },
            },
        }
        mock_ti = task_instance_factory.build_ti(
            {
                "tag_and_grist_rows_v2": {
                    "simplifions-v2-cas-d-usages": cas_usages_data,
                },
                "grist_tables_for_filters": grist_tables_for_filters,
            }
        )

        topics_manager.update_topics(mock_ti)

        topics = topics_factory.get_records("topics")
        assert len(topics) == 1
        assert topics[0]["name"] == "👋 Cas usage 1"
        assert topics[0]["description"] == "Blabla"
        assert not topics[0]["private"]
        assert topics[0]["tags"] == [
            "simplifions-v2",
            "simplifions-v2-dag-generated",
            "simplifions-v2-cas-d-usages",
            "simplifions-v2-cas-d-usages-1",
            "simplifions-v2-budget-b1",
            "simplifions-v2-types-de-simplification-t2",
            "simplifions-v2-fournisseurs-de-service-f1",
            "simplifions-v2-target-users-u2",
        ]
        assert topics[0]["extras"]["simplifions-v2-cas-d-usages"]["id"] == 1

    def test_with_new_solutions_and_cas_d_usages(self, grist_tables_for_filters):
        solutions_data = {
            1: {
                "id": 1,
                "fields": {
                    "Nom": "Solution 1",
                    "Description_courte": "Blabla",
                    "Visible_sur_simplifions": True,
                },
            },
            2: {
                "id": 2,
                "fields": {
                    "Nom": "Solution 2",
                    "Description_courte": "Blabla",
                    "Visible_sur_simplifions": True,
                },
            },
        }
        cas_usages_data = {
            1: {
                "id": 1,
                "fields": {
                    "Nom": "Cas usage 1",
                    "Description_courte": "Blabla",
                    "Visible_sur_simplifions": True,
                },
            },
            2: {
                "id": 2,
                "fields": {
                    "Nom": "Cas usage 2",
                    "Description_courte": "Blabla",
                    "Visible_sur_simplifions": True,
                },
            },
        }
        mock_ti = task_instance_factory.build_ti(
            {
                "tag_and_grist_rows_v2": {
                    "simplifions-v2-solutions": solutions_data,
                    "simplifions-v2-cas-d-usages": cas_usages_data,
                },
                "grist_tables_for_filters": grist_tables_for_filters,
            }
        )

        topics_manager.update_topics(mock_ti)

        topics = topics_factory.get_records("topics")
        assert len(topics) == 4

    def test_with_private_solutions(self, grist_tables_for_filters):
        solutions_data = {
            1: {
                "id": 1,
                "fields": {
                    "Nom": "Solution 1",
                    "Description_courte": "Blabla",
                    "Visible_sur_simplifions": True,
                },
            },
            2: {
                "id": 2,
                "fields": {
                    "Nom": "Solution 2",
                    "Description_courte": "Blabla",
                    "Visible_sur_simplifions": False,
                },
            },
        }
        mock_ti = task_instance_factory.build_ti(
            {
                "tag_and_grist_rows_v2": {
                    "simplifions-v2-solutions": solutions_data,
                },
                "grist_tables_for_filters": grist_tables_for_filters,
            }
        )

        topics_manager.update_topics(mock_ti)

        topics = topics_factory.get_records("topics")
        assert len(topics) == 2
        assert not topics[0]["private"]
        assert topics[1]["private"]

    def test_update_of_existing_solution(self, grist_tables_for_filters):
        existing_solution = topics_factory.create_record(
            "simplifions-v2-solutions",
            {
                "name": "Existing Solution",
                "description": "Existing solution description",
                "private": True,
                "tags": [
                    "simplifions-v2",
                    "simplifions-v2-dag-generated",
                    "simplifions-v2-solutions",
                ],
                "extras": {"id": 1},
            },
        )

        topics = topics_factory.get_records("topics")
        assert len(topics) == 1
        assert topics[0] == existing_solution

        solutions_data = {
            1: {
                "id": 1,
                "fields": {
                    "Nom": "Solution 1",
                    "Description_courte": "Blabla",
                    "Visible_sur_simplifions": True,
                    "Budget_requis": 1,
                    "Types_de_simplification": 2,
                    "A_destination_de": 1,
                    "Pour_simplifier_les_demarches_de": 2,
                },
            },
        }
        mock_ti = task_instance_factory.build_ti(
            {
                "tag_and_grist_rows_v2": {
                    "simplifions-v2-solutions": solutions_data,
                },
                "grist_tables_for_filters": grist_tables_for_filters,
            }
        )

        topics_manager.update_topics(mock_ti)

        topics = topics_factory.get_records("topics")
        assert len(topics) == 1
        assert topics[0]["name"] == "Solution 1"
        assert topics[0]["description"] == "Blabla"
        assert not topics[0]["private"]
        assert topics[0]["tags"] == [
            "simplifions-v2",
            "simplifions-v2-dag-generated",
            "simplifions-v2-solutions",
            "simplifions-v2-solutions-1",
            "simplifions-v2-budget-b1",
            "simplifions-v2-types-de-simplification-t2",
            "simplifions-v2-fournisseurs-de-service-f1",
            "simplifions-v2-target-users-u2",
        ]
        assert topics[0]["extras"]["simplifions-v2-solutions"]["id"] == 1

    def test_delete_of_existing_solution(self, grist_tables_for_filters):
        topics_factory.create_record(
            "simplifions-v2-solutions",
            {
                "name": "Delete this solution",
                "extras": {"simplifions-v2-solutions": {"id": 1}},
            },
        )
        topics = topics_factory.get_records("topics")
        assert len(topics) == 1
        assert topics[0]["extras"]["simplifions-v2-solutions"]["id"] == 1
        assert topics[0]["name"] == "Delete this solution"

        solutions_data = {
            2: {
                "id": 2,
                "fields": {
                    "Nom": "New Solution",
                    "Description_courte": "Blabla",
                    "Visible_sur_simplifions": True,
                    "Budget_requis": 1,
                    "Types_de_simplification": 2,
                    "A_destination_de": 1,
                    "Pour_simplifier_les_demarches_de": 2,
                },
            },
        }
        mock_ti = task_instance_factory.build_ti(
            {
                "tag_and_grist_rows_v2": {
                    "simplifions-v2-solutions": solutions_data,
                },
                "grist_tables_for_filters": grist_tables_for_filters,
            }
        )

        topics_manager.update_topics(mock_ti)

        topics = topics_factory.get_records("topics")
        assert len(topics) == 1
        assert topics[0]["extras"]["simplifions-v2-solutions"]["id"] == 2
        assert topics[0]["name"] == "New Solution"
