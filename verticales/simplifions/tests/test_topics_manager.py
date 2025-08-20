import pytest

# The factory must be imported before the manager because it initializes the mocks
from factories.topics_factory import TopicsFactory
from factories.task_instance_factory import TaskInstanceFactory
from topics_manager import TopicsManager
from datagouvfr_data_pipelines.utils.datagouv import local_client

topics_manager = TopicsManager(local_client)
topics_factory = TopicsFactory()
task_instance_factory = TaskInstanceFactory()


def test_generated_search_tags():
    topic = {
        "target_users": "particuliers",
        "budget": ["Aucun développement, ni budget", "Avec des moyens techniques"],
        "types_de_simplification": [
            "⭐️ Accès facile",
            "⭐️⭐️ DLNUF",
            "⭐️⭐️⭐️ Proactivité",
        ],
        "other_attribute": "some_value",
    }
    assert topics_manager._generated_search_tags(topic) == [
        "simplifions-target-users-particuliers",
        "simplifions-budget-aucun-developpement-ni-budget",
        "simplifions-budget-avec-des-moyens-techniques",
        "simplifions-types-de-simplification-acces-facile",
        "simplifions-types-de-simplification-dlnuf",
        "simplifions-types-de-simplification-proactivite",
    ]


class TestTopicsApiMockedRequests:
    def test_get_all_topics_for_tag(self):
        """Example of how to mock the topics API requests using TopicsFactory"""

        topics_factory.clear_all_resources()
        topics_factory.create_record("simplifions-solutions", {"id": "topic-1"})
        topics_factory.create_record("simplifions-solutions", {"id": "topic-2"})
        topics_factory.create_record("simplifions-cas-d-usages", {"id": "topic-3"})

        solutions = list(
            topics_manager._get_all_topics_for_tag("simplifions-solutions")
        )
        cas_usages = list(
            topics_manager._get_all_topics_for_tag("simplifions-cas-d-usages")
        )

        assert len(solutions) == 2
        assert solutions[0]["id"] == "topic-1"
        assert solutions[1]["id"] == "topic-2"
        assert len(cas_usages) == 1
        assert cas_usages[0]["id"] == "topic-3"

    def test_create_topic(self):
        topic_data = topics_factory.build_record("simplifions-solutions")
        response = topics_manager._create_topic(topic_data)
        assert response.status_code == 201
        assert response.json()["name"] == topic_data["name"]

    def test_update_topic(self):
        topic_data = topics_factory.build_record(
            "simplifions-solutions", {"name": "new name"}
        )
        response = topics_manager._update_topic_by_id(topic_data["id"], topic_data)
        assert response.status_code == 204
        assert response.json()["name"] == "new name"

    def test_delete_topic(self):
        response = topics_manager._delete_topic("some-id")
        assert response.status_code == 204

    def test_clear_all_resources(self):
        topics_factory.create_record("simplifions-solutions")
        topics_factory.create_record("simplifions-cas-d-usages")
        topics_factory.clear_all_resources()
        assert topics_factory.get_records("topics") == []

    def test_clear_resource(self):
        topics_factory.create_record("simplifions-solutions")
        topics_factory.create_record("simplifions-cas-d-usages")
        topics_factory.clear_resource("topics")
        assert topics_factory.get_records("topics") == []


class TestNestedPop:
    def test_basic_nested_pop(self):
        """Test basic nested key removal from a multi-level dictionary"""
        nested_dict = {
            "level1": {
                "level2": {"target_key": "value_to_remove", "keep_key": "keep_this"},
                "other_key": "also_keep",
            },
            "root_key": "root_value",
        }
        topics_manager._nested_pop(nested_dict, "level1.level2.target_key")

        assert nested_dict == {
            "level1": {"level2": {"keep_key": "keep_this"}, "other_key": "also_keep"},
            "root_key": "root_value",
        }

    def test_single_level_pop(self):
        """Test removing a key from the root level of a dictionary"""
        test_dict = {"key_to_remove": "value", "keep_key": "keep"}
        topics_manager._nested_pop(test_dict, "key_to_remove")
        assert test_dict == {"keep_key": "keep"}

    def test_array_pop(self):
        """Test removing a key from an array"""
        test_dict = {
            "array_key": [
                {"key_to_remove": "value"},
                {"key_to_remove": "value", "keep_key": "keep"},
            ]
        }
        topics_manager._nested_pop(test_dict, "array_key[].key_to_remove")
        assert test_dict == {"array_key": [{}, {"keep_key": "keep"}]}

    def test_nonexistent_root_key_raises_error(self):
        """Test that KeyError is raised when root key doesn't exist"""
        test_dict = {"existing": {"nested": "value"}}

        with pytest.raises(KeyError) as exc_info:
            topics_manager._nested_pop(test_dict, "nonexistent.key")

        assert "Key nonexistent of nonexistent.key not found" in str(exc_info.value)

    def test_nonexistent_intermediate_key_raises_error(self):
        """Test that KeyError is raised when intermediate key doesn't exist"""
        test_dict = {"existing": {"wrong_nested": "value"}}

        with pytest.raises(KeyError) as exc_info:
            topics_manager._nested_pop(test_dict, "existing.missing.key")

        assert "Key missing of existing.missing.key not found" in str(exc_info.value)

    def test_array_key_points_to_dict_raises_error(self):
        """Test that TypeError is raised when array key points to a dictionary"""
        test_dict = {"not_an_array": {"key": "value"}}

        with pytest.raises(TypeError) as exc_info:
            topics_manager._nested_pop(test_dict, "not_an_array[].key")

        assert "Expected list for key not_an_array, got <class 'dict'>" in str(
            exc_info.value
        )

    def test_final_key_not_found_no_error(self):
        """Test that no error is raised when the final key doesn't exist"""
        test_dict = {"level1": {"level2": {"existing_key": "value"}}}

        # Should not raise an error even though 'nonexistent_key' doesn't exist
        topics_manager._nested_pop(test_dict, "level1.level2.nonexistent_key")

        # The dictionary should remain unchanged
        assert test_dict == {"level1": {"level2": {"existing_key": "value"}}}

    def test_final_key_not_found_in_array_no_error(self):
        """Test that no error is raised when the final key doesn't exist in array items"""
        test_dict = {
            "array_key": [
                {"existing_key": "value1"},
                {"existing_key": "value2", "other_key": "other"},
            ]
        }

        # Should not raise an error even though 'nonexistent_key' doesn't exist in array items
        topics_manager._nested_pop(test_dict, "array_key[].nonexistent_key")

        # The dictionary should remain unchanged
        assert test_dict == {
            "array_key": [
                {"existing_key": "value1"},
                {"existing_key": "value2", "other_key": "other"},
            ]
        }


class TestTopicsAreSimilarSoWeCanSkipUpdate:
    def test_equal_cas_usages(self):
        topic1 = topics_factory.build_record("simplifions-cas-d-usages")
        topic2 = topics_factory.build_record("simplifions-cas-d-usages")
        assert topics_manager._topics_are_similar_so_we_can_skip_update(topic1, topic2)

    def test_equal_solutions(self):
        topic1 = topics_factory.build_record("simplifions-solutions")
        topic2 = topics_factory.build_record("simplifions-solutions")
        assert topics_manager._topics_are_similar_so_we_can_skip_update(topic1, topic2)

    def test_different_topics_names(self):
        topic1 = topics_factory.build_record("simplifions-cas-d-usages")
        topic2 = topics_factory.build_record(
            "simplifions-cas-d-usages", {"name": "Different Name"}
        )
        assert not topics_manager._topics_are_similar_so_we_can_skip_update(
            topic1, topic2
        )

    def test_different_topics_tags(self):
        topic1 = topics_factory.build_record("simplifions-cas-d-usages")
        topic2 = topics_factory.build_record(
            "simplifions-cas-d-usages",
            {"tags": ["simplifions-cas-d-usages", "another-tag"]},
        )
        assert not topics_manager._topics_are_similar_so_we_can_skip_update(
            topic1, topic2
        )

    def test_equal_cas_usages_with_different_topics_references(self):
        topic1 = topics_factory.build_record(
            "simplifions-cas-d-usages",
            {
                "extras": {
                    "simplifions-cas-d-usages": {
                        "reco_solutions": [
                            {
                                "solution_topic_id": "solution-1",
                                "Nom_de_la_recommandation": "some recommendation",
                            }
                        ]
                    }
                }
            },
        )
        topic2 = topics_factory.build_record(
            "simplifions-cas-d-usages",
            {
                "extras": {
                    "simplifions-cas-d-usages": {
                        "reco_solutions": [
                            {"Nom_de_la_recommandation": "some recommendation"}
                        ]
                    }
                }
            },
        )
        assert topics_manager._topics_are_similar_so_we_can_skip_update(topic1, topic2)

    def test_diffent_cas_usages_with_different_recos(self):
        topic1 = topics_factory.build_record(
            "simplifions-cas-d-usages",
            {
                "extras": {
                    "simplifions-cas-d-usages": {
                        "reco_solutions": [
                            {
                                "solution_topic_id": "extra_reco",
                                "Nom_de_la_recommandation": "extra recommendation",
                            },
                            {
                                "solution_topic_id": "solution-1",
                                "Nom_de_la_recommandation": "some recommendation",
                            },
                        ]
                    }
                }
            },
        )
        topic2 = topics_factory.build_record(
            "simplifions-cas-d-usages",
            {
                "extras": {
                    "simplifions-cas-d-usages": {
                        "reco_solutions": [
                            {"Nom_de_la_recommandation": "some recommendation"}
                        ]
                    }
                }
            },
        )
        assert not topics_manager._topics_are_similar_so_we_can_skip_update(
            topic1, topic2
        )

    def test_equal_solutions_with_different_topics_references(self):
        topic1 = topics_factory.build_record("simplifions-solutions")
        topic2 = topics_factory.build_record(
            "simplifions-solutions",
            {
                "extras": {
                    "simplifions-solutions": {"cas_d_usages_topics_ids": ["some topic"]}
                }
            },
        )
        assert topics_manager._topics_are_similar_so_we_can_skip_update(topic1, topic2)


# ============ PYTEST FIXTURES ============


@pytest.fixture
def clear_topics():
    """Fixture to clear all topics before each test"""
    topics_factory.clear_all_resources()
    yield
    # Cleanup after test if needed
    topics_factory.clear_all_resources()


@pytest.fixture
def grist_solution_data():
    """Template for basic solution data in Grist format"""
    return {
        "slug": "test_solution_slug",
        "Ref_Nom_de_la_solution": "Test Solution Name",
        "Description_courte": "Test solution description",
        "Visible_sur_simplifions": True,
        "target_users": "particuliers",
        "budget": ["Aucun développement, ni budget"],
        "fournisseurs_de_service": "administrations-centrales",
        "types_de_simplification": ["⭐️ Accès facile"],
    }


@pytest.fixture
def grist_cas_usage_data():
    """Template for basic cas d'usage data in Grist format"""
    return {
        "slug": "test_cas_usage_slug",
        "Titre": "Test Cas Usage Name",
        "Description_courte": "Test cas usage description",
        "Visible_sur_simplifions": True,
        "target_users": "entreprises",
        "budget": ["Avec des moyens techniques"],
        "fournisseurs_de_service": "collectivites-territoriales",
        "types_de_simplification": ["⭐️⭐️ DLNUF"],
    }


@pytest.fixture
def existing_solution_topic():
    """Template for an existing solution topic"""
    return {
        "id": "existing_solution_id",
        "name": "Existing Solution Name",
        "description": "Existing solution description",
        "tags": ["simplifions", "simplifions-solutions"],
        "extras": {"simplifions-solutions": {"slug": "existing_solution_slug"}},
    }


@pytest.fixture
def existing_cas_usage_topic():
    """Template for an existing cas d'usage topic"""
    return {
        "id": "existing_cas_usage_id",
        "name": "Existing Cas Usage Name",
        "description": "Existing cas usage description",
        "tags": ["simplifions", "simplifions-cas-d-usages"],
        "extras": {"simplifions-cas-d-usages": {"slug": "existing_cas_usage_slug"}},
    }


@pytest.fixture
def expected_solution_tags():
    """Expected tags for a basic solution"""
    return [
        "simplifions",
        "simplifions-dag-generated",
        "simplifions-solutions",
        "simplifions-fournisseurs-de-service-administrations-centrales",
        "simplifions-target-users-particuliers",
        "simplifions-budget-aucun-developpement-ni-budget",
        "simplifions-types-de-simplification-acces-facile",
    ]


@pytest.fixture
def expected_cas_usage_tags():
    """Expected tags for a basic cas d'usage"""
    return [
        "simplifions",
        "simplifions-dag-generated",
        "simplifions-cas-d-usages",
        "simplifions-fournisseurs-de-service-collectivites-territoriales",
        "simplifions-target-users-entreprises",
        "simplifions-budget-avec-des-moyens-techniques",
        "simplifions-types-de-simplification-dlnuf",
    ]


def create_mock_ti_with_solutions(solutions_data):
    """Helper function to create mock task instance with solutions data"""
    return task_instance_factory.build_ti({"simplifions-solutions": solutions_data})


def create_mock_ti_with_cas_usages(cas_usages_data):
    """Helper function to create mock task instance with cas d'usages data"""
    return task_instance_factory.build_ti({"simplifions-cas-d-usages": cas_usages_data})


def create_mock_ti_with_mixed_data(solutions_data, cas_usages_data):
    """Helper function to create mock task instance with both solutions and cas d'usages data"""
    return task_instance_factory.build_ti(
        {
            "simplifions-solutions": solutions_data,
            "simplifions-cas-d-usages": cas_usages_data,
        }
    )


@pytest.fixture
def topic_to_keep_solution():
    """Template for a solution topic that should be kept during deletion tests"""
    return {
        "id": "topic_id_to_keep",
        "name": "Topic to keep",
        "description": "This topic should be kept",
        "tags": ["simplifions", "simplifions-solutions"],
        "extras": {"simplifions-solutions": {"slug": "topic_to_keep"}},
    }


@pytest.fixture
def topic_to_delete_solution():
    """Template for a solution topic that should be deleted during deletion tests"""
    return {
        "id": "topic_id_to_delete",
        "name": "Topic to delete",
        "description": "This topic should be deleted",
        "tags": ["simplifions", "simplifions-solutions"],
        "extras": {"simplifions-solutions": {"slug": "topic_to_delete"}},
    }


@pytest.fixture
def topic_to_keep_cas_usage():
    """Template for a cas d'usage topic that should be kept during deletion tests"""
    return {
        "id": "topic_id_to_keep",
        "name": "Topic to keep",
        "description": "This topic should be kept",
        "tags": ["simplifions", "simplifions-cas-d-usages"],
        "extras": {"simplifions-cas-d-usages": {"slug": "topic_to_keep"}},
    }


@pytest.fixture
def topic_to_delete_cas_usage():
    """Template for a cas d'usage topic that should be deleted during deletion tests"""
    return {
        "id": "topic_id_to_delete",
        "name": "Topic to delete",
        "description": "This topic should be deleted",
        "tags": ["simplifions", "simplifions-cas-d-usages"],
        "extras": {"simplifions-cas-d-usages": {"slug": "topic_to_delete"}},
    }


@pytest.fixture
def diversified_test_existing_topics():
    """Complex fixture for the diversified test with 4 existing topics organized by type"""
    return {
        "simplifions-cas-d-usages": [
            {
                "id": "existing_cas_1_id",
                "name": "Original Cas Usage 1",
                "description": "Original description 1",
                "tags": ["simplifions", "simplifions-cas-d-usages"],
                "extras": {"simplifions-cas-d-usages": {"slug": "existing_cas_1"}},
            },
            {
                "id": "existing_cas_2_id",
                "name": "Cas Usage to Delete",
                "description": "This will be deleted",
                "tags": ["simplifions", "simplifions-cas-d-usages"],
                "extras": {
                    "simplifions-cas-d-usages": {"slug": "existing_cas_2_to_delete"}
                },
            },
        ],
        "simplifions-solutions": [
            {
                "id": "existing_solution_1_id",
                "name": "Original Solution 1",
                "description": "Original solution description 1",
                "tags": ["simplifions", "simplifions-solutions"],
                "extras": {"simplifions-solutions": {"slug": "existing_solution_1"}},
            },
            {
                "id": "existing_solution_2_id",
                "name": "Solution to Delete",
                "description": "This solution will be deleted",
                "tags": ["simplifions", "simplifions-solutions"],
                "extras": {
                    "simplifions-solutions": {"slug": "existing_solution_2_to_delete"}
                },
            },
        ],
    }


@pytest.fixture
def diversified_test_grist_data():
    """Grist data for the diversified test with updates and new creations"""
    return {
        "cas_usages": {
            "existing_cas_1": {
                "slug": "existing_cas_1",
                "Titre": "Updated Cas Usage 1",
                "Description_courte": "Updated cas usage description",
                "Visible_sur_simplifions": True,
                "target_users": "entreprises",
                "budget": ["Avec des moyens techniques"],
                "types_de_simplification": ["⭐️⭐️ DLNUF"],
            },
            "new_cas_usage": {
                "slug": "new_cas_usage",
                "Titre": "Brand New Cas Usage",
                "Description_courte": "New cas usage description",
                "Visible_sur_simplifions": True,
                "target_users": "particuliers",
                "budget": ["Aucun développement, ni budget"],
                "types_de_simplification": ["⭐️ Accès facile"],
            },
        },
        "solutions": {
            "existing_solution_1": {
                "slug": "existing_solution_1",
                "Ref_Nom_de_la_solution": "Updated Solution 1",
                "Description_courte": "Updated solution description",
                "Visible_sur_simplifions": True,
                "target_users": "entreprises",
                "budget": ["Avec des moyens techniques"],
                "fournisseurs_de_service": "collectivites-territoriales",
                "types_de_simplification": ["⭐️⭐️⭐️ Proactivité"],
            },
            "new_solution": {
                "slug": "new_solution",
                "Ref_Nom_de_la_solution": "Brand New Solution",
                "Description_courte": "New solution description",
                "Visible_sur_simplifions": True,
                "target_users": "particuliers",
                "budget": ["Aucun développement, ni budget"],
                "fournisseurs_de_service": "administrations-centrales",
                "types_de_simplification": ["⭐️ Accès facile"],
            },
        },
    }


class TestUpdateTopics:
    def test_update_topics_with_new_solution(
        self, clear_topics, grist_solution_data, expected_solution_tags
    ):
        """Test that update_topics correctly creates a new solution if it doesn't exist"""

        mock_ti = create_mock_ti_with_solutions(
            {grist_solution_data["slug"]: grist_solution_data}
        )

        topics_manager.update_topics(mock_ti)

        topics = topics_factory.get_records("topics")
        assert len(topics) == 1

        created_topic = topics[0]
        assert created_topic["name"] == grist_solution_data["Ref_Nom_de_la_solution"]
        assert created_topic["description"] == grist_solution_data["Description_courte"]
        assert not created_topic["private"]
        assert created_topic["tags"] == expected_solution_tags
        assert (
            created_topic["extras"]["simplifions-solutions"]["slug"]
            == grist_solution_data["slug"]
        )

    def test_update_topics_with_new_cas_usage(
        self, clear_topics, grist_cas_usage_data, expected_cas_usage_tags
    ):
        """Test that update_topics correctly creates a new cas d'usage if it doesn't exist"""

        mock_ti = create_mock_ti_with_cas_usages(
            {grist_cas_usage_data["slug"]: grist_cas_usage_data}
        )

        topics_manager.update_topics(mock_ti)

        topics = topics_factory.get_records("topics")
        assert len(topics) == 1

        created_topic = topics[0]
        assert created_topic["name"] == grist_cas_usage_data["Titre"]
        assert (
            created_topic["description"] == grist_cas_usage_data["Description_courte"]
        )
        assert not created_topic["private"]
        assert created_topic["tags"] == expected_cas_usage_tags
        assert (
            created_topic["extras"]["simplifions-cas-d-usages"]["slug"]
            == grist_cas_usage_data["slug"]
        )

    def test_update_topics_with_existing_solution(
        self, clear_topics, existing_solution_topic
    ):
        """Test that update_topics correctly updates an existing solution instead of creating a duplicate"""

        # Create updated Grist data for the existing solution
        updated_grist_data = {
            "slug": existing_solution_topic["extras"]["simplifions-solutions"]["slug"],
            "Ref_Nom_de_la_solution": "Updated solution name",
            "Description_courte": "Updated solution description",
            "Visible_sur_simplifions": True,
        }

        mock_ti = create_mock_ti_with_solutions(
            {updated_grist_data["slug"]: updated_grist_data}
        )

        # Create the existing topic
        existing_topic = topics_factory.create_record(
            "simplifions-solutions", existing_solution_topic
        )

        # Verify we have exactly 1 topic before update
        topics_before = topics_factory.resource_mock().get_records("topics")
        assert len(topics_before) == 1
        assert topics_before[0]["name"] == existing_topic["name"]

        # Call update_topics - this should update the existing topic, not create a new one
        topics_manager.update_topics(mock_ti)

        # Verify we still have exactly 1 topic after update (not 2)
        topics_after = topics_factory.resource_mock().get_records("topics")
        assert len(topics_after) == 1

        # Verify the topic was updated with new data
        updated_topic = topics_after[0]
        assert updated_topic["id"] == existing_topic["id"]  # Same ID
        assert (
            updated_topic["name"] == updated_grist_data["Ref_Nom_de_la_solution"]
        )  # Updated name
        assert (
            updated_topic["description"] == updated_grist_data["Description_courte"]
        )  # Updated description

    def test_update_topics_with_existing_cas_d_usage(
        self, clear_topics, existing_cas_usage_topic
    ):
        """Test that update_topics correctly updates an existing cas d'usage instead of creating a duplicate"""

        # Create updated Grist data for the existing cas d'usage
        updated_grist_data = {
            "slug": existing_cas_usage_topic["extras"]["simplifions-cas-d-usages"][
                "slug"
            ],
            "Titre": "Updated cas usage name",
            "Description_courte": "Updated cas usage description",
            "Visible_sur_simplifions": True,
        }

        mock_ti = create_mock_ti_with_cas_usages(
            {updated_grist_data["slug"]: updated_grist_data}
        )

        # Create the existing topic with original data
        existing_topic_with_original_data = {
            **existing_cas_usage_topic,
            "name": "Original cas usage name",
            "description": "Original cas usage description",
        }
        existing_topic = topics_factory.create_record(
            "simplifions-cas-d-usages", existing_topic_with_original_data
        )

        # Verify we have exactly 1 topic before update
        topics_before = topics_factory.resource_mock().get_records("topics")
        assert len(topics_before) == 1
        assert topics_before[0]["name"] == existing_topic["name"]

        # Call update_topics - this should update the existing topic, not create a new one
        topics_manager.update_topics(mock_ti)

        # Verify we still have exactly 1 topic after update (not 2)
        topics_after = topics_factory.resource_mock().get_records("topics")
        assert len(topics_after) == 1

        # Verify the topic was updated with new data
        updated_topic = topics_after[0]
        assert updated_topic["id"] == existing_topic["id"]  # Same ID
        assert updated_topic["name"] == updated_grist_data["Titre"]  # Updated name
        assert (
            updated_topic["description"] == updated_grist_data["Description_courte"]
        )  # Updated description

    def test_update_topics_with_deleted_solution(
        self, clear_topics, topic_to_keep_solution, topic_to_delete_solution
    ):
        """Test that update_topics correctly deletes a solution if it doesn't exist in the grist data"""

        # Create two existing topics
        topics_factory.create_record("simplifions-solutions", topic_to_keep_solution)
        topics_factory.create_record("simplifions-solutions", topic_to_delete_solution)

        # Verify we have 2 topics before update
        topics_before = topics_factory.resource_mock().get_records("topics")
        assert len(topics_before) == 2

        # Create Grist data with only the topic to keep (implicitly deleting the other)
        keep_grist_data = {
            "slug": topic_to_keep_solution["extras"]["simplifions-solutions"]["slug"],
            "Ref_Nom_de_la_solution": "Updated solution name",
            "Description_courte": "This topic should be kept",
            "Visible_sur_simplifions": True,
        }

        mock_ti = create_mock_ti_with_solutions(
            {keep_grist_data["slug"]: keep_grist_data}
        )

        topics_manager.update_topics(mock_ti)

        # Verify we now have only 1 topic (the deleted one should be gone)
        topics_after = topics_factory.resource_mock().get_records("topics")
        assert len(topics_after) == 1

        # Verify the remaining topic is the correct one
        remaining_topic = topics_after[0]
        assert (
            remaining_topic["extras"]["simplifions-solutions"]["slug"]
            == keep_grist_data["slug"]
        )
        assert remaining_topic["name"] == keep_grist_data["Ref_Nom_de_la_solution"]
        assert remaining_topic["description"] == keep_grist_data["Description_courte"]

    def test_update_topics_with_deleted_cas_d_usage(
        self, clear_topics, topic_to_keep_cas_usage, topic_to_delete_cas_usage
    ):
        """Test that update_topics correctly deletes a cas d'usage if it doesn't exist in the grist data"""

        # Create two existing topics
        topics_factory.create_record(
            "simplifions-cas-d-usages", topic_to_keep_cas_usage
        )
        topics_factory.create_record(
            "simplifions-cas-d-usages", topic_to_delete_cas_usage
        )

        # Verify we have 2 topics before update
        topics_before = topics_factory.resource_mock().get_records("topics")
        assert len(topics_before) == 2

        # Create Grist data with only the topic to keep (implicitly deleting the other)
        keep_grist_data = {
            "slug": topic_to_keep_cas_usage["extras"]["simplifions-cas-d-usages"][
                "slug"
            ],
            "Titre": "Updated cas usage name",
            "Description_courte": "This topic should be kept",
            "Visible_sur_simplifions": True,
        }

        mock_ti = create_mock_ti_with_cas_usages(
            {keep_grist_data["slug"]: keep_grist_data}
        )

        topics_manager.update_topics(mock_ti)

        # Verify we now have only 1 topic (the deleted one should be gone)
        topics_after = topics_factory.resource_mock().get_records("topics")
        assert len(topics_after) == 1

        # Verify the remaining topic is the correct one
        remaining_topic = topics_after[0]
        assert (
            remaining_topic["extras"]["simplifions-cas-d-usages"]["slug"]
            == keep_grist_data["slug"]
        )
        assert remaining_topic["name"] == keep_grist_data["Titre"]
        assert remaining_topic["description"] == keep_grist_data["Description_courte"]

    def test_update_topics_with_diversified_data(
        self,
        clear_topics,
        diversified_test_existing_topics,
        diversified_test_grist_data,
    ):
        """Test comprehensive scenario with mixed operations: existing updates, new creations, and deletions"""

        # Create all existing topics
        for topic_type, topics_list in diversified_test_existing_topics.items():
            for topic_data in topics_list:
                topics_factory.create_record(topic_type, topic_data)

        # Verify initial state: 4 topics total (2 cas-d-usages + 2 solutions)
        topics_before = topics_factory.get_records("topics")
        assert len(topics_before) == 4

        # Create mock task instance with mixed data that will:
        # - Update existing_cas_1 and existing_solution_1
        # - Create new_cas_usage and new_solution
        # - Implicitly delete existing_cas_2_to_delete and existing_solution_2_to_delete (not in grist data)
        mock_ti = create_mock_ti_with_mixed_data(
            diversified_test_grist_data["solutions"],
            diversified_test_grist_data["cas_usages"],
        )

        # Execute the update
        topics_manager.update_topics(mock_ti)

        # Verify final state: 4 topics total (2 cas-d-usages + 2 solutions)
        # - 1 updated cas-d-usage + 1 new cas-d-usage = 2 cas-d-usages
        # - 1 updated solution + 1 new solution = 2 solutions
        # - 2 topics deleted (existing_cas_2_to_delete, existing_solution_2_to_delete)
        topics_after = topics_factory.get_records("topics")
        assert len(topics_after) == 4

        # Filter topics by type for more detailed assertions
        cas_usages_after = [
            t for t in topics_after if "simplifions-cas-d-usages" in t["tags"]
        ]
        solutions_after = [
            t for t in topics_after if "simplifions-solutions" in t["tags"]
        ]

        # Simple length checks as requested
        assert len(cas_usages_after) == 2  # 1 updated + 1 new
        assert len(solutions_after) == 2  # 1 updated + 1 new

        # Verify we have the expected slugs (updated + new, deleted ones should be gone)
        cas_usage_slugs = {
            t["extras"]["simplifions-cas-d-usages"]["slug"] for t in cas_usages_after
        }
        solution_slugs = {
            t["extras"]["simplifions-solutions"]["slug"] for t in solutions_after
        }

        assert cas_usage_slugs == {"existing_cas_1", "new_cas_usage"}
        assert solution_slugs == {"existing_solution_1", "new_solution"}


class TestUpdateTopicsReferences:
    def test_update_topics_references_happy_path(self, clear_topics):
        """Test basic happy path of update_topics_references with simple solution and cas d'usage"""

        # Create a solution topic that references a cas d'usage
        topics_factory.create_record(
            "simplifions-solutions",
            {
                "id": "solution-1",
                "name": "Test Solution (publique)",
                "private": False,
                "tags": ["simplifions", "simplifions-solutions"],
                "extras": {
                    "simplifions-solutions": {
                        "slug": "grist_solution_1",
                        "cas_d_usages_slugs": ["grist_cas_usage_1"],
                        "is_public": True,
                    }
                },
            },
        )
        topics_factory.create_record(
            "simplifions-solutions",
            {
                "id": "solution-2",
                "name": "Test Solution 2 (editeur)",
                "private": False,
                "tags": ["simplifions", "simplifions-solutions"],
                "extras": {
                    "simplifions-solutions": {
                        "slug": "grist_solution_2",
                        "operateur_nom": "Test Editeur",
                        "is_public": False,
                    }
                },
            },
        )

        # Create a cas d'usage topic that should reference both solutions
        topics_factory.create_record(
            "simplifions-cas-d-usages",
            {
                "id": "cas-usage-1",
                "name": "Test Cas Usage",
                "private": False,
                "tags": ["simplifions", "simplifions-cas-d-usages"],
                "extras": {
                    "simplifions-cas-d-usages": {
                        "slug": "grist_cas_usage_1",
                        "reco_solutions": [
                            {
                                "solution_slug": "grist_solution_1",
                                "solution_editeurs_slugs": ["grist_solution_2"],
                            }
                        ],
                    }
                },
            },
        )

        # Mock task instance (update_topics_references doesn't need xcom data)
        mock_ti = task_instance_factory.build_ti({})

        # Execute the method
        topics_manager.update_topics_references(mock_ti)

        # Verify the solution topic got the cas d'usage reference
        solution_updated = topics_factory.get_record_by_id("topics", "solution-1")
        assert (
            "cas_d_usages_topics_ids"
            in solution_updated["extras"]["simplifions-solutions"]
        )
        assert solution_updated["extras"]["simplifions-solutions"][
            "cas_d_usages_topics_ids"
        ] == ["cas-usage-1"]

        # Verify the cas d'usage topic got the solution reference
        cas_usage_updated = topics_factory.get_record_by_id("topics", "cas-usage-1")
        assert (
            "solution_topic_id"
            in cas_usage_updated["extras"]["simplifions-cas-d-usages"][
                "reco_solutions"
            ][0]
        )
        assert (
            cas_usage_updated["extras"]["simplifions-cas-d-usages"]["reco_solutions"][
                0
            ]["solution_topic_id"]
            == "solution-1"
        )

        # Verify the solution topic got the editeur reference
        solution_updated = topics_factory.get_record_by_id("topics", "solution-2")
        assert (
            "solutions_editeurs_topics"
            in cas_usage_updated["extras"]["simplifions-cas-d-usages"][
                "reco_solutions"
            ][0]
        )
        assert cas_usage_updated["extras"]["simplifions-cas-d-usages"][
            "reco_solutions"
        ][0]["solutions_editeurs_topics"] == [
            {
                "topic_id": "solution-2",
                "solution_name": "Test Solution 2 (editeur)",
                "editeur_name": "Test Editeur",
            }
        ]
