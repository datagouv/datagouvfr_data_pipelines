import pytest
from unittest.mock import Mock, patch
import time

# The factory must be imported before the task functions because it initializes the mocks
from factories.grist_factory import GristFactory
from factories.topics_factory import TopicsFactory
from factories.task_instance_factory import TaskInstanceFactory
from task_functions import (
    get_and_format_grist_v2_data,
    update_topics_v2,
    watch_grist_data,
)
from datagouvfr_data_pipelines.utils.datagouv import local_client

grist_factory = GristFactory()
topics_factory = TopicsFactory()
task_instance_factory = TaskInstanceFactory()


class TestGetAndFormatGristV2Data:
    def setup_method(self):
        grist_factory.clear_all_resources()

    def test_first_ti_call_with_one_record_each(self):
        grist_factory.create_record("Cas_d_usages")
        grist_factory.create_record("Solutions")

        ti_mock = Mock()
        get_and_format_grist_v2_data(ti_mock)

        # Verify xcom_push was called with correct structure
        ti_mock.xcom_push.assert_called()
        first_call = ti_mock.xcom_push.call_args_list[0][1]["value"]

        # Check basic structure: both tags present with expected counts
        assert len(first_call["simplifions-v2-cas-d-usages"]) == 1
        assert len(first_call["simplifions-v2-solutions"]) == 1
        # Check the structure : { id: row }
        assert 1 in first_call["simplifions-v2-cas-d-usages"]
        # Check the structure of a row : { id: 1, fields: { Nom: "bla", ...} }
        assert first_call["simplifions-v2-cas-d-usages"][1]["id"] == 1
        assert (
            first_call["simplifions-v2-cas-d-usages"][1]["fields"]["Nom"]
            == "Marchés publics | Dépôt et instruction des candidatures"
        )

    def test_first_ti_call_with_multiple_records(self):
        grist_factory.create_records("Cas_d_usages", 2)
        grist_factory.create_records("Solutions", 5)

        ti_mock = Mock()
        get_and_format_grist_v2_data(ti_mock)

        ti_mock.xcom_push.assert_called()
        first_call = ti_mock.xcom_push.call_args_list[0][1]["value"]

        assert len(first_call["simplifions-v2-cas-d-usages"]) == 2
        assert len(first_call["simplifions-v2-solutions"]) == 5

    def test_second_ti_call(self):
        grist_factory.create_records("Fournisseurs_de_services", 1)
        grist_factory.create_records("Types_de_simplification", 2)
        grist_factory.create_records("Usagers", 3)
        grist_factory.create_records("Budgets_de_mise_en_oeuvre", 4)
        grist_factory.create_records("Categories_de_solution", 2)

        ti_mock = Mock()
        get_and_format_grist_v2_data(ti_mock)

        ti_mock.xcom_push.assert_called()
        second_call = ti_mock.xcom_push.call_args_list[1][1]["value"]

        assert len(second_call["Fournisseurs_de_services"]) == 1
        assert len(second_call["Types_de_simplification"]) == 2
        assert len(second_call["Usagers"]) == 3
        assert len(second_call["Budgets_de_mise_en_oeuvre"]) == 4
        assert len(second_call["Categories_de_solution"]) == 2


class TestWatchGristData:
    def setup_method(self):
        grist_factory.clear_all_resources()

    @staticmethod
    def create_backup_mock_side_effect(backup_data):
        def side_effect(table_id, filter=None, document_id=None):
            if document_id and document_id != "ofSVjCSAnMb6SGZSb7GGrv":
                return backup_data
            raise Exception("Unexpected call to _request_table_records")

        return side_effect

    @patch("task_functions.send_message")
    def test_watch_grist_data(self, mock_send_message):
        # Mock the table metadata endpoints that watch_grist_data uses
        grist_factory.resource_mock().mock_table_metadata()

        # Create test data with a recently modified record
        grist_factory.create_records("Cas_d_usages", 2)
        grist_factory.create_records("Solutions", 2)
        grist_factory.create_record(
            "Cas_d_usages",
            {
                "Modifie_le": time.time()
                - 3600,  # Modified 1 hour ago (within 24h window)
                "Modifie_par": "Testman the tester",
                "technical_title": "Test Case Modified",
            },
        )

        ti_mock = Mock()
        watch_grist_data(ti_mock)

        # Verify that send_message was called
        mock_send_message.assert_called_once()

        # Get the call arguments
        call_args = mock_send_message.call_args
        message_text = call_args.kwargs["text"]

        # Assert message content contains expected information
        assert "Cas_d_usages" in message_text
        assert "Testman the tester" in message_text
        assert "Test Case Modified" in message_text

        assert "Solutions" not in message_text

    @patch("task_functions.send_message")
    @patch("task_functions.GristV2Manager._request_table_records")
    def test_watch_grist_data_with_diff(
        self, mock_request_table_records, mock_send_message
    ):
        # Mock the table metadata endpoints
        grist_factory.resource_mock().mock_table_metadata()

        # Create the modified record - it will have id=1 (first record in Cas_d_usages)
        grist_factory.create_record(
            "Cas_d_usages",
            {
                "Nom": "Test Case Modified",
                "Description_courte": "New description",
                "Modifie_le": time.time() - 3600,  # Modified 1 hour ago
                "Modifie_par": "Testman the tester",
                "technical_title": "Test Case Modified",
                "anchor_link": "https://example.com/test",
            },
        )

        # Define backup data with old values - id=1 matches the first record
        backup_data = [
            {
                "id": 1,
                "fields": {
                    "Nom": "Test Case Original",
                    "Description_courte": "Old description",
                    "Modifie_le": time.time() - 86400,  # Modified 24 hours ago
                    "Modifie_par": "Original author",
                    "technical_title": "Test Case Modified",
                    "anchor_link": "https://example.com/test",
                },
            }
        ]

        # Mock _request_table_records to return backup data when called with backup document_id
        mock_request_table_records.side_effect = self.create_backup_mock_side_effect(
            backup_data
        )

        ti_mock = Mock()
        watch_grist_data(ti_mock)

        # Verify that send_message was called
        mock_send_message.assert_called_once()

        # Get the call arguments
        call_args = mock_send_message.call_args
        message_text = call_args.kwargs["text"]

        # Assert message content contains expected information
        assert "Cas_d_usages" in message_text
        assert "Testman the tester" in message_text
        assert "Test Case Modified" in message_text

        # Assert diff information is present
        assert "Nom" in message_text
        assert ":heavy_minus_sign: `Test Case Original`" in message_text
        assert ":heavy_plus_sign: `Test Case Modified`" in message_text
        assert "Description_courte" in message_text
        assert ":heavy_minus_sign: `Old description`" in message_text
        assert ":heavy_plus_sign: `New description`" in message_text

    @patch("task_functions.send_message")
    @patch("task_functions.GristV2Manager._request_table_records")
    def test_watch_grist_data_with_new_record(
        self, mock_request_table_records, mock_send_message
    ):
        # Mock the table metadata endpoints
        grist_factory.resource_mock().mock_table_metadata()

        # Create a recently modified record that is new (doesn't exist in backup)
        grist_factory.create_record(
            "Cas_d_usages",
            {
                "Nom": "Brand New Case",
                "Description_courte": "This is a new record",
                "Modifie_le": time.time() - 3600,  # Modified 1 hour ago
                "Modifie_par": "Testman the tester",
                "technical_title": "Brand New Case",
                "anchor_link": "https://example.com/new",
            },
        )

        # Mock _request_table_records to return empty backup data (record doesn't exist in backup)
        mock_request_table_records.side_effect = self.create_backup_mock_side_effect([])

        ti_mock = Mock()
        watch_grist_data(ti_mock)

        # Verify that send_message was called
        mock_send_message.assert_called_once()

        # Get the call arguments
        call_args = mock_send_message.call_args
        message_text = call_args.kwargs["text"]

        # Assert message content contains expected information
        assert "Cas_d_usages" in message_text
        assert "Testman the tester" in message_text
        assert "Brand New Case" in message_text

        # Assert that the new line prefix is present
        assert "(Nouvelle ligne)" in message_text

        # Assert that no diff information is present (since it's a new record, not modified)
        assert "Backup:" not in message_text
        assert "Nouveau:" not in message_text

    @patch("task_functions.send_message")
    @patch("task_functions.GristV2Manager._request_table_records")
    def test_watch_grist_data_with_deleted_record(
        self, mock_request_table_records, mock_send_message
    ):
        # Mock the table metadata endpoints
        grist_factory.resource_mock().mock_table_metadata()

        # Create one current record (id=1)
        grist_factory.create_record(
            "Cas_d_usages",
            {
                "Nom": "Current Case",
                "Modifie_le": time.time() - 7200,  # Modified 2 hours ago
                "Modifie_par": "Current author",
                "technical_title": "Current Case",
                "anchor_link": "https://example.com/current",
            },
        )

        # Define backup data with two records: the current one + a deleted one
        backup_data = [
            {
                "id": 1,
                "fields": {
                    "Nom": "Current Case",
                    "Modifie_le": time.time() - 7200,
                    "Modifie_par": "Current author",
                    "technical_title": "Current Case",
                    "anchor_link": "https://example.com/current",
                },
            },
            {
                "id": 2,
                "fields": {
                    "Nom": "Deleted Case",
                    "Description_courte": "This record was deleted",
                    "Modifie_le": time.time() - 3600,  # Modified 1 hour ago
                    "Modifie_par": "Deleter Person",
                    "technical_title": "Deleted Case",
                    "anchor_link": "https://example.com/deleted",
                },
            },
        ]

        # Mock _request_table_records to return appropriate data
        mock_request_table_records.side_effect = self.create_backup_mock_side_effect(
            backup_data
        )

        ti_mock = Mock()
        watch_grist_data(ti_mock)

        # Verify that send_message was called
        mock_send_message.assert_called_once()

        # Get the call arguments
        call_args = mock_send_message.call_args
        message_text = call_args.kwargs["text"]

        # Assert message content contains expected information about the deleted record
        assert "Lignes supprimées" in message_text
        assert "Cas_d_usages" in message_text
        assert "Deleted Case" in message_text

    @patch("task_functions.send_message")
    @patch("task_functions.GristV2Manager._request_table_records")
    def test_watch_grist_data_with_only_deleted_rows(
        self, mock_request_table_records, mock_send_message
    ):
        """Test that tables with only deleted rows (no modified rows) are still reported"""
        # Mock the table metadata endpoints
        grist_factory.resource_mock().mock_table_metadata()

        # Create a record in Cas_d_usages that is NOT recently modified
        # (this ensures Cas_d_usages won't be in tables_with_modified_rows)
        grist_factory.create_record(
            "Cas_d_usages",
            {
                "Nom": "Old Case",
                "Modifie_le": time.time()
                - 86400 * 2,  # Modified 2 days ago (outside 24h window)
                "Modifie_par": "Old author",
                "technical_title": "Old Case",
                "anchor_link": "https://example.com/old",
            },
        )

        # Define backup data with records that were deleted (exist in backup but not in current)
        # The backup contains two records, but current only has one
        backup_data = [
            {
                "id": 1,
                "fields": {
                    "Nom": "Old Case",
                    "Modifie_le": time.time() - 86400 * 2,
                    "Modifie_par": "Old author",
                    "technical_title": "Old Case",
                    "anchor_link": "https://example.com/old",
                },
            },
            {
                "id": 2,
                "fields": {
                    "Nom": "Recently Deleted Case",
                    "Description_courte": "This was recently deleted",
                    "Modifie_le": time.time()
                    - 3600,  # Modified 1 hour ago (within 24h window)
                    "Modifie_par": "Deleter Person",
                    "technical_title": "Recently Deleted Case",
                    "anchor_link": "https://example.com/deleted-case",
                },
            },
        ]

        # Mock _request_table_records to return backup data when called with backup document_id
        mock_request_table_records.side_effect = self.create_backup_mock_side_effect(
            backup_data
        )

        ti_mock = Mock()
        watch_grist_data(ti_mock)

        # Verify that send_message was called
        mock_send_message.assert_called_once()

        # Get the call arguments
        call_args = mock_send_message.call_args
        message_text = call_args.kwargs["text"]

        # Assert that Cas_d_usages table is in the message (even though it has no modified rows)
        assert "Cas_d_usages" in message_text
        assert "Lignes supprimées" in message_text
        assert "Recently Deleted Case" in message_text

        # Assert that the old case (not recently modified) is NOT mentioned in deleted rows
        assert "Old Case" not in message_text or "Recently Deleted Case" in message_text


class TestUpdateTopicsV2:
    def setup_method(self):
        topics_factory.clear_all_resources()

    @pytest.fixture
    def grist_tables_for_filters(self):
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
                # Test with a list of slugs
                {
                    "id": 1,
                    "fields": {
                        "Label": "Fournisseur 1",
                        "slugs": ["f1", "f1-bis"],
                        # this one should be ignored, slugs is preferred
                        "slug": "f1-ter",
                    },
                },
                {"id": 2, "fields": {"Label": "Fournisseur 2", "slug": "f2"}},
                # duplicates one of fournisseur 1's slugs
                {"id": 3, "fields": {"Label": "Fournisseur 3", "slug": "f1"}},
            ],
            "Categories_de_solution": [
                {
                    "id": 1,
                    "fields": {"Nom": "Brique technique", "slug": "brique-technique"},
                },
                {
                    "id": 2,
                    "fields": {"Nom": "Logiciel métier", "slug": "logiciel-metier"},
                },
            ],
        }

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
                    "A_destination_de": [1, 3],
                    "Pour_simplifier_les_demarches_de": 2,
                    "Categorie_de_solution": [1, 2],
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

        update_topics_v2(mock_ti, local_client)

        topics = topics_factory.get_records("topics")
        assert len(topics) == 1
        assert topics[0]["name"] == "Solution 1"
        assert topics[0]["description"] == "Blabla"
        assert not topics[0]["private"]
        assert sorted(topics[0]["tags"]) == sorted(
            [
                "simplifions-v2",
                "simplifions-v2-dag-generated",
                "simplifions-v2-solutions",
                "simplifions-v2-solutions-1",
                "simplifions-v2-budget-b1",
                "simplifions-v2-types-de-simplification-t2",
                "simplifions-v2-fournisseurs-de-service-f1",
                "simplifions-v2-fournisseurs-de-service-f1-bis",
                "simplifions-v2-target-users-u2",
                "simplifions-v2-categorie-de-solution-brique-technique",
                "simplifions-v2-categorie-de-solution-logiciel-metier",
            ]
        )
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
        assert topics[0]["extras"]["simplifions-v2-solutions"]["A_destination_de"] == [
            {"id": 1, "label": "Fournisseur 1"},
            {"id": 3, "label": "Fournisseur 3"},
        ]

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

        update_topics_v2(mock_ti, local_client)

        topics = topics_factory.get_records("topics")
        assert len(topics) == 1
        assert topics[0]["name"] == "👋 Cas usage 1"
        assert topics[0]["description"] == "Blabla"
        assert not topics[0]["private"]
        assert sorted(topics[0]["tags"]) == sorted(
            [
                "simplifions-v2",
                "simplifions-v2-dag-generated",
                "simplifions-v2-cas-d-usages",
                "simplifions-v2-cas-d-usages-1",
                "simplifions-v2-budget-b1",
                "simplifions-v2-types-de-simplification-t2",
                "simplifions-v2-fournisseurs-de-service-f1",
                "simplifions-v2-fournisseurs-de-service-f1-bis",
                "simplifions-v2-target-users-u2",
            ]
        )
        assert topics[0]["extras"]["simplifions-v2-cas-d-usages"]["id"] == 1
        assert topics[0]["extras"]["simplifions-v2-cas-d-usages"][
            "A_destination_de"
        ] == [
            {"id": 1, "label": "Fournisseur 1"},
        ]

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

        update_topics_v2(mock_ti, local_client)

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

        update_topics_v2(mock_ti, local_client)

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
                "extras": {"simplifions-v2-solutions": {"id": 1}},
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
                    "Categorie_de_solution": 1,
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

        update_topics_v2(mock_ti, local_client)

        topics = topics_factory.get_records("topics")
        assert len(topics) == 1
        assert topics[0]["name"] == "Solution 1"
        assert topics[0]["description"] == "Blabla"
        assert not topics[0]["private"]
        assert sorted(topics[0]["tags"]) == sorted(
            [
                "simplifions-v2",
                "simplifions-v2-dag-generated",
                "simplifions-v2-solutions",
                "simplifions-v2-solutions-1",
                "simplifions-v2-budget-b1",
                "simplifions-v2-types-de-simplification-t2",
                "simplifions-v2-fournisseurs-de-service-f1",
                "simplifions-v2-fournisseurs-de-service-f1-bis",
                "simplifions-v2-target-users-u2",
                "simplifions-v2-categorie-de-solution-brique-technique",
            ]
        )
        assert topics[0]["extras"]["simplifions-v2-solutions"]["id"] == 1
        assert topics[0]["extras"]["simplifions-v2-solutions"]["A_destination_de"] == [
            {"id": 1, "label": "Fournisseur 1"},
        ]

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

        update_topics_v2(mock_ti, local_client)

        topics = topics_factory.get_records("topics")
        assert len(topics) == 1
        assert topics[0]["extras"]["simplifions-v2-solutions"]["id"] == 2
        assert topics[0]["name"] == "New Solution"

    def test_skip_topics_with_empty_names(self, grist_tables_for_filters):
        solutions_data = {
            1: {
                "id": 1,
                "fields": {
                    "Nom": "",  # Empty name
                    "Description_courte": "Solution with empty name",
                    "Visible_sur_simplifions": True,
                },
            },
            2: {
                "id": 2,
                "fields": {
                    "Nom": None,  # None name
                    "Description_courte": "Solution with None name",
                    "Visible_sur_simplifions": True,
                },
            },
            3: {
                "id": 3,
                "fields": {
                    "Nom": "Valid Solution",  # Valid name
                    "Description_courte": "Solution with valid name",
                    "Visible_sur_simplifions": True,
                },
            },
        }
        cas_usages_data = {
            1: {
                "id": 1,
                "fields": {
                    "Nom": "Valid Cas Usage",  # Valid name
                    "Description_courte": "Cas usage with valid name",
                    "Visible_sur_simplifions": True,
                    "Icone_du_titre": "📋",
                },
            },
            2: {
                "id": 2,
                "fields": {
                    "Nom": "",  # Empty name
                    "Description_courte": "Cas usage with empty name but with icon",
                    "Visible_sur_simplifions": True,
                    "Icone_du_titre": "📋",  # With icon present
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

        update_topics_v2(mock_ti, local_client)

        topics = topics_factory.get_records("topics")
        # Should only create 2 topics: 1 valid solution + 1 valid cas d'usage
        # Should skip: 1 empty solution + 1 None solution
        assert len(topics) == 2

        topic_names = [topic["name"] for topic in topics]

        # Check that only valid topics were created
        assert "Valid Solution" in topic_names
        assert "📋 Valid Cas Usage" in topic_names

        # Ensure empty/None names are not in created topics
        assert "" not in topic_names
        assert None not in topic_names
        assert "📋 " not in topic_names
