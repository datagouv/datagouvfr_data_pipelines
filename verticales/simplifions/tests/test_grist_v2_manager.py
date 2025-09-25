from unittest.mock import Mock

# The factory must be imported before the manager because it initializes the mocks
from factories.grist_factory import GristFactory
from grist_v2_manager import GristV2Manager

grist_v2_manager = GristV2Manager()
grist_factory = GristFactory()


def test_clean_row():
    row = {
        "id": 1,
        "fields": {
            "key": "value",
            "list_key": ["L", "1", "2", "3"],
            "list_key_2": ["L", "A", "B", "C"],
        },
    }
    cleaned_row = grist_v2_manager._clean_row(row)
    assert cleaned_row == {
        "id": 1,
        "fields": {
            "key": "value",
            "list_key": ["1", "2", "3"],
            "list_key_2": ["A", "B", "C"],
        },
    }


class TestGetAndFormatGristV2Data:
    def setup_method(self):
        grist_factory.clear_all_resources()

    def test_first_ti_call_with_one_record_each(self):
        grist_factory.create_record("Cas_d_usages")
        grist_factory.create_record("Solutions")

        ti_mock = Mock()
        grist_v2_manager.get_and_format_grist_v2_data(ti_mock)

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
        grist_v2_manager.get_and_format_grist_v2_data(ti_mock)

        ti_mock.xcom_push.assert_called()
        first_call = ti_mock.xcom_push.call_args_list[0][1]["value"]

        assert len(first_call["simplifions-v2-cas-d-usages"]) == 2
        assert len(first_call["simplifions-v2-solutions"]) == 5

    def test_second_ti_call(self):
        grist_factory.create_records("Fournisseurs_de_services", 1)
        grist_factory.create_records("Types_de_simplification", 2)
        grist_factory.create_records("Usagers", 3)
        grist_factory.create_records("Budgets_de_mise_en_oeuvre", 4)

        ti_mock = Mock()
        grist_v2_manager.get_and_format_grist_v2_data(ti_mock)

        ti_mock.xcom_push.assert_called()
        second_call = ti_mock.xcom_push.call_args_list[1][1]["value"]

        assert len(second_call["Fournisseurs_de_services"]) == 1
        assert len(second_call["Types_de_simplification"]) == 2
        assert len(second_call["Usagers"]) == 3
        assert len(second_call["Budgets_de_mise_en_oeuvre"]) == 4
