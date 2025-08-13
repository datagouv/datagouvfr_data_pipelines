import sys
from pathlib import Path
import pytest
import requests_mock

# Add the parent directory to the path so we can import the module
sys.path.append(str(Path(__file__).parent.parent))

# The factory must be imported before the manager because it initializes the mocks
from grist_factory import GristFactory
from grist_manager import GristManager

grist_manager = GristManager()
grist_factory = GristFactory()


@pytest.fixture(scope="function")
def diversified_test_data():
    """Create diversified test data with varying record counts for all tables and subtables"""
    grist_factory.clear_all_tables()

    # Create subdata records with different counts
    # Apidata: 3 records (most referenced subtable)
    for i in range(1, 4):
        grist_factory.create_record(
            "Apidata",
            {
                "Nom_donnees_ou_API": f"API {i}",
                "Type": "API",
                "Visible_sur_simplifions": True,
            },
        )
    # 4th record is hidden
    grist_factory.create_record(
        "Apidata",
        {
            "Nom_donnees_ou_API": "API 4",
            "Type": "API",
            "Visible_sur_simplifions": False,
        },
    )

    # SIMPLIFIONS_reco_solutions_cas_usages: 2 records
    for i in range(1, 3):
        grist_factory.create_record(
            "SIMPLIFIONS_reco_solutions_cas_usages",
            {
                "Titre": f"Reco Solution {i}",
                "Visible_sur_simplifions": True,
            },
        )

    # SIMPLIFIONS_description_apidata_cas_usages: 1 record
    grist_factory.create_record(
        "SIMPLIFIONS_description_apidata_cas_usages",
        {
            "Description": "API Description 1",
            "Visible_sur_simplifions": True,
        },
    )

    # Create main table records with different counts and varying subdata references

    # SIMPLIFIONS_cas_usages: 2 records
    grist_factory.create_record(
        "SIMPLIFIONS_cas_usages",
        {
            "slug": "cas-usage-1",
            "Titre": "Cas Usage 1",
            "API_et_donnees_utiles": ["L", 1, 2],  # References 2 APIs
            "reco_solutions": ["L", 1],  # References 1 reco
            "descriptions_api_et_donnees_utiles": ["L", 1],  # References 1 description
        },
    )
    grist_factory.create_record(
        "SIMPLIFIONS_cas_usages",
        {
            "slug": "cas-usage-2",
            "Titre": "Cas Usage 2",
            "API_et_donnees_utiles": ["L", 3],  # References 1 API
            "reco_solutions": ["L", 1, 2],  # References 2 recos
            "descriptions_api_et_donnees_utiles": ["L", 1],  # References 1 description
        },
    )

    # SIMPLIFIONS_produitspublics: 1 record
    grist_factory.create_record(
        "SIMPLIFIONS_produitspublics",
        {
            "slug": "produit-public-1",
            "Ref_Nom_de_la_solution": "Produit Public 1",
            "API_et_data_disponibles": [
                "L",
                1,
                2,
                3,
                4,
            ],  # References all 4 APIs, even the hidden one
        },
    )
    # and another one hidden
    grist_factory.create_record(
        "SIMPLIFIONS_produitspublics",
        {
            "slug": "produit-public-2",
            "Ref_Nom_de_la_solution": "Produit Public 2",
            "API_et_data_disponibles": ["L", 1, 2],
            "Visible_sur_simplifions": False,
        },
    )

    # SIMPLIFIONS_solutions_editeurs: 3 records
    for i in range(1, 4):
        grist_factory.create_record(
            "SIMPLIFIONS_solutions_editeurs",
            {
                "slug": f"solution-editeur-{i}",
                "Ref_Nom_de_la_solution": f"Solution Editeur {i}",
                "API_et_data_disponibles": ["L", 1, 3],  # References API 1 and 3
            },
        )


def test_clean_row():
    row = {
        "key": "value",
        "list_key": ["L", "1", "2", "3"],
        "list_key_2": ["L", "A", "B", "C"],
    }
    cleaned_row = grist_manager._clean_row(row)
    assert cleaned_row == {
        "key": "value",
        "list_key": ["1", "2", "3"],
        "list_key_2": ["A", "B", "C"],
    }


class TestRequestGristTable:
    def test_request_grist_table(self):
        """Basic tests of mocked grist responses"""

        api_data_records = [grist_factory.create_record("Apidata")]
        cas_usages_records = grist_factory.create_records("SIMPLIFIONS_cas_usages", 3)

        apidata_results = grist_manager._request_grist_table("Apidata")
        assert apidata_results == api_data_records

        cas_usages_results = grist_manager._request_grist_table(
            "SIMPLIFIONS_cas_usages"
        )
        assert len(cas_usages_results) == 3
        assert cas_usages_results == cas_usages_records

        not_mocked_table_results = grist_manager._request_grist_table(
            "SIMPLIFIONS_produitspublics"
        )
        assert not_mocked_table_results == []

        with pytest.raises(requests_mock.exceptions.NoMockAddress):
            grist_manager._request_grist_table("NonExistentTable")

    def test_request_grist_table_filtering(self, diversified_test_data):
        """Test that mocked data filtered by ID works correctly"""

        # Test filtering by single ID using diversified data
        filtered_results = grist_manager._request_grist_table("Apidata", '{"id": 2}')
        assert len(filtered_results) == 1
        assert filtered_results[0]["Nom_donnees_ou_API"] == "API 2"

        # Test filtering by multiple IDs
        filtered_results = grist_manager._request_grist_table(
            "Apidata", '{"id": [1, 3]}'
        )
        assert len(filtered_results) == 2
        assert filtered_results[0]["Nom_donnees_ou_API"] == "API 1"
        assert filtered_results[1]["Nom_donnees_ou_API"] == "API 3"


class TestGetSubdata:
    """Tests for the _get_subdata method"""

    @pytest.mark.parametrize(
        "args",
        (
            (value, expected)
            for value, expected in [
                (None, None),
                ("", ""),
                ([], []),
            ]
        ),
    )
    def test_with_empty_values(self, args):
        """Test _get_subdata with empty/falsy values"""
        value, expected = args
        table_info = {
            "tag": "test-tag",
            "sub_tables": {
                "API_et_donnees_utiles": "Apidata",
            },
        }

        result = grist_manager._get_subdata("API_et_donnees_utiles", value, table_info)
        assert result == expected

    def test_with_non_sub_table_key(self):
        """Test _get_subdata with key not in sub_tables"""
        table_info = {
            "tag": "test-tag",
            "sub_tables": {
                "API_et_donnees_utiles": "Apidata",
            },
        }

        # Test with a key that's not in sub_tables
        test_value = ["some", "value"]
        result = grist_manager._get_subdata("other_key", test_value, table_info)
        assert result == test_value

    def test_with_single_value(self):
        """Test _get_subdata with single value that maps to sub_table"""
        table_info = {
            "tag": "test-tag",
            "sub_tables": {
                "API_et_donnees_utiles": "Apidata",
            },
        }

        # Clear existing data and create a fresh test record
        grist_factory.clear_table("Apidata")
        grist_factory.create_record(
            "Apidata",
            {"Nom_donnees_ou_API": "Test API", "Visible_sur_simplifions": True},
        )

        # Test with single value (use ID 1 since it's the first record)
        result = grist_manager._get_subdata("API_et_donnees_utiles", 1, table_info)

        # Should return a single record (not a list)
        assert isinstance(result, dict)
        assert result["Nom_donnees_ou_API"] == "Test API"
        assert "Visible_sur_simplifions" in result

    def test_with_list_value(self, diversified_test_data):
        """Test _get_subdata with list value that maps to sub_table"""
        table_info = {
            "tag": "test-tag",
            "sub_tables": {
                "API_et_donnees_utiles": "Apidata",
            },
        }

        # Test with list value using diversified APIs (IDs 1 and 3)
        result = grist_manager._get_subdata("API_et_donnees_utiles", [1, 3], table_info)

        # Should return a list of records
        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0]["Nom_donnees_ou_API"] == "API 1"
        assert result[1]["Nom_donnees_ou_API"] == "API 3"

    def test_visibility_filtering(self, diversified_test_data):
        """Test _get_subdata filters by Visible_sur_simplifions"""
        table_info = {
            "tag": "test-tag",
            "sub_tables": {
                "API_et_donnees_utiles": "Apidata",
            },
        }

        # Add a hidden record to the diversified data
        grist_factory.create_record(
            "Apidata",
            {"Nom_donnees_ou_API": "Hidden API", "Visible_sur_simplifions": False},
        )

        # Test with list containing both visible and hidden records (IDs 1, 2 are visible, 4 is hidden)
        result = grist_manager._get_subdata(
            "API_et_donnees_utiles", [1, 2, 4], table_info
        )

        # Should only return the visible records
        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0]["Nom_donnees_ou_API"] == "API 1"
        assert result[1]["Nom_donnees_ou_API"] == "API 2"

    def test_with_table_info_no_sub_tables(self):
        """Test _get_subdata with table_info that has no sub_tables"""
        table_info = {
            "tag": "test-tag",
            "sub_tables": None,
        }

        test_value = [1, 2, 3]
        result = grist_manager._get_subdata(
            "API_et_donnees_utiles", test_value, table_info
        )
        assert result == test_value


class TestCleanedRowWithSubdata:
    """Tests for the _cleaned_row_with_subdata method"""

    def test_cleans_lists_and_fetches_subdata(self):
        """Test that lists are cleaned ('L' removed) and subdata fetched, with subdata also cleaned"""
        table_info = {
            "tag": "test-tag",
            "sub_tables": {
                "API_et_donnees_utiles": "Apidata",
            },
        }

        # Clear existing data and create test record with 'L' prefixed list in subdata
        grist_factory.clear_table("Apidata")
        grist_factory.create_record(
            "Apidata",
            {
                "Nom_donnees_ou_API": "Test API",
                "Visible_sur_simplifions": True,
                "Cas_d_usages_simplifions": [
                    "L",
                    1,
                    2,
                    3,
                ],  # This should also be cleaned
            },
        )

        # Input row with "L" prefixed lists
        input_row = {
            "title": "Test Title",
            "API_et_donnees_utiles": ["L", 1],  # Should be cleaned and subdata fetched
            "regular_field": [
                "L",
                "value1",
                "value2",
            ],  # Should be cleaned but no subdata
        }

        result = grist_manager._cleaned_row_with_subdata(input_row, table_info)

        # Check that regular lists are cleaned
        assert result["regular_field"] == ["value1", "value2"]

        # Check that subdata was fetched and the subdata lists are also cleaned
        assert isinstance(result["API_et_donnees_utiles"], list)
        assert len(result["API_et_donnees_utiles"]) == 1
        assert result["API_et_donnees_utiles"][0]["Nom_donnees_ou_API"] == "Test API"
        assert result["API_et_donnees_utiles"][0]["Cas_d_usages_simplifions"] == [
            1,
            2,
            3,
        ]  # Cleaned subdata


class TestGetAndFormatGristData:
    """Tests for the get_and_format_grist_data method"""

    def test_fetches_formats_and_groups_diversified_data(self, diversified_test_data):
        """Test that data is fetched, formatted, and grouped correctly with diversified record counts"""
        from unittest.mock import Mock

        ti_mock = Mock()
        grist_manager.get_and_format_grist_data(ti_mock)

        # Verify xcom_push was called with correct structure
        ti_mock.xcom_push.assert_called_once()
        result = ti_mock.xcom_push.call_args[1]["value"]

        # Check basic structure: both tags present with expected counts
        assert len(result["simplifions-cas-d-usages"]) == 2  # 2 cas d'usage records
        assert (
            len(result["simplifions-solutions"]) == 5
        )  # 2 produits + 3 editeurs, hidden one is included

        # Verify ID filtering works: cas-usage-1 references [1, 2] and should get API 1 & API 2
        cas_usage_1 = result["simplifions-cas-d-usages"]["cas-usage-1"]
        assert len(cas_usage_1["API_et_donnees_utiles"]) == 2
        assert cas_usage_1["API_et_donnees_utiles"][0]["Nom_donnees_ou_API"] == "API 1"
        assert cas_usage_1["API_et_donnees_utiles"][1]["Nom_donnees_ou_API"] == "API 2"

        # Verify cas-usage-2 references [3] and should get only API 3 (
        cas_usage_2 = result["simplifions-cas-d-usages"]["cas-usage-2"]
        assert len(cas_usage_2["API_et_donnees_utiles"]) == 1
        assert cas_usage_2["API_et_donnees_utiles"][0]["Nom_donnees_ou_API"] == "API 3"

        # Verify filtering: solution-editeur-2 references [1, 3] and should get API 1 & API 3
        solutions = result["simplifions-solutions"]
        editeur_2 = solutions["solution-editeur-2"]
        assert len(editeur_2["API_et_data_disponibles"]) == 2
        assert editeur_2["API_et_data_disponibles"][0]["Nom_donnees_ou_API"] == "API 1"
        assert editeur_2["API_et_data_disponibles"][1]["Nom_donnees_ou_API"] == "API 3"

        # Verify main table behavior: main records aren't filtered by visibility
        assert "produit-public-2" in solutions

        # Verify subtable visibility filtering: produit-public-1 references [1,2,3,4] but API 4 is hidden
        produit_1 = solutions["produit-public-1"]
        assert [
            api["Nom_donnees_ou_API"] for api in produit_1["API_et_data_disponibles"]
        ] == ["API 1", "API 2", "API 3"]
