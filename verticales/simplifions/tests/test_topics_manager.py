import sys
from pathlib import Path
from unittest.mock import Mock, patch

# Add the parent directory to the path so we can import the module
sys.path.append(str(Path(__file__).parent.parent))

# Mock the datagouvfr_data_pipelines modules before importing topics_manager
mock_utils = Mock()
mock_utils.get_all_from_api_query = Mock()

with patch.dict(
    "sys.modules",
    {
        "datagouvfr_data_pipelines": Mock(),
        "datagouvfr_data_pipelines.utils": mock_utils,
        "datagouvfr_data_pipelines.utils.datagouv": mock_utils,
    },
):
    from topics_manager import TopicsManager

# Create a mock client for testing
mock_client = Mock()
mock_client.base_url = "https://demo.data.gouv.fr"
mock_client.session.headers = {"Authorization": "Bearer test-key"}

topics_manager = TopicsManager(mock_client)

def test__generated_search_tags():
    topic = {
        "target_users": "particuliers",
        "budget": ["aucun-developpement-ni-budget", "avec-des-moyens-techniques"],
        "other_attribute": "some_value",
    }
    assert topics_manager._generated_search_tags(topic) == ["simplifions-target_users-particuliers", "simplifions-budget-aucun-developpement-ni-budget", "simplifions-budget-avec-des-moyens-techniques"]