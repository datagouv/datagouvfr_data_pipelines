import xml.etree.ElementTree as ET
from unittest.mock import Mock, patch
from datetime import datetime
import time

# The factory must be imported before the task functions because it initializes the mocks
from factories.topics_factory import TopicsFactory
from factories.grist_factory import GristFactory
from task_functions import generate_simplifions_sitemap, STATIC_PAGES, SITE_BASE_URL
from datagouvfr_data_pipelines.utils.datagouv import local_client

topics_factory = TopicsFactory()
grist_factory = GristFactory()


# Helper functions to reduce duplication
def extract_xml_from_logs(mock_logging):
    """Extract XML content from logging calls"""
    for call in mock_logging.info.call_args_list:
        args = call[0]
        if len(args) > 0 and "<?xml" in str(args[0]):
            return args[0]
    return None


def parse_sitemap_xml(xml_content):
    """Parse XML and return root element and list of URL elements"""
    root = ET.fromstring(xml_content)
    urls = root.findall(".//{http://www.sitemaps.org/schemas/sitemap/0.9}url")
    return root, urls


def get_url_locs(urls):
    """Extract location URLs from url elements"""
    return [
        url.find("{http://www.sitemaps.org/schemas/sitemap/0.9}loc").text
        for url in urls
    ]


def get_url_lastmod(urls, loc_pattern):
    """Get lastmod date for a URL matching the pattern"""
    for url in urls:
        loc = url.find("{http://www.sitemaps.org/schemas/sitemap/0.9}loc").text
        if loc_pattern in loc:
            lastmod = url.find("{http://www.sitemaps.org/schemas/sitemap/0.9}lastmod")
            return lastmod.text if lastmod is not None else None
    return None


class TestGenerateSimplifionsSitemap:
    def setup_method(self):
        topics_factory.clear_all_resources()
        grist_factory.clear_all_resources()

    @patch("task_functions.S3Client")
    @patch("task_functions.logging")
    @patch("task_functions.GristV2Manager._request_grist_table")
    def test_generate_sitemap_basic_structure(
        self, mock_request_grist_table, mock_logging, mock_minio_client
    ):
        """Test sitemap generation with basic structure and static pages only"""
        # Mock Grist data to return empty lists
        mock_request_grist_table.return_value = []

        mock_ti = Mock()

        generate_simplifions_sitemap(mock_ti, local_client)

        # Extract and parse XML
        xml_content = extract_xml_from_logs(mock_logging)
        assert xml_content is not None, "XML content should be logged"

        # Verify basic XML structure
        assert "<?xml" in xml_content
        assert 'xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"' in xml_content

        # Parse XML and verify structure
        root, urls = parse_sitemap_xml(xml_content)
        assert root.tag == "{http://www.sitemaps.org/schemas/sitemap/0.9}urlset"
        assert len(urls) == len(STATIC_PAGES)

        # Verify all static pages are present
        url_locs = get_url_locs(urls)
        for page in STATIC_PAGES:
            expected_url = f"{SITE_BASE_URL}{page}"
            assert expected_url in url_locs

        # Verify XCom push
        mock_ti.xcom_push.assert_called()
        push_calls = {
            call[1]["key"]: call[1]["value"]
            for call in mock_ti.xcom_push.call_args_list
        }
        assert push_calls["sitemap_url_count"] == len(STATIC_PAGES)

    @patch("task_functions.S3Client")
    @patch("task_functions.logging")
    @patch("task_functions.GristV2Manager._request_grist_table")
    def test_generate_sitemap_with_cas_usages_and_recommendations(
        self, mock_request_grist_table, mock_logging, mock_minio_client
    ):
        """Test cas d'usages with recommendations - should use MAX date from all sources"""
        # Create test cas d'usage topic
        topics_factory.create_record(
            "simplifions-v2-cas-d-usages",
            {
                "id": "cas-1",
                "slug": "cas-usage-1",
                "extras": {"simplifions-v2-cas-d-usages": {"id": 1}},
            },
        )

        # Mock Grist data
        # Cas d'usage modified on 2024-01-15
        # Recommendation 1 (solution) modified on 2024-01-20 (more recent - should be used)
        # Recommendation 2 (solution) modified on 2024-01-10 (older)
        def mock_grist_table(table_id):
            if table_id == "Cas_d_usages":
                return [
                    {
                        "id": 1,
                        "fields": {
                            "Modifie_le": time.mktime(
                                datetime(2024, 1, 15).timetuple()
                            ),
                            "Recommandations": ["L", 1, 2],  # With "L" prefix
                        },
                    },
                ]
            elif table_id == "Solutions":
                return []
            elif table_id == "Recommandations":
                return [
                    {
                        "id": 1,
                        "fields": {
                            "Modifie_le": time.mktime(datetime(2024, 1, 20).timetuple())
                        },
                    },
                    {
                        "id": 2,
                        "fields": {
                            "Modifie_le": time.mktime(datetime(2024, 1, 10).timetuple())
                        },
                    },
                ]
            return []

        mock_request_grist_table.side_effect = mock_grist_table

        mock_ti = Mock()

        generate_simplifions_sitemap(mock_ti, local_client)

        # Extract and parse XML
        xml_content = extract_xml_from_logs(mock_logging)
        assert xml_content is not None, "XML content should be logged"

        root, urls = parse_sitemap_xml(xml_content)

        # Verify cas d'usage URL is present
        url_locs = get_url_locs(urls)
        assert f"{SITE_BASE_URL}/cas-d-usages/cas-usage-1" in url_locs

        # Verify it uses the MAX date (from recommendation 1: 2024-01-20)
        lastmod = get_url_lastmod(urls, "cas-usage-1")
        assert lastmod == "2024-01-20", (
            f"Should use max date from cas d'usage and recommendations. "
            f"Expected 2024-01-20, got {lastmod}"
        )

        # Verify URL count
        push_calls = {
            call[1]["key"]: call[1]["value"]
            for call in mock_ti.xcom_push.call_args_list
        }
        assert push_calls["sitemap_url_count"] == len(STATIC_PAGES) + 1

    @patch("task_functions.S3Client")
    @patch("task_functions.logging")
    @patch("task_functions.GristV2Manager._request_grist_table")
    def test_generate_sitemap_with_solutions(
        self, mock_request_grist_table, mock_logging, mock_minio_client
    ):
        """Test sitemap generation with solutions topics"""
        # Create test topics
        topics_factory.create_record(
            "simplifions-v2-solutions",
            {
                "id": "solution-1",
                "slug": "solution-1",
                "last_modified": "2024-02-01T08:00:00Z",
                "extras": {"simplifions-v2-solutions": {"id": 1}},
            },
        )
        topics_factory.create_record(
            "simplifions-v2-solutions",
            {
                "id": "solution-2",
                "slug": "solution-2",
                "extras": {"simplifions-v2-solutions": {"id": 2}},
            },
        )

        # Mock Grist data
        def mock_grist_table(table_id):
            if table_id == "Cas_d_usages":
                return []
            elif table_id == "Solutions":
                return [
                    {
                        "id": 1,
                        "fields": {
                            "Modifie_le": time.mktime(datetime(2024, 2, 1).timetuple())
                        },
                    },
                    {
                        "id": 2,
                        "fields": {
                            "Modifie_le": time.mktime(datetime.now().timetuple())
                        },
                    },
                ]
            elif table_id == "Recommandations":
                return []
            return []

        mock_request_grist_table.side_effect = mock_grist_table

        mock_ti = Mock()

        generate_simplifions_sitemap(mock_ti, local_client)

        # Extract and parse XML
        xml_content = extract_xml_from_logs(mock_logging)
        assert xml_content is not None, "XML content should be logged"

        root, urls = parse_sitemap_xml(xml_content)

        # Should have static pages + 2 solutions
        assert len(urls) == len(STATIC_PAGES) + 2

        # Verify solutions URLs are present
        url_locs = get_url_locs(urls)
        assert f"{SITE_BASE_URL}/solutions/solution-1" in url_locs
        assert f"{SITE_BASE_URL}/solutions/solution-2" in url_locs

        # Verify lastmod dates
        lastmod1 = get_url_lastmod(urls, "solution-1")
        assert lastmod1 == "2024-02-01"

        lastmod2 = get_url_lastmod(urls, "solution-2")
        # Should use Grist date (today)
        assert len(lastmod2) == 10  # YYYY-MM-DD format
        assert lastmod2.count("-") == 2

        # Verify URL count
        push_calls = {
            call[1]["key"]: call[1]["value"]
            for call in mock_ti.xcom_push.call_args_list
        }
        assert push_calls["sitemap_url_count"] == len(STATIC_PAGES) + 2

    @patch("task_functions.S3Client")
    @patch("task_functions.logging")
    @patch("task_functions.GristV2Manager._request_grist_table")
    def test_generate_sitemap_date_fallback(
        self, mock_request_grist_table, mock_logging, mock_minio_client
    ):
        """Test date fallback logic: Grist → topic.last_modified"""
        # Create test topics with different date scenarios
        # Scenario 1: Has Grist date (should use Grist date, ignore topic.last_modified)
        topics_factory.create_record(
            "simplifions-v2-cas-d-usages",
            {
                "id": "cas-1",
                "slug": "has-grist-date",
                "last_modified": "2024-01-10T10:00:00Z",  # Should be ignored
                "extras": {"simplifions-v2-cas-d-usages": {"id": 1}},
            },
        )
        # Scenario 2: No Grist date but has last_modified (should use last_modified)
        topics_factory.create_record(
            "simplifions-v2-cas-d-usages",
            {
                "id": "cas-2",
                "slug": "has-last-modified",
                "last_modified": "2024-02-15T10:00:00Z",
                "extras": {"simplifions-v2-cas-d-usages": {"id": 2}},
            },
        )

        # Mock Grist data
        def mock_grist_table(table_id):
            if table_id == "Cas_d_usages":
                return [
                    {
                        "id": 1,
                        "fields": {
                            "Modifie_le": time.mktime(datetime(2024, 1, 25).timetuple())
                        },
                    },
                    {"id": 2, "fields": {}},  # No Modifie_le field
                ]
            elif table_id == "Solutions":
                return []
            elif table_id == "Recommandations":
                return []
            return []

        mock_request_grist_table.side_effect = mock_grist_table

        mock_ti = Mock()

        generate_simplifions_sitemap(mock_ti, local_client)

        # Extract and parse XML
        xml_content = extract_xml_from_logs(mock_logging)
        assert xml_content is not None, "XML content should be logged"

        root, urls = parse_sitemap_xml(xml_content)

        # Scenario 1: Uses Grist date (2024-01-25), not topic.last_modified
        lastmod1 = get_url_lastmod(urls, "has-grist-date")
        assert lastmod1 == "2024-01-25", "Should use Grist date when available"

        # Scenario 2: Uses topic.last_modified (2024-02-15) when no Grist date
        lastmod2 = get_url_lastmod(urls, "has-last-modified")
        assert lastmod2 == "2024-02-15", (
            "Should fallback to topic.last_modified when no Grist date"
        )

        # Verify URL count
        push_calls = {
            call[1]["key"]: call[1]["value"]
            for call in mock_ti.xcom_push.call_args_list
        }
        assert push_calls["sitemap_url_count"] == len(STATIC_PAGES) + 2
