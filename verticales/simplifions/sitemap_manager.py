import xml.etree.ElementTree as ET
from datetime import datetime
from urllib.parse import urljoin


class SitemapManager:
    """Manages sitemap XML generation for simplifions.data.gouv.fr"""

    def __init__(self, site_base_url: str, static_pages: list[str]):
        """
        Initialize the sitemap manager.

        Args:
            site_base_url: Base URL of the site (e.g., "https://simplifions.data.gouv.fr")
            static_pages: List of static page paths to include in the sitemap
        """
        self.site_base_url = site_base_url
        self.static_pages = static_pages

    def create_urlset(self) -> ET.Element:
        """Create the root urlset element for the sitemap."""
        return ET.Element("urlset", xmlns="http://www.sitemaps.org/schemas/sitemap/0.9")

    def add_static_pages(
        self, urlset: ET.Element, lastmod_date: str | None = None
    ) -> None:
        """
        Add static pages to the sitemap.

        Args:
            urlset: The root urlset element
            lastmod_date: Date to use for lastmod (YYYY-MM-DD format).
                         If None, uses today's date.
        """
        if lastmod_date is None:
            lastmod_date = datetime.now().strftime("%Y-%m-%d")

        for path in self.static_pages:
            url_elem = ET.SubElement(urlset, "url")
            loc_elem = ET.SubElement(url_elem, "loc")
            loc_elem.text = urljoin(self.site_base_url, path)
            lastmod_elem = ET.SubElement(url_elem, "lastmod")
            lastmod_elem.text = lastmod_date

    def add_topic_pages(
        self,
        urlset: ET.Element,
        topics: list[dict],
        base_path: str,
        modification_dates: dict[int, float],
        tag: str,
    ) -> None:
        """
        Add topic detail pages to the sitemap.

        Args:
            urlset: The root urlset element
            topics: List of topic dictionaries from the API
            base_path: Base path for the topic pages (e.g., "/cas-d-usages" or "/solutions")
            modification_dates: Dictionary mapping grist_id to Modifie_le timestamp
            tag: Tag used to identify the topic type (for extracting grist_id from extras)
        """
        today = datetime.now().strftime("%Y-%m-%d")

        for topic in topics:
            slug = topic.get("slug")
            if not slug:
                continue

            url_elem = ET.SubElement(urlset, "url")
            loc_elem = ET.SubElement(url_elem, "loc")
            loc_elem.text = urljoin(self.site_base_url, f"{base_path}/{slug}")
            lastmod_elem = ET.SubElement(url_elem, "lastmod")

            # Get modification date from Grist
            modifie_le_timestamp = self._get_topic_modification_date(
                topic, tag, modification_dates
            )
            if modifie_le_timestamp:
                try:
                    # Convert timestamp to datetime and format as YYYY-MM-DD
                    # Exceptions can occur if timestamp is invalid (ValueError),
                    # not a number (TypeError), or out of range (OSError)
                    dt = datetime.fromtimestamp(modifie_le_timestamp)
                    lastmod_elem.text = dt.strftime("%Y-%m-%d")
                except (ValueError, TypeError, OSError):
                    # Fallback to today's date if timestamp conversion fails
                    lastmod_elem.text = today
            else:
                # Fallback to topic's last_modified if Grist data not available
                last_modified = topic.get("last_modified")
                if last_modified:
                    try:
                        # Exceptions can occur if format is invalid (ValueError)
                        # or last_modified is not a string (AttributeError)
                        dt = datetime.fromisoformat(
                            last_modified.replace("Z", "+00:00")
                        )
                        lastmod_elem.text = dt.strftime("%Y-%m-%d")
                    except (ValueError, AttributeError):
                        # Fallback to today's date if date parsing fails
                        lastmod_elem.text = today
                else:
                    lastmod_elem.text = today

    def _get_topic_modification_date(
        self, topic: dict, tag: str, modification_dates: dict[int, float]
    ) -> float | None:
        """
        Get the modification date for a topic from Grist data.

        Args:
            topic: Topic dictionary from the API
            tag: Tag used to identify the topic type
            modification_dates: Dictionary mapping grist_id to Modifie_le timestamp

        Returns:
            Modification timestamp or None if not found
        """
        grist_id = self._extract_grist_id(topic, tag)
        if grist_id is None:
            return None

        return modification_dates.get(grist_id)

    def _extract_grist_id(self, topic: dict, tag: str) -> int | None:
        """
        Extract grist_id from topic extras.

        Args:
            topic: Topic dictionary from the API
            tag: Tag used to identify the topic type

        Returns:
            Grist ID as integer or None if not found
        """
        grist_id = None
        if tag in topic.get("extras", {}):
            grist_id = topic["extras"][tag].get("id")
        elif "id" in topic.get("extras", {}):
            grist_id = topic["extras"]["id"]

        if not grist_id:
            return None

        # Convert grist_id to int if needed
        try:
            return int(grist_id)
        except (ValueError, TypeError):
            return None

    def generate_xml(self, urlset: ET.Element) -> str:
        """
        Generate XML string from urlset element.

        Args:
            urlset: The root urlset element

        Returns:
            Formatted XML string with declaration
        """
        ET.indent(urlset, space="  ")
        return ET.tostring(urlset, encoding="unicode", xml_declaration=True)
