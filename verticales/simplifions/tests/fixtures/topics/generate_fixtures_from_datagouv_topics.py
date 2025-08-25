#!/usr/bin/env python3
"""
Script to generate test fixtures from real data.gouv.fr topics.
This script fetches public data from the simplifions topics and stores them as JSON fixtures.

No API key required - only accesses publicly available topic data.
"""

import json
import sys
import requests
from pathlib import Path

# Use demo by default for development (no API key needed for public data)
BASE_URL = "https://demo.data.gouv.fr"

# Tags to fetch fixtures for
TAGS_TO_FETCH = [
    "simplifions-cas-d-usages",
    "simplifions-solutions",
]


def get_one_topic_for_tag(tag: str) -> dict:
    """Standalone function to get API response for topics with a specific tag (public data only)."""
    url = f"{BASE_URL}/api/1/topics/?tag={tag}&page_size=1"

    r = requests.get(url)
    r.raise_for_status()
    data = r.json()

    if not data["data"]:
        raise ValueError(f"No topics found for tag: {tag}")

    return data


def fetch_and_save_tag_fixture(tag: str):
    """Fetch one topic example with a specific tag and save as fixture."""

    print(f"\n--- Fetching one topic example with tag: {tag} ---")

    try:
        # Fetch API response from data.gouv.fr API
        api_data = get_one_topic_for_tag(tag)

        # Extract the first record and total from the API response
        sample_record = api_data["data"][0]
        total_available = api_data["total"]

        print(f"Successfully fetched topic example: {sample_record.get('name', 'N/A')}")

        # Create the fixture data structure with the sample record
        fixture_data = {
            "resource_name": tag,
            "total_records_available": total_available,
            "sample_record": sample_record,
        }

        # Save to current directory (we're already in the fixtures directory)
        fixtures_dir = Path(__file__).parent

        # Convert tag name to filename (replace hyphens with underscores)
        filename = tag.replace("-", "_") + ".json"
        fixture_file = fixtures_dir / filename

        with open(fixture_file, "w", encoding="utf-8") as f:
            json.dump(fixture_data, f, indent=2, ensure_ascii=False)

        print(f"Fixture saved to: {fixture_file}")

        return True

    except Exception as e:
        print(f"Error fetching data for tag {tag}: {e}")
        return False


def fetch_and_save_all_fixtures():
    """Fetch data from all required topic tags and save as fixtures."""

    # Check that we're in the correct directory
    current_dir = Path.cwd()
    expected_path_suffix = Path("verticales/simplifions/tests/fixtures/topics")
    if not current_dir.parts[-5:] == expected_path_suffix.parts:
        raise RuntimeError(
            "This script must be executed from the verticales/simplifions/tests/fixtures/topics/ directory"
        )

    success_count = 0
    failed_tags = []

    for tag in TAGS_TO_FETCH:
        success = fetch_and_save_tag_fixture(tag)
        if success:
            success_count += 1
        else:
            failed_tags.append(tag)

    if failed_tags:
        raise RuntimeError(
            f"Failed to generate fixtures for the following tags: {failed_tags}"
        )


if __name__ == "__main__":
    try:
        fetch_and_save_all_fixtures()
        print("\n✅ Fixture generation completed successfully!")
    except Exception as e:
        print(f"\n❌ Fixture generation failed: {e}")
        sys.exit(1)
