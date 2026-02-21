#!/usr/bin/env python3
"""
Script to generate test fixtures from real Grist data.
This script fetches data from the SIMPLIFIONS_cas_usages table and stores it as JSON fixtures.
"""

import json
import os
import sys
from pathlib import Path

import requests

# Grist configuration (copied from grist_manager.py to make this script standalone)
GRIST_DOC_ID = "ofSVjCSAnMb6"

# Get configuration from environment variables
GRIST_API_URL = os.getenv("GRIST_API_URL", "")
SECRET_GRIST_API_KEY = os.getenv("SECRET_GRIST_API_KEY", "")

# All tables to fetch fixtures for
TABLES_TO_FETCH = [
    "Cas_d_usages",
    "Solutions",
    "Fournisseurs_de_services",
    "Types_de_simplification",
    "Usagers",
    "Categories_de_solution",
]


def request_grist_table(table_id: str, filter: str = None) -> list[dict]:
    """Standalone function to request data from Grist table (copied from GristManager)."""
    r = requests.get(
        GRIST_API_URL + f"docs/{GRIST_DOC_ID}/tables/{table_id}/records",
        headers={
            "Authorization": "Bearer " + SECRET_GRIST_API_KEY,
            "Content-Type": "application/json",
            "Accept": "application/json",
        },
        params={"filter": filter} if filter else None,
    )
    r.raise_for_status()
    return [record["fields"] for record in r.json()["records"]]


def fetch_and_save_table_fixture(table_id: str):
    """Fetch data from a specific Grist table and save as fixture."""

    print(f"\n--- Fetching table: {table_id} ---")

    try:
        # Fetch raw data from Grist
        raw_data = request_grist_table(table_id)
        print(f"Successfully fetched {len(raw_data)} records")

        # Get the first record as a sample for factory patterns
        sample_record = raw_data[0] if raw_data else {}

        # Create the fixture data structure with just one sample record
        fixture_data = {
            "resource_name": table_id,
            "total_records_available": len(raw_data),
            "sample_record": sample_record,
        }

        # Save to current directory (we're already in the fixtures directory)
        fixtures_dir = Path(__file__).parent

        # Convert table name to filename (lowercase)
        filename = table_id.lower() + ".json"
        fixture_file = fixtures_dir / filename

        with open(fixture_file, "w", encoding="utf-8") as f:
            json.dump(fixture_data, f, indent=2, ensure_ascii=False)

        print(f"Fixture saved to: {fixture_file}")
        print(
            f"Total records available: {len(raw_data)} (using first record as sample)"
        )

        # Print a sample of the data structure for verification
        if sample_record:
            sample_keys = list(sample_record.keys())[:10]  # Show first 10 keys
            print(f"Sample record keys: {sample_keys}")
            if len(sample_record.keys()) > 10:
                print(f"... and {len(sample_record.keys()) - 10} more keys")

        return True

    except Exception as e:
        print(f"Error fetching data for {table_id}: {e}")
        return False


def fetch_and_save_all_fixtures():
    """Fetch data from all required Grist tables and save as fixtures."""

    # Check that we're in the correct directory
    current_dir = Path.cwd()
    expected_path_suffix = Path("verticales/simplifions/tests/fixtures/grist")
    if not current_dir.parts[-5:] == expected_path_suffix.parts:
        raise RuntimeError(
            "This script must be executed from the verticales/simplifions/tests/fixtures/grist/ directory"
        )

    # Ensure we have the required environment variables
    if not GRIST_API_URL or not SECRET_GRIST_API_KEY:
        raise ValueError(
            "GRIST_API_URL and SECRET_GRIST_API_KEY must be set. Please set these environment variables before running this script."
        )

    print(f"Fetching data from Grist API: {GRIST_API_URL}")
    print(f"Document ID: {GRIST_DOC_ID}")
    print(f"Tables to fetch: {len(TABLES_TO_FETCH)}")

    success_count = 0
    failed_tables = []

    for table_id in TABLES_TO_FETCH:
        success = fetch_and_save_table_fixture(table_id)
        if success:
            success_count += 1
        else:
            failed_tables.append(table_id)

    print("\n--- Summary ---")
    print(
        f"Successfully generated fixtures for {success_count}/{len(TABLES_TO_FETCH)} tables"
    )

    if failed_tables:
        raise RuntimeError(
            f"Failed to generate fixtures for the following tables: {failed_tables}"
        )


if __name__ == "__main__":
    try:
        fetch_and_save_all_fixtures()
        print("\n✅ Fixture generation completed successfully!")
    except Exception as e:
        print(f"\n❌ Fixture generation failed: {e}")
        sys.exit(1)
