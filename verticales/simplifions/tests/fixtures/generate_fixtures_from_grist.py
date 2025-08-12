#!/usr/bin/env python3
"""
Script to generate test fixtures from real Grist data.
This script fetches data from the SIMPLIFIONS_cas_usages table and stores it as JSON fixtures.
"""

import json
import os
import sys
import requests
from pathlib import Path

# Grist configuration (copied from grist_manager.py to make this script standalone)
GRIST_DOC_ID = "c5pt7QVcKWWe"

# Get configuration from environment variables
GRIST_API_URL = os.getenv("GRIST_API_URL", "")
SECRET_GRIST_API_KEY = os.getenv("SECRET_GRIST_API_KEY", "")

# All tables to fetch fixtures for
TABLES_TO_FETCH = [
    "SIMPLIFIONS_cas_usages",
    "SIMPLIFIONS_produitspublics", 
    "SIMPLIFIONS_solutions_editeurs",
    "SIMPLIFIONS_reco_solutions_cas_usages",
    "Apidata",
    "SIMPLIFIONS_description_apidata_cas_usages"
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
    return [row["fields"] for row in r.json()["records"]]


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
            "table_id": table_id,
            "total_records_available": len(raw_data),
            "sample_record": sample_record
        }
        
        # Save to current directory (we're already in the fixtures directory)
        fixtures_dir = Path(__file__).parent
        
        # Convert table name to filename (lowercase and replace underscores)
        filename = table_id.lower().replace("_", "_") + ".json"
        fixture_file = fixtures_dir / filename
        
        with open(fixture_file, 'w', encoding='utf-8') as f:
            json.dump(fixture_data, f, indent=2, ensure_ascii=False)
        
        print(f"Fixture saved to: {fixture_file}")
        print(f"Total records available: {len(raw_data)} (using first record as sample)")
        
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
    
    # Ensure we have the required environment variables
    if not GRIST_API_URL or not SECRET_GRIST_API_KEY:
        print("Error: GRIST_API_URL and SECRET_GRIST_API_KEY must be set")
        print("Please set these environment variables before running this script.")
        return False
    
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
    
    print(f"\n--- Summary ---")
    print(f"Successfully generated fixtures for {success_count}/{len(TABLES_TO_FETCH)} tables")
    
    if failed_tables:
        print(f"Failed tables: {failed_tables}")
        return False
    
    return True


if __name__ == "__main__":
    success = fetch_and_save_all_fixtures()
    if success:
        print("\n✅ Fixture generation completed successfully!")
    else:
        print("\n❌ Fixture generation failed!")
        sys.exit(1)
