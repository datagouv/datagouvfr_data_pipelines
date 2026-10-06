"""Columns and types of the published files, read from their Table Schema
descriptors in schemas/ (also published as documentation resources)."""

import json
from pathlib import Path

SCHEMAS_FOLDER = Path(__file__).resolve().parent / "schemas"
# the results files, aggregated from the sources
SCOPES = ["general", "candidats"]
# all the published tables, each with a schema
TABLES = SCOPES + ["nuances", "communes"]
# Table Schema type -> duckdb type, used for the parquet conversion
DUCKDB_TYPES = {"string": "VARCHAR", "integer": "INT32", "number": "FLOAT"}


def schema_path(table: str) -> Path:
    # the results files vs the mapping tables built on top of them
    kind = "results" if table in SCOPES else "mapping"
    return SCHEMAS_FOLDER / f"schema-{table}-{kind}.json"


def load_schema(table: str) -> dict:
    return json.loads(schema_path(table).read_text(encoding="utf-8"))


dtypes: dict[str, dict[str, str]] = {
    table: {
        field["name"]: DUCKDB_TYPES[field["type"]]
        for field in load_schema(table)["fields"]
    }
    for table in TABLES
}
