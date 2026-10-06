"""Checks of the produced files against their Table Schema (no Airflow, called by
the check_outputs task)."""

import duckdb

INTEGER = r"-?[0-9]+"


def check_file(csv_path: str, schema: dict) -> list[str]:
    """Returns the errors found, empty if the file matches its schema."""
    relation = duckdb.read_csv(
        csv_path, delimiter=";", all_varchar=True, quotechar='"', escapechar='"'
    )
    expected = [field["name"] for field in schema["fields"]]
    if relation.columns != expected:
        return [f"columns {relation.columns} instead of {expected}"]
    errors = []
    for field in schema["fields"]:
        column = f'"{field["name"]}"'
        rules = []
        if field["type"] == "integer":
            rules.append(
                ("not an integer", f"not regexp_full_match({column}, '{INTEGER}')")
            )
        elif field["type"] == "number":
            rules.append(("not a number", f"try_cast({column} as double) is null"))
        if "enum" in field.get("constraints", {}):
            values = ", ".join(
                "'" + value.replace("'", "''") + "'"
                for value in field["constraints"]["enum"]
            )
            rules.append(("not in the allowed values", f"{column} not in ({values})"))
        for label, condition in rules:
            # empty values are allowed (missingValues), duckdb reads them as NULL
            count, examples = (
                relation.filter(f"{column} is not null and ({condition})")
                .aggregate(f"count(*), list_slice(list(distinct {column}), 1, 5)")
                .fetchone()
            )
            if count:
                errors.append(
                    f"{field['name']}: {count} values {label}, e.g. {examples}"
                )
    return errors
