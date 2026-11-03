"""Checks of values of the produced files against values read by hand in the original
publications (no Airflow). Called by the check_outputs task, or standalone on a
produced file, from dags/:
python -m datagouvfr_data_pipelines.data_processing.elections.aggregation.tests.check_values general <csv path>
"""

import sys
from decimal import Decimal
from pathlib import Path

import duckdb
import yaml

EXPECTED_VALUES = Path(__file__).resolve().parent / "expected_values.yaml"
TABLES = ["general", "candidats"]
# aggregate functions of the "agregats" cases, on the rows matching the filters
AGGREGATES = {
    "somme": "sum(try_cast({} as double))",
    "moyenne": "avg(try_cast({} as double))",
    "min": "min(try_cast({} as double))",
    "max": "max(try_cast({} as double))",
    "nombre": "count({})",
}


def load_cases(table: str) -> list[dict]:
    cases = yaml.safe_load(EXPECTED_VALUES.read_text(encoding="utf-8")) or []
    for case in cases:
        if (
            case["table"] not in TABLES
            or not case.get("source")
            or "id_election" not in case.get("filtres", {})
            # either the values of one row, or aggregates over the filtered rows
            or ("valeurs" in case) == ("agregats" in case)
            or any(
                function not in AGGREGATES
                for functions in case.get("agregats", {}).values()
                for function in functions
            )
        ):
            raise ValueError(f"invalid case in {EXPECTED_VALUES.name}: {case}")
    return [case for case in cases if case["table"] == table]


def quote(value) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def matches(expected, found: str | None) -> bool:
    if found is None:
        return expected is None
    if isinstance(expected, float):
        # compared at the precision written in the YAML: 98.72 → 2 decimals
        decimals = -Decimal(str(expected)).as_tuple().exponent
        return round(float(found), decimals) == expected
    if isinstance(expected, int) and not isinstance(expected, bool):
        # the aggregates are computed as doubles: 450123.0
        return float(found) == expected
    return found == str(expected)


def check_values(csv_path: str, cases: list[dict]) -> list[str]:
    """Returns the mismatches found, empty if all the expected values are found."""
    if not cases:
        return []
    con = duckdb.connect()
    # one scan of the file (27M rows for candidats), restricted to the checked elections
    elections = ", ".join(quote(case["filtres"]["id_election"]) for case in cases)
    con.execute(
        f"""create table produced as select * from read_csv(
            '{csv_path}', delim=';', all_varchar=true, quote='"', escape='"'
        ) where id_election in ({elections})"""
    )
    columns = [row[0] for row in con.execute("describe produced").fetchall()]
    errors = []
    for case in cases:
        label = ", ".join(f"{column}={value}" for column, value in case["filtres"].items())
        checked = case["valeurs"] if "valeurs" in case else case["agregats"]
        unknown = [c for c in [*case["filtres"], *checked] if c not in columns]
        if unknown:
            errors.append(f"[{label}] unknown columns {unknown}")
            continue
        condition = " and ".join(
            f'"{column}" = {quote(value)}' for column, value in case["filtres"].items()
        )
        if "valeurs" in case:
            # (column, expected value) of the single row
            expected_values = list(case["valeurs"].items())
            selected = ", ".join(f'"{column}"' for column, _ in expected_values)
            rows = con.execute(
                f"select {selected} from produced where {condition}"
            ).fetchall()
            # an ambiguous case must not pass for checked
            if len(rows) != 1:
                errors.append(f"[{label}] {len(rows)} rows instead of 1")
                continue
            found_values = rows[0]
        else:
            # ("somme(inscrits)", expected value) over all the filtered rows
            expected_values = [
                (f"{function}({column})", expected)
                for column, functions in case["agregats"].items()
                for function, expected in functions.items()
            ]
            selected = ", ".join(
                AGGREGATES[function].format(f'"{column}"') + "::varchar"
                for column, functions in case["agregats"].items()
                for function in functions
            )
            count, *found_values = con.execute(
                f"select count(*), {selected} from produced where {condition}"
            ).fetchone()
            if not count:
                errors.append(f"[{label}] no row")
                continue
        for (column, expected), found in zip(expected_values, found_values):
            if not matches(expected, found):
                errors.append(
                    f"[{label}] {column}: {found} instead of {expected}"
                    f" (source: {case['source']})"
                )
    return errors


if __name__ == "__main__":
    table, csv_path = sys.argv[1:]
    cases = load_cases(table)
    errors = check_values(csv_path, cases)
    print("\n".join(errors) or f"{len(cases)} cases checked, no mismatch")
    sys.exit(1 if errors else 0)
