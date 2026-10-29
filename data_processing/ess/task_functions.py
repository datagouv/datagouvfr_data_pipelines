import csv
import json
import logging
import re
from collections.abc import Iterator
from urllib.parse import urljoin

import openpyxl
import requests
from airflow.sdk import task
from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_HOME,
    AIRFLOW_DAG_TMP,
    AIRFLOW_ENV,
)
from datagouvfr_data_pipelines.utils.datagouv import local_client

DAG_FOLDER = "datagouvfr_data_pipelines/data_processing/"
TMP_FOLDER = f"{AIRFLOW_DAG_TMP}ess/"
OUTPUT_FILE = "entreprises_ess.csv"
with open(f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}ess/config.json") as fp:
    config = json.load(fp)

# Same columns and order as the CSV previously published by ESS France
# https://www.data.gouv.fr/datasets/liste-des-entreprises-de-less
COLUMNS = [
    "SIREN",
    "Nom ou raison sociale de l'entreprise",
    "Famille juridique de l'entreprise",
    "Code postal",
    "Libellé de la commune de l'établissement",
    "Code du département de l'établissement",
    "Département de l'établissement",
    "Région de l'établissement",
]
POSTAL_CODE = COLUMNS.index("Code postal")
DEPARTEMENT_CODE = COLUMNS.index("Code du département de l'établissement")


def get_source_urls(html: str) -> list[str]:
    """The data is split in several regional Excel files"""
    urls = sorted(
        {
            urljoin(config["source_page"], href)
            for href in re.findall(r'href="([^"]+\.xlsx)"', html)
        }
    )
    if not urls:
        raise ValueError(f"No Excel file found on {config['source_page']}")
    return urls


def get_files_updated_date(urls: list[str]) -> str:
    """
    Return the most recent uploaded dates.
    Dates are extracted from the file names, e.g. /system/files/2026-06/Liste1CvlNorHdfPdl_0.xlsx
    """
    periods = []
    for url in urls:
        match = re.search(r"/(\d{4}-\d{2})/[^/]+$", url)
        if not match:
            raise ValueError(f"No year and month found in {url}")
        periods.append(match.group(1))
    return max(periods)


def normalize_postal_code(code: str) -> str:
    """
    Remove decimal, add trailing zeros and remove the trailing zero for non-diffusibles.
    "3000.0" -> "03000"
    "0[ND]" -> "[ND]"
    """
    code = re.sub(r"\.0$", "", code.strip())
    if "[ND]" in code:
        return "[ND]"
    return code.zfill(5) if code.isdigit() else code


def normalize_departement_code(code: str) -> str:
    """Source files mix "2", "02", "044" and "02A" for metropolitan departements."""
    code = code.strip()
    if len(code) == 3 and code.startswith("0"):
        code = code[1:]
    return code.zfill(2)


def convert_cell_to_str(value) -> str:
    """Convert any cell to a string, empty cells included."""
    if value is None:
        return ""
    return str(value)


def read_source_file(path: str) -> Iterator[list[str]]:
    """
    Yield the rows of the first sheet, in the order of COLUMNS.
    The sheet is streamed so that the whole file is never loaded in memory.
    """
    wb = openpyxl.load_workbook(path, read_only=True)
    try:
        rows = wb.worksheets[0].iter_rows(values_only=True)
        header = [convert_cell_to_str(cell).strip() for cell in next(rows)]
        header = ["SIREN" if cell == "N°SIREN" else cell for cell in header]
        if set(header) != set(COLUMNS):
            raise ValueError(
                f"Unexpected columns in {path}: {header} (expected: {COLUMNS})"
            )
        # Columns in input are not in the same position as the expected output
        columns_positions = [header.index(cell) for cell in COLUMNS]
        for row in rows:
            values = [
                convert_cell_to_str(row[position]) for position in columns_positions
            ]
            if not any(values):
                continue
            values[POSTAL_CODE] = normalize_postal_code(values[POSTAL_CODE])
            values[DEPARTEMENT_CODE] = normalize_departement_code(
                values[DEPARTEMENT_CODE]
            )
            yield values
    finally:
        wb.close()


@task()
def gather_data(**context):
    r = requests.get(config["source_page"])
    r.raise_for_status()
    urls = get_source_urls(r.text)
    total = 0
    with open(TMP_FOLDER + OUTPUT_FILE, "w", encoding="utf8", newline="") as ess_csv:
        writer = csv.writer(ess_csv, lineterminator="\n")
        writer.writerow(COLUMNS)
        for url in urls:
            logging.info(f"Processing {url}")
            r = requests.get(url)
            r.raise_for_status()
            path = TMP_FOLDER + url.split("/")[-1]
            with open(path, "wb") as f:
                f.write(r.content)
            count = 0
            for row in read_source_file(path):
                writer.writerow(row)
                count += 1
            logging.info(f"> {count} rows processed")
            total += count
    logging.info(f"Total: {total} rows processed")
    context["ti"].xcom_push(key="period", value=get_files_updated_date(urls))


@task()
def publish_on_datagouv(**context):
    """Updating the existing resource. The file is ~120 Mo."""
    period = context["ti"].xcom_pull(key="period", task_ids="gather_data")
    local_client.resource(
        id=config["csv"][AIRFLOW_ENV]["resource_id"],
        dataset_id=config["csv"][AIRFLOW_ENV]["dataset_id"],
    ).update(
        file_to_upload=TMP_FOLDER + OUTPUT_FILE,
        timeout=600,
        payload={
            "title": f"entreprises_ess_{period}.csv",
            "description": (
                f"Liste des entreprises de l'ESS ({period}), consolidée à partir des "
                f"[fichiers régionaux publiés par ESS France]({config['source_page']})."
            ),
        },
    )
