import json
import logging
import os
import re
from email.message import Message
from pathlib import Path
from zipfile import ZipFile

import duckdb
import pandas as pd
import py7zr
import requests
from airflow.sdk import task
from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_HOME,
    AIRFLOW_DAG_TMP,
    AIRFLOW_ENV,
    S3_BUCKET_DATA_PIPELINE,
    S3_BUCKET_DATA_PIPELINE_OPEN,
)
from datagouvfr_data_pipelines.utils.datagouv import (
    local_client,
)
from datagouvfr_data_pipelines.utils.download import download_files
from datagouvfr_data_pipelines.utils.filesystem import File
from datagouvfr_data_pipelines.utils.s3 import S3Client
from datagouvfr_data_pipelines.utils.tchap import send_message

DAG_FOLDER = "datagouvfr_data_pipelines/data_processing/"
TMP_FOLDER = f"{AIRFLOW_DAG_TMP}dfi/"
METADATA_FILE = "metadata.json"
with open(f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}dfi/config.json") as fp:
    config = json.load(fp)

CREATE_NATURE_DFI_CODES = """CREATE OR REPLACE TABLE nature_dfi_codes AS
   SELECT data.code_nature_dfi::INTEGER AS code_nature_dfi, data.nom_nature_dfi
   FROM (VALUES (1,'document d’arpentage'),
   (2,'croquis de conservation'),
   (4,'remaniement'),
   (5,'document d’arpentage numérique'),
   (6,'lotissement numérique'),
   (7,'lotissement'),
   (8,'rénovation')) data(code_nature_dfi, nom_nature_dfi);
"""
# EDNC = Extraction_domaine_non_cadastre
# PDP = Passage_domaine_public
CREATE_DFI_TABLE = f"""CREATE OR REPLACE TABLE dfi AS
SELECT CAST(code_insee AS VARCHAR) AS code_insee, CAST(code_dept AS VARCHAR) AS code_departement, code_com AS code_commune,prefixe_section,id_dfi AS identifiant_dfi,nature_dfi AS code_nature_dfi, nom_nature_dfi,
       CAST(CAST(date_valid_dfi AS VARCHAR)[0:4] || '-' || CAST(date_valid_dfi AS VARCHAR)[5:6] || '-' || CAST(date_valid_dfi AS VARCHAR)[7:8] AS DATE) AS date_validation_dfi,
       CASE WHEN p_mere = 'EDNC' THEN p_mere ELSE lpad(p_mere, 6, '0') END AS parcelle_mere,
       CASE WHEN p_fille = 'PDP' THEN p_fille ELSE lpad(p_fille, 6, '0') END AS parcelle_fille
FROM read_csv("{TMP_FOLDER}dfi.csv",
  names = ['code_insee', 'code_dept', 'code_com', 'prefixe_section', 'id_dfi', 'nature_dfi', 'date_valid_dfi', 'p_mere', 'p_fille'],
  types = {{'code_insee': 'VARCHAR', 'code_dept': 'VARCHAR', 'code_com': 'VARCHAR', 'prefixe_section': 'VARCHAR', 'id_dfi': 'VARCHAR', 'nature_dfi': 'BIGINT', 'date_valid_dfi': 'VARCHAR', 'p_mere': 'VARCHAR', 'p_fille': 'VARCHAR'}}
) LEFT JOIN nature_dfi_codes ON code_nature_dfi = nature_dfi
"""

EXPORT_DFI_TO_PARQUET = f"""COPY dfi
TO '{TMP_FOLDER}dfi.parquet'
(FORMAT parquet, COMPRESSION zstd);"""

QUERY_MIN_MAX_DATE = """
SELECT min(date_validation_dfi), max(date_validation_dfi) FROM dfi;
"""

output_duckdb_database_name = f"{TMP_FOLDER}dfi.duckdb"

queries = [
    CREATE_NATURE_DFI_CODES,
    CREATE_DFI_TABLE,
    EXPORT_DFI_TO_PARQUET,
]


# ---------------------------------------------------------------------------
# Archive format history (why both 7z and zip are handled here)
#
# The DGFIP publishes, each quarter, two "volume" files splitting the French
# departments (e.g. `dept 2A0 à dept 49` and `dept 50 à dept 976`). Downstream,
# each volume contains one `.txt` file per department with the DFI rows.
#
# PAST (up to 2025): the downloadable volume was a 7z container, although
# it was misleadingly named with a `.zip` extension. It unpacked to individual
# per-department ZIP files, each containing a `.txt`. The pipeline therefore
# historically did: py7zr (volume) -> ZipFile (per-department) -> .txt rows.
# The DGFIP themselves confirmed this (see discussions on data.gouv.fr:
# "Le premier dossier est compressé en .7z puis les sous dossiers en .zip").
#
# CURRENT (since 2026): the volume is now a plain ZIP whose members
# are the `.txt` files directly, without the inner per-department ZIP layer.
# This is what triggered issue #833: opening a plain ZIP with py7zr raised
# `Bad7zFile: not a 7z file`.
#
# To stay robust against either format, we don't trust the file extension (the
# producer has historically mislabeled archives). We inspect the magic bytes
# and branch accordingly: 7z -> unwrap 7z then its inner ZIPs; zip -> read the
# `.txt` members directly.
#
# CURRENT STATE (checked Nov 2026, actual magic bytes):
#   Jan 2025 (dept 590-976 .zip)  -> ZIP
#   Apr 2025 (dept 590-976 .zip)  -> 7z   (mislabeled .zip)
#   Jul 2025 (dept 2A0-580 .zip)  -> 7z   (mislabeled .zip)
#   Jul 2025 (dept 590-976 .7z)   -> 7z   (correctly named)
#   Jul 2026 (both volumes)       -> ZIP
#   Oct 2026 (both volumes)       -> ZIP
# -> Today's deliveries are plain ZIPs, but the producer is inconsistent:
#    still-reachable older volumes are genuine 7z.
# DECISION: we therefore KEEP the 7z branch even though today's deliveries are
# ZIP, only in case the producer once again publishes 7z in the future (or the
# pipeline reprocesses a historical 7z volume). This is cheap to keep (we just
# read 6 magic bytes). Reassess whether we still need this branch in the future
# if several consecutive quarters are published as plain ZIPs.
# ---------------------------------------------------------------------------
SEVEN_ZIP_MAGIC = b"7z\xbc\xaf'\x1c"


def is_seven_zip(filepath):
    """True when the file is a real 7z archive (checks magic bytes, not the
    `.zip` file extension which the producer has historically mislabeled)."""
    with open(filepath, "rb") as fp:
        return fp.read(6) == SEVEN_ZIP_MAGIC


def _reformat_txt_lines(infile, lines, text):
    cleaned_line = re.sub(r";\n|\n", "", text.replace("\r", ""))
    common = cleaned_line[:30].split(";")
    (
        code_dept,
        code_com,
        prefixe_section,
        id_dfi,
        nature_dfi,
        date_valid_dfi,
    ) = common
    code_dept = code_dept[0:3] if code_dept.startswith("97") else code_dept[0:2]
    code_insee = code_dept[0:2] + code_com
    lines.append(cleaned_line)
    if len(lines) == 2:
        origins = [
            i.strip().zfill(6) for i in lines[0][76:].split(";") if len(i.strip()) > 0
        ]
        destinations = [
            i.strip().zfill(6) for i in lines[1][76:].split(";") if len(i.strip()) > 0
        ]
        if len(origins) == 0:
            origins.append("EDNC")
        if len(destinations) == 0:
            destinations.append("PDP")
        for origin in origins:
            for destination in destinations:
                out = [
                    code_insee,
                    code_dept,
                    code_com,
                    prefixe_section,
                    id_dfi,
                    nature_dfi,
                    date_valid_dfi,
                ] + [origin, destination]
                infile.write(",".join(out) + "\n")
        lines.clear()
    return lines


def _process_txt_files(infile, lines, archive):
    for member in archive.filelist:
        if not member.filename.endswith(".txt"):
            continue
        with archive.open(member) as file:
            for line in file:
                lines = _reformat_txt_lines(infile, lines, line.decode())


def reformat_dfi(filenames, output_name):
    lines = []
    with open(output_name, "w") as infile:
        for filename in filenames:
            logging.info(filename)
            if is_seven_zip(filename):
                # Kept only in case the producer once again publishes 7z (or we
                # reprocess a historical 7z volume); see module comment above.
                # Legacy format: 7z volume whose inner members are per-department
                # ZIP files. Unwrap the 7z, then read each inner ZIP's `.txt`.
                with py7zr.SevenZipFile(filename, mode="r") as archive:
                    for inner_name in archive.getnames():
                        logging.info(inner_name)
                        archive.reset()
                        archive.extract(targets=[inner_name])
                        with ZipFile(inner_name) as zip_archive:
                            _process_txt_files(infile, lines, zip_archive)
                        Path(inner_name).unlink()
            else:
                # Current format (since ~ October 2026): plain ZIP whose members
                # are the `.txt` files directly.
                with ZipFile(filename) as zip_archive:
                    _process_txt_files(infile, lines, zip_archive)


# All French department codes the DFI dataset is expected to cover: the 96
# metropolitan departments (01-95, with 20 split into 2A/2B) plus the 5 DOM
# (971 Guadeloupe, 972 Martinique, 973 Guyane, 974 La Réunion, 976 Mayotte).
EXPECTED_DEPARTMENTS = ({f"{n:02d}" for n in range(1, 96)} - {"20"}) | {
    "2A",
    "2B",
    "971",
    "972",
    "973",
    "974",
    "976",
}


def check_department_coverage(duckdb_con):
    """Assert the combined two shards cover every expected department.

    The producer has a track record of partial/broken deliveries (e.g. July
    2025 missing departments 92/93/94/95/976, and dead attachment links). This
    guards against silently publishing a dataset that is missing whole
    departments. Raises listing the missing departments rather than letting the
    partial data be sent to S3.
    """
    found = {
        row[0]
        for row in duckdb_con.sql(
            "SELECT DISTINCT code_departement FROM dfi"
        ).fetchall()
    }
    missing = sorted(EXPECTED_DEPARTMENTS - found)
    if missing:
        raise RuntimeError(
            f"DFI delivery is incomplete: {len(missing)} department(s) missing "
            f"from the combined shards ({', '.join(missing)}). "
            f"Refusing to publish a partial dataset. "
            f"Actions: (1) contact the DGFIP producer to report the missing "
            f"departments and obtain a corrected delivery; (2) if the omission "
            f"is deliberate on our side, adjust the EXPECTED_DEPARTMENTS constant "
            f"in data_processing/dfi/task_functions.py to match the actual "
            f"expected coverage before re-running."
        )
    logging.info(
        f"Department coverage OK: all {len(EXPECTED_DEPARTMENTS)} expected "
        f"departments present."
    )


def get_real_name_from_content_disposition(url):
    r_head = requests.head(url)
    content_disposition_header = r_head.headers["content-disposition"]
    msg = Message()
    msg["content-disposition"] = content_disposition_header
    return msg.get_filename()


_PERIOD_RE = re.compile(r"situation_(?P<month>[a-z]+)_(?P<year>\d{4})")


def extract_period(resource):
    """Extract the release period `(year, month)` from the resource URL.

    URLs embed the period as `situation_<mois>_<année>` (e.g. `..._situation_
    octobre_2026_dept2a_a_dept49zip`). Returns `(int, str)` — `(year, month)`.
    """
    match = _PERIOD_RE.search(str(resource.get("url") or ""))
    if not match:
        raise RuntimeError(
            f"Could not determine the release period of resource "
            f"{resource.get('id')} ({resource.get('url')}); expected URL to "
            "contain `situation_<mois>_<année>`."
        )
    return (int(match.group("year")), match.group("month").lower())


def assert_same_period(resources):
    """Raise if the resources are not all from the same release period.

    Prevents aggregating data from two different releases (e.g. the first half
    of July mixed with the second half of October): each half individually has
    disjoint departments, so the department-coverage check alone would not
    catch such a mix.
    """
    periods = {extract_period(r) for r in resources}
    if len(periods) != 1:
        raise RuntimeError(
            f"Resources span different release periods ({sorted(periods)}); "
            "refusing to aggregate data from multiple releases. Check the source "
            "dataset and remove stale resources."
        )


def assert_resources_available(resources):
    """Raise if any selected resource does not resolve (HTTP != 200).

    A selected shard that 404s means a release is incomplete: downloading the
    remaining shard(s) alone would produce a partial delivery. Fail loudly so
    the broken resource is investigated instead of silently publishing partial
    data.
    """
    failing = []
    for res in resources:
        try:
            status = requests.head(res.get("url")).status_code
        except requests.RequestException:
            status = None
        if status != 200:
            failing.append((res.get("id"), res.get("url"), status))
    if failing:
        details = "\n".join(
            f"- {rid} ({url}) -> HTTP {status}" for rid, url, status in failing
        )
        raise RuntimeError(
            "Selected DFI resource(s) are not reachable; refusing to publish a "
            "partial or mixed release:\n" + details
        )


def get_download_ressources_infos(urls, destination_dir):
    filenames = []
    for remote_url in reversed(urls):
        zip_file_output_name = get_real_name_from_content_disposition(remote_url)
        filenames.append(
            {
                "url": remote_url,
                "dest_path": destination_dir,
                "dest_name": zip_file_output_name,
            }
        )
    return filenames


def send_metadata_to_s3():
    S3Client(bucket=S3_BUCKET_DATA_PIPELINE).send_file(
        File(
            source_path=TMP_FOLDER,
            source_name=f"{METADATA_FILE}",
            dest_path="dev/dfi/",
            dest_name=f"{METADATA_FILE}",
        ),
        ignore_airflow_env=True,
    )


def check_if_modif():
    s3_process = S3Client(bucket=S3_BUCKET_DATA_PIPELINE)
    dataset_content = local_client.dataset(
        id=config["dfi_info"][AIRFLOW_ENV]["dataset_id"],
    )
    # Take the two most recently modified resources. Refuse to aggregate if
    # they are not from the same release period, to avoid mixing two releases
    # (e.g. the first half of July with the second half of October).
    last_2_files = sorted(
        dataset_content.resources, key=lambda d: d.last_modified or "", reverse=True
    )[:2]
    metadata = [
        {
            "title": resource.title,
            "url": resource.url,
            "id": resource.id,
            "last_modified": resource.last_modified,
        }
        for resource in last_2_files
    ]
    assert_same_period(metadata)
    with open(f"{TMP_FOLDER}{METADATA_FILE}", "w") as infile:
        json.dump(metadata, infile)
    metadata_does_exist = s3_process.does_file_exist_in_bucket("dev/dfi/metadata.json")
    if not metadata_does_exist:
        send_metadata_to_s3()
        return True
    else:
        metadata_content = json.loads(
            s3_process.get_file_content("dev/dfi/metadata.json")
        )
        previous = sorted([i["last_modified"] for i in metadata_content])
        current = sorted([i["last_modified"] for i in metadata])
        if len(set(previous).intersection(current)) != 2:
            send_metadata_to_s3()
            return True
        else:
            return False


@task()
def gather_data():
    logging.info("Getting resources list")
    metadata_content = json.loads(
        S3Client(bucket=S3_BUCKET_DATA_PIPELINE).get_file_content(
            "dev/dfi/metadata.json"
        )
    )
    resources = [
        {"title": i.get("title"), "url": i.get("url"), "id": i.get("id")}
        for i in metadata_content
    ]
    # Refuse to publish a partial or mixed release: the selected resources must
    # all resolve (HTTP 200) and come from the same release period.
    assert_resources_available(resources)
    assert_same_period(resources)
    information_date_about_dataset = re.findall(
        r"\((.*?)\)", metadata_content[0].get("title")
    )
    urls_resources = [r.get("url") for r in resources]
    logging.info("Start downloading DFI files")
    filenames_infos = get_download_ressources_infos(urls_resources, TMP_FOLDER)
    download_files(filenames_infos)
    filenames = [
        f"{filename_info.get('dest_path')}{filename_info.get('dest_name')}"
        for filename_info in filenames_infos
    ]
    logging.info("End downloading DFI files")
    logging.info("Reformat CSV to get a child and parent parcelle for each line")
    reformat_dfi(filenames, f"{TMP_FOLDER}dfi.csv")
    logging.info("Load into Duckdb and export to parquet")
    with duckdb.connect(output_duckdb_database_name) as con:
        for query in queries:
            logging.info(query)
            con.sql(query)
        # Refuse to publish a partial delivery: check the combined two shards
        # cover every expected department before anything is sent to S3.
        check_department_coverage(con)
        min_date, max_date = con.sql(QUERY_MIN_MAX_DATE).fetchone()

    Path(output_duckdb_database_name).unlink()
    Path(f"{TMP_FOLDER}dfi.csv").unlink()

    parquet_dfi = f"{TMP_FOLDER}dfi.parquet"
    logging.info(
        "Convert parquet to get the exact structure beetween parquet and CSV contrary to reformated CSV"
    )
    df = pd.read_parquet(parquet_dfi)
    df.to_csv(parquet_dfi.replace(".parquet", ".csv"), index=False)
    logging.info(f"Convert parquet file {parquet_dfi} to CSV")
    return {
        "min_date": f"{min_date.strftime('%Y-%m-%d')}T00:00:00.000000Z",
        "max_date": f"{max_date.strftime('%Y-%m-%d')}T00:00:00.000000Z",
        "information_date_about_dataset": information_date_about_dataset,
    }


@task()
def send_to_s3():
    logging.info("Start to send files to S3")
    S3Client(bucket=S3_BUCKET_DATA_PIPELINE_OPEN).send_files(
        list_files=[
            File(
                source_path=TMP_FOLDER,
                source_name=f"{filename}",
                dest_path="dfi/",
                dest_name=f"{filename}",
            )
            for filename in [f"dfi.{ext}" for ext in ["csv", "parquet"]]
        ],
        ignore_airflow_env=True,
        is_public=True,
    )
    logging.info("End sending files to S3 Open")


@task()
def publish_on_datagouv(info):
    min_date = info["min_date"]
    max_date = info["max_date"]
    information_date_about_dataset = info["information_date_about_dataset"]
    information_date_about_dataset = (
        f", {information_date_about_dataset[0]}"
        if len(information_date_about_dataset) == 1
        else ""
    )
    for _ext in ["csv", "parquet"]:
        local_client.resource(
            id=config[f"dfi_publi_{_ext}"][AIRFLOW_ENV]["resource_id"],
            dataset_id=config[f"dfi_publi_{_ext}"][AIRFLOW_ENV]["dataset_id"],
            fetch=False,
        ).update(
            payload={
                "url": S3Client(bucket=S3_BUCKET_DATA_PIPELINE_OPEN).get_file_url(
                    f"dfi/dfi.{_ext}"
                ),
                "filesize": os.path.getsize(TMP_FOLDER + f"dfi.{_ext}"),
                "title": (
                    f"Documents de filiation informatisés (DFI) des parcelles{information_date_about_dataset} (format {_ext})"
                ),
                "format": _ext,
                "description": (
                    f"Documents de filiation informatisés (DFI) des parcelles, {information_date_about_dataset} (format {_ext})"
                    " (créé à partir des [fichiers des Documents de filiation informatisés (DFI) des parcelles]"
                    "(https://www.data.gouv.fr/datasets/documents-de-filiation-informatises-dfi-des-parcelles/))"
                ),
            },
        )
    local_client.dataset(
        config["dfi_publi_csv"][AIRFLOW_ENV]["dataset_id"], fetch=False
    ).update(
        payload={
            "temporal_coverage": {
                "start": min_date,
                "end": max_date,
            },
            "tags": [
                "administration",
                "arpentage",
                "cadastre",
                "dfi",
                "filiations",
                "geometres-experts",
                "parcelles",
                "plan-cadastral",
            ],
        },
    )


@task()
def notification():
    dataset_id = config["dfi_publi_csv"][AIRFLOW_ENV]["dataset_id"]
    send_message(
        f"Données DFI agrégées :\n"
        f"\n- uploadées sur S3"
        f"\n- publiées [sur {'demo.' if AIRFLOW_ENV == 'dev' else ''}data.gouv.fr]"
        f"({local_client.base_url}/datasets/{dataset_id}/)"
    )
