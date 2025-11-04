import os
import re
import json
import logging
from email.message import Message
from zipfile import ZipFile
from pathlib import Path

import pandas as pd
import requests
import py7zr
import duckdb

from datagouvfr_data_pipelines.config import (
    AIRFLOW_ENV,
    AIRFLOW_DAG_HOME,
    AIRFLOW_DAG_TMP,
    MINIO_BUCKET_DATA_PIPELINE_OPEN,
    MINIO_BUCKET_DATA_PIPELINE,
)
from datagouvfr_data_pipelines.utils.filesystem import File
from datagouvfr_data_pipelines.utils.download import download_files
from datagouvfr_data_pipelines.utils.minio import MinIOClient
from datagouvfr_data_pipelines.utils.datagouv import (
    local_client,
)
from datagouvfr_data_pipelines.utils.mattermost import send_message

DAG_FOLDER = "datagouvfr_data_pipelines/data_processing/"
DATADIR = f"{AIRFLOW_DAG_TMP}dfi"
METADATA_FILE = "metadata.json"
minio_open = MinIOClient(bucket=MINIO_BUCKET_DATA_PIPELINE_OPEN)
minio_process = MinIOClient(bucket=MINIO_BUCKET_DATA_PIPELINE)
with open(f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}dfi/config/dgv.json") as fp:
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
FROM read_csv("{DATADIR}/dfi.csv",
  names = ['code_insee', 'code_dept', 'code_com', 'prefixe_section', 'id_dfi', 'nature_dfi', 'date_valid_dfi', 'p_mere', 'p_fille'],
  types = {{'code_insee': 'VARCHAR', 'code_dept': 'VARCHAR', 'code_com': 'VARCHAR', 'prefixe_section': 'VARCHAR', 'id_dfi': 'VARCHAR', 'nature_dfi': 'BIGINT', 'date_valid_dfi': 'VARCHAR', 'p_mere': 'VARCHAR', 'p_fille': 'VARCHAR'}}
) LEFT JOIN nature_dfi_codes ON code_nature_dfi = nature_dfi
"""

EXPORT_DFI_TO_PARQUET = f"""COPY dfi
TO '{DATADIR}/dfi.parquet'
(FORMAT parquet, COMPRESSION zstd);"""

QUERY_MIN_MAX_DATE = """
SELECT min(date_validation_dfi), max(date_validation_dfi) FROM dfi;
"""

output_duckdb_database_name = f"{DATADIR}/dfi.duckdb"

queries = [
    CREATE_NATURE_DFI_CODES,
    CREATE_DFI_TABLE,
    EXPORT_DFI_TO_PARQUET,
]


def reformat_dfi(filenames, output_name):
    lines = []
    with open(output_name, "w") as infile:
        for filename in filenames:
            logging.info(filename)
            with py7zr.SevenZipFile(filename, mode="r") as archive:
                for file in archive.getnames():
                    logging.info(file)
                    archive.reset()
                    archive.extract(targets=[file])
                    with ZipFile(file) as zip_archive:
                        for zip in zip_archive.filelist:
                            for line in zip_archive.open(zip):
                                # logging.info(line.decode())
                                cleaned_line = re.sub(
                                    r";\n|\n", "", line.decode().replace("\r", "")
                                )
                                common = cleaned_line[:30].split(";")
                                (
                                    code_dept,
                                    code_com,
                                    prefixe_section,
                                    id_dfi,
                                    nature_dfi,
                                    date_valid_dfi,
                                ) = common
                                code_dept = (
                                    code_dept[0:3]
                                    if code_dept.startswith("97")
                                    else code_dept[0:2]
                                )
                                code_insee = code_dept[0:2] + code_com
                                lines.append(cleaned_line)
                                if len(lines) == 2:
                                    origins = [
                                        i.strip().zfill(6)
                                        for i in lines[0][76:].split(";")
                                        if len(i.strip()) > 0
                                    ]
                                    destinations = [
                                        i.strip().zfill(6)
                                        for i in lines[1][76:].split(";")
                                        if len(i.strip()) > 0
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
                                    lines = []
                    Path(file).unlink()


def get_real_name_from_content_disposition(url):
    r_head = requests.head(url)
    content_disposition_header = r_head.headers["content-disposition"]
    msg = Message()
    msg["content-disposition"] = content_disposition_header
    return msg.get_filename()


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


def send_metadata_to_minio():
    minio_process.send_file(
        File(
            source_path=f"{DATADIR}/",
            source_name=f"{METADATA_FILE}",
            dest_path="dev/dfi/",
            dest_name=f"{METADATA_FILE}",
        ),
        ignore_airflow_env=True,
    )


def check_if_modif():
    dataset_content = local_client.dataset(
        id=config["dfi_info"][AIRFLOW_ENV]["dataset_id"],
    )
    last_2_files = sorted(dataset_content.resources, key=lambda d: d.last_modified)[-2:]
    metadata = [
        {
            "title": resource.title,
            "url": resource.url,
            "id": resource.id,
            "last_modified": resource.last_modified,
        }
        for resource in last_2_files
    ]
    with open(f"{DATADIR}/{METADATA_FILE}", "w") as infile:
        json.dump(metadata, infile)
    metadata_does_exist = minio_process.does_file_exist_on_minio(
        "dev/dfi/metadata.json"
    )
    if not metadata_does_exist:
        send_metadata_to_minio()
        return True
    else:
        metadata_content = json.loads(
            minio_process.get_file_content("dev/dfi/metadata.json")
        )
        previous = sorted([i.get("last_modified") for i in metadata_content])
        current = sorted([i.get("last_modified") for i in metadata])
        if len(set(previous).intersection(current)) != 2:
            send_metadata_to_minio()
            return True
        else:
            return False


def gather_data(ti):
    logging.info("Getting resources list")
    metadata_content = json.loads(
        minio_process.get_file_content("dev/dfi/metadata.json")
    )
    urls_resources = [i.get("url") for i in metadata_content]
    information_date_about_dataset = re.findall(
        r"\((.*?)\)", metadata_content[0].get("title")
    )
    logging.info("Start downloading DFI files")
    filenames_infos = get_download_ressources_infos(urls_resources, DATADIR)
    download_files(filenames_infos)
    filenames = [
        f"{filename_info.get('dest_path')}/{filename_info.get('dest_name')}"
        for filename_info in filenames_infos
    ]
    logging.info("End downloading DFI files")
    logging.info("Reformat CSV to get a child and parent parcelle for each line")
    reformat_dfi(filenames, f"{DATADIR}/dfi.csv")
    logging.info("Load into Duckdb and export to parquet")
    with duckdb.connect(output_duckdb_database_name) as con:
        for query in queries:
            logging.info(query)
            con.sql(query)
        min_date, max_date = con.sql(QUERY_MIN_MAX_DATE).fetchone()

    Path(output_duckdb_database_name).unlink()
    Path(f"{DATADIR}/dfi.csv").unlink()

    parquet_dfi = f"{DATADIR}/dfi.parquet"
    logging.info(
        "Convert parquet to get the exact structure beetween parquet and CSV contrary to reformated CSV"
    )
    df = pd.read_parquet(parquet_dfi)
    df.to_csv(parquet_dfi.replace(".parquet", ".csv"), index=False)
    logging.info(f"Convert parquet file {parquet_dfi} to CSV")
    ti.xcom_push(
        key="min_date", value=f"{min_date.strftime('%Y-%m-%d')}T00:00:00.000000Z"
    )
    ti.xcom_push(
        key="max_date", value=f"{max_date.strftime('%Y-%m-%d')}T00:00:00.000000Z"
    )
    ti.xcom_push(
        key="information_date_about_dataset", value=information_date_about_dataset
    )


def send_to_minio():
    logging.info("Start to send files to Minio")
    exts = ["csv", "parquet"]
    fileslist = [
        File(
            source_path=f"{DATADIR}/",
            source_name=f"{filename}",
            dest_path="dfi/",
            dest_name=f"{filename}",
        )
        for filename in [f"dfi.{ext}" for ext in exts]
    ]
    minio_open.send_files(
        list_files=fileslist,
        ignore_airflow_env=True,
    )
    logging.info("End sending files to Minio Open")


def publish_on_datagouv(ti):
    min_date = ti.xcom_pull(key="min_date", task_ids="gather_data")
    max_date = ti.xcom_pull(key="max_date", task_ids="gather_data")
    information_date_about_dataset = ti.xcom_pull(
        key="information_date_about_dataset", task_ids="gather_data"
    )
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
                "url": (
                    f"https://object.files.data.gouv.fr/{MINIO_BUCKET_DATA_PIPELINE_OPEN}"
                    f"/dfi/dfi.{_ext}"
                ),
                "filesize": os.path.getsize(DATADIR + f"/dfi.{_ext}"),
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


def notification_mattermost():
    dataset_id = config["dfi_publi_csv"][AIRFLOW_ENV]["dataset_id"]
    send_message(
        f"Données DFI agrégées :"
        f"\n- uploadées sur Minio"
        f"\n- publiées [sur {'demo.' if AIRFLOW_ENV == 'dev' else ''}data.gouv.fr]"
        f"({local_client.base_url}/datasets/{dataset_id}/)"
    )
