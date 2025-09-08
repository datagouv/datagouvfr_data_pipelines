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
from datagouvfr_data_pipelines.utils.minio import MinIOClient
from datagouvfr_data_pipelines.utils.datagouv import (
    local_client,
)

DAG_FOLDER = "datagouvfr_data_pipelines/data_processing/"
DATADIR = f"{AIRFLOW_DAG_TMP}dfi"
METADATA_FILE = "metadata.json"
minio_open = MinIOClient(bucket=MINIO_BUCKET_DATA_PIPELINE_OPEN)
minio_process = MinIOClient(bucket=MINIO_BUCKET_DATA_PIPELINE)
with open(f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}dfi/config/dgv.json") as fp:
    config = json.load(fp)

# EDNC = Extraction_domaine_non_cadastre
# PDP = Passage_domaine_public
CREATE_DFI_TABLE = f"""CREATE OR REPLACE TABLE dfi AS
SELECT CAST(code_insee AS VARCHAR) AS code_insee, CAST(code_dept AS VARCHAR) AS code_dept, code_com,prefixe_section,id_dfi,nature_dfi,
       CAST(CAST(date_valid_dfi AS VARCHAR)[0:4] || '-' || CAST(date_valid_dfi AS VARCHAR)[5:6] || '-' || CAST(date_valid_dfi AS VARCHAR)[7:8] AS DATE) AS date_valid_dfi,
       CASE WHEN p_mere = 'EDNC' THEN p_mere ELSE CAST(code_insee AS VARCHAR) || prefixe_section || lpad(p_mere, 6, '0') END AS p_mere,
       CASE WHEN p_fille = 'PDP' THEN p_fille ELSE CAST(code_insee AS VARCHAR) || prefixe_section || lpad(p_fille, 6, '0') END AS p_fille
FROM read_csv("{DATADIR}/dfi.csv", names = ['code_insee', 'code_dept', 'code_com', 'prefixe_section', 'id_dfi', 'nature_dfi', 'date_valid_dfi', 'p_mere', 'p_fille'], types = {{'code_insee': 'VARCHAR', 'code_dept': 'VARCHAR', 'code_com': 'VARCHAR', 'prefixe_section': 'VARCHAR', 'id_dfi': 'VARCHAR', 'nature_dfi': 'BIGINT', 'date_valid_dfi': 'VARCHAR', 'p_mere': 'VARCHAR', 'p_fille': 'VARCHAR'}})
"""
# Need duckdb 1.3.3 or later or may got https://github.com/duckdb/duckdb/issues/18190 when running below query
CREATE_CODE_INSEE_IDX = "CREATE INDEX code_insee_idx ON dfi(code_insee);"
CREATE_CODE_DEPT_IDX = "CREATE INDEX code_dept_idx ON dfi(code_dept);"
CREATE_DATE_VALID_DFI_IDX = "CREATE INDEX date_valid_dfi_idx ON dfi(date_valid_dfi);"
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
EXPORT_DFI_TO_PARQUET = f"""COPY dfi
TO '{DATADIR}/dfi.parquet'
(FORMAT parquet, COMPRESSION zstd);"""
EXPORT_NATURE_DFI_CODES_TO_PARQUET = f"""COPY nature_dfi_codes
TO '{DATADIR}/nature_dfi_codes.parquet'
(FORMAT parquet, COMPRESSION zstd);"""

output_duckdb_database_name = f"{DATADIR}/dfi.duckdb"

queries = [
    CREATE_DFI_TABLE,
    CREATE_CODE_INSEE_IDX,
    CREATE_CODE_DEPT_IDX,
    CREATE_DATE_VALID_DFI_IDX,
    CREATE_NATURE_DFI_CODES,
    EXPORT_DFI_TO_PARQUET,
    EXPORT_NATURE_DFI_CODES_TO_PARQUET,
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


def download_as_stream(remote_url, output_name):
    with requests.get(remote_url, stream=True) as response:
        with open(output_name, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)


def get_real_name_from_content_disposition(url):
    r_head = requests.head(url)
    content_disposition_header = r_head.headers["content-disposition"]
    msg = Message()
    msg["content-disposition"] = content_disposition_header
    return msg.get_filename()


def download_ressources(urls, destination_dir):
    filenames = []
    for remote_url in reversed(urls):
        zip_file_output_name = get_real_name_from_content_disposition(remote_url)
        filenames.append(f"{destination_dir}/{zip_file_output_name}")
        if not Path(zip_file_output_name).is_file():
            download_as_stream(remote_url, f"{destination_dir}/{zip_file_output_name}")
        else:
            print(f"{destination_dir}/{zip_file_output_name}" + " already exists!")
    return filenames


def send_metadata_to_minio():
    minio_process.send_files(
        list_files=[
            File(
                source_path=f"{DATADIR}/",
                source_name=f"{METADATA_FILE}",
                dest_path="dev/dfi/",
                dest_name=f"{METADATA_FILE}",
            )
        ],
        ignore_airflow_env=True,
    )


def check_if_modif():
    # minio_process.get_file_content
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
            return True


def gather_data(ti):
    print("Getting resources list")
    metadata_content = json.loads(
        minio_process.get_file_content("dev/dfi/metadata.json")
    )
    urls_resources = [i.get("url") for i in metadata_content]
    logging.info("Start downloading DFI files")
    filenames = download_ressources(urls_resources, DATADIR)
    logging.info("End downloading DFI files")
    logging.info("Reformat CSV to get a child and parent parcelle for each line")
    reformat_dfi(filenames, f"{DATADIR}/dfi.csv")
    logging.info("Load into Duckdb and export to parquet")
    with duckdb.connect(output_duckdb_database_name) as con:
        for query in queries:
            logging.info(query)
            con.sql(query)

    Path(output_duckdb_database_name).unlink()
    Path(f"{DATADIR}/dfi.csv").unlink()

    parquets = [f"{DATADIR}/dfi.parquet", f"{DATADIR}/nature_dfi_codes.parquet"]
    logging.info(
        "Convert parquet to get the exact structure beetween parquet and CSV contrary to reformated CSV"
    )
    for parquet in parquets:
        df = pd.read_parquet(parquet)
        df.to_csv(parquet.replace(".parquet", ".csv"), index=False)
        logging.info(f"Convert parquet file {parquet} to CSV")


def send_to_minio():
    logging.info("Start to send files to Minio")
    names = ["dfi", "nature_dfi_codes"]
    exts = ["csv", "parquet"]
    fileslist = [
        File(
            source_path=f"{DATADIR}/",
            source_name=f"{filename}",
            dest_path="dfi/",
            dest_name=f"{filename}",
        )
        for filename in [
            i for g in [[f"{name}.{ext}" for name in names] for ext in exts] for i in g
        ]
    ]
    minio_open.send_files(
        list_files=fileslist,
        ignore_airflow_env=True,
    )
    logging.info("End sending files to Minio Open")


# def publish_on_datagouv(ti):
#     min_date = ti.xcom_pull(key="min_date", task_ids="gather_data")
#     max_date = ti.xcom_pull(key="max_date", task_ids="gather_data")
#     for _ext in ["csv", "parquet"]:
#         local_client.resource(
#             id=config[f"deces_{_ext}"][AIRFLOW_ENV]["resource_id"],
#             dataset_id=config[f"deces_{_ext}"][AIRFLOW_ENV]["dataset_id"],
#             fetch=False,
#         ).update(
#             payload={
#                 "url": (
#                     f"https://object.files.data.gouv.fr/{MINIO_BUCKET_DATA_PIPELINE_OPEN}"
#                     f"/deces/deces.{_ext}"
#                 ),
#                 "filesize": os.path.getsize(DATADIR + f"/deces.{_ext}"),
#                 "title": (
#                     f"Personnes décédées entre {min_date} et {build_year_month(max_date)} (format {_ext})"
#                 ),
#                 "format": _ext,
#                 "description": (
#                     f"Personnes décédées entre {min_date} et {build_year_month(max_date)} (format {_ext})"
#                     " (créé à partir des [fichiers de l'INSEE]"
#                     "(https://www.data.gouv.fr/fr/datasets/5de8f397634f4164071119c5/))"
#                 ),
#             },
#         )
#     min_iso, max_iso = build_temporal_coverage(min_date, max_date)
#     local_client.dataset(
#         config["deces_csv"][AIRFLOW_ENV]["dataset_id"], fetch=False
#     ).update(
#         payload={
#             "temporal_coverage": {
#                 "start": min_iso,
#                 "end": max_iso,
#             },
#         },
#     )


# def notification_mattermost():
#     dataset_id = config["deces_csv"][AIRFLOW_ENV]["dataset_id"]
#     send_message(
#         f"Données DFI agrégées :"
#         f"\n- uploadées sur Minio"
#         f"\n- publiées [sur {'demo.' if AIRFLOW_ENV == 'dev' else ''}data.gouv.fr]"
#         f"({local_client.base_url}/fr/datasets/{dataset_id}/)"
#     )
