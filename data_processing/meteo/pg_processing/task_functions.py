import json
import requests
import os
from pathlib import Path
import gzip
import shutil
import psycopg2
import csv
from datetime import datetime, timedelta
import re
from jinja2 import Environment, FileSystemLoader
import aiohttp
import asyncio
from typing import Optional

from airflow.hooks.base import BaseHook

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_HOME,
    AIRFLOW_DAG_TMP,
)
from datagouvfr_data_pipelines.utils.postgres import (
    execute_sql_file,
    execute_query,
)
from datagouvfr_data_pipelines.utils.download import download_files

ROOT_FOLDER = "datagouvfr_data_pipelines/data_processing/"
DATADIR = f"{AIRFLOW_DAG_TMP}meteo_pg/data/"
with open(f"{AIRFLOW_DAG_HOME}{ROOT_FOLDER}meteo/config/dgv.json") as fp:
    config = json.load(fp)


conn = BaseHook.get_connection("POSTGRES_METEO")

SCHEMA_NAME = 'meteo'

DEPIDS = [
    '01', '02', '03', '04', '05', '06', '07', '08', '09', '10',
    '11', '12', '13', '14', '15', '16', '17', '18', '19', '20',
    '21', '22', '23', '24', '25', '26', '27', '28', '29', '30',
    '31', '32', '33', '34', '35', '36', '37', '38', '39', '40',
    '41', '42', '43', '44', '45', '46', '47', '48', '49', '50',
    '51', '52', '53', '54', '55', '56', '57', '58', '59', '60',
    '61', '62', '63', '64', '65', '66', '67', '68', '69', '70',
    '71', '72', '73', '74', '75', '76', '77', '78', '79', '80',
    '81', '82', '83', '84', '85', '86', '87', '88', '89', '90',
    '91', '92', '93', '94', '95',
    '971', '972', '973', '974', '975',
    '984', '985', '986', '987', '988',
    '99',
]


def create_tables_if_not_exists():
    file_loader = FileSystemLoader(f"{AIRFLOW_DAG_HOME}{ROOT_FOLDER}meteo/pg_processing/sql/")
    env = Environment(loader=file_loader)
    template = env.get_template('create.sql.jinja')
    output = template.render(depids=DEPIDS)
    with open(f"{DATADIR}create.sql", 'w') as file:
        file.write(output)

    execute_sql_file(
        conn.host,
        conn.port,
        conn.schema,
        conn.login,
        conn.password,
        [
            {
                "source_path": DATADIR,
                "source_name": "create.sql",
            }
        ],
        SCHEMA_NAME,
    )


def retrieve_latest_processed_date(ti):
    data = execute_query(
        conn.host,
        conn.port,
        conn.schema,
        conn.login,
        conn.password,
        "SELECT MAX(processed) FROM dag_processed;",
        SCHEMA_NAME,
    )
    print(data)
    ti.xcom_push(key="latest_processed_date", value=data[0]["max"])


def get_latest_ftp_processing(ti):
    r = requests.get("https://object.data.gouv.fr/meteofrance/data/updated_files.json")
    r.raise_for_status()
    ti.xcom_push(key="latest_ftp_processing", value=r.json())


async def fetch_resources(dataset):
    async with aiohttp.ClientSession(raise_for_status=True) as session:
        async with session.get(
            f"https://www.data.gouv.fr/api/1/datasets/{config[dataset]['dataset_id']['prod']}",
            headers={"X-fields": "resources"},
        ) as response:
            r = await response.json()
            return r["resources"]


def download_resource(res, dataset):
    if dataset == "BASE/QUOT":
        if "Vent" in res['title']:
            file_path = (
                f"{DATADIR}{config[dataset]['table_name'] + '_vent'}"
                f"/{res['title']}.{res['format']}"
            )
        else:
            file_path = (
                f"{DATADIR}{config[dataset]['table_name'] + '_autres'}"
                f"/{res['title']}.{res['format']}"
            )
    else:
        file_path = (
            f"{DATADIR}{config[dataset]['table_name']}"
            f"/{res['title']}.{res['format']}"
        )
    file_path = Path(file_path)
    download_files([{
        "url": res["url"],
        "dest_path": file_path.parent.as_posix(),
        "dest_name": file_path.name,
    }])
    return file_path


def get_regex_infos(pattern, filename, params):
    match = re.match(pattern, filename)
    mydict = {}
    if match:
        for item in params:
            mydict[params[item]] = match.group(item)
    return mydict


async def process_resources(
    resources: list[dict],
    dataset_name: str,
    latest_ftp_processing: list,
    dates: Optional[list] = None,
):
    async def _process_ressource(
        res: dict,
        dataset_name: str,
        latest_ftp_processing: list,
        dates: Optional[list] = None,
    ):
        if res["type"] != "main":
            return
        regex_infos = get_regex_infos(
            config[dataset_name]["source_pattern"],
            res["url"].split("/")[-1],
            config[dataset_name]["params"],
        )
        if regex_infos["DEP"] not in DEPIDS:
            return
        if dates:
            for d in dates:
                files = [
                    item["name"] for item in latest_ftp_processing[d]
                    if d in latest_ftp_processing
                ]
                if (
                    res["url"].replace(
                        f"https://object.files.data.gouv.fr/meteofrance/data/synchro_ftp/{dataset_name}/",
                        ""
                    ) in files
                ):
                    file_path = download_resource(res, dataset_name)
        else:
            file_path = download_resource(res, dataset_name)

        if dataset_name == "BASE/QUOT":
            if "_autres" in file_path.name:
                table_name = "base_quot_autres"
            else:
                table_name = "base_quot_vent"
        else:
            table_name = config[dataset_name]["table_name"]
        data = {"name": file_path.name, "regex_infos": regex_infos}
        print(file_path)
        csv_path = unzip_csv_gz(file_path)
        delete_and_insert_into_pg(data, table_name, csv_path)

    await asyncio.gather(*[
        _process_ressource(
            res=resource,
            dataset_name=dataset_name,
            latest_ftp_processing=latest_ftp_processing,
            dates=dates
        )
        for resource in resources
    ])


def download_data(ti, dataset_name):
    latest_processed_date = ti.xcom_pull(
        key="latest_processed_date",
        task_ids="retrieve_latest_processed_date"
    )
    latest_ftp_processing = ti.xcom_pull(
        key="latest_ftp_processing",
        task_ids="get_latest_ftp_processing"
    )
    dates = None
    # ugly but that's how it comes out of postgres
    if latest_processed_date == "None":
        # Process everything
        new_latest_date = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    else:
        # Process subset
        dates = [item for item in latest_ftp_processing if item != 'latest_update']
        dates = [item for item in dates if item > latest_processed_date]
        new_latest_date = max(dates)
    loop = asyncio.get_event_loop()
    resources = loop.run_until_complete(fetch_resources(dataset_name))
    loop.run_until_complete(
        process_resources(resources, dataset_name, latest_ftp_processing, dates=dates)
    )
    ti.xcom_push(key="latest_processed_date", value=new_latest_date)


def unzip_csv_gz(file_path):
    output_file_path = str(file_path)[:-3]
    with gzip.open(file_path, 'rb') as f_in:
        with open(output_file_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    os.remove(file_path)
    return output_file_path


def drop_indexes(conn, table_name):
    cursor = conn.cursor()
    for col in ["dep", "num_poste", "nom_usuel", "year"]:
        query = f"DROP INDEX IF EXISTS idx_{table_name}_{col}"
        cursor.execute(query)
    print("DROP INDEXES OK")
    cursor.close()


def create_indexes(conn, table_name, period):
    cursor = conn.cursor()
    query = f"CREATE INDEX IF NOT EXISTS idx_{table_name}_dep ON meteo.{table_name} (DEP)"
    cursor.execute(query)
    query = f"CREATE INDEX IF NOT EXISTS idx_{table_name}_num_poste ON meteo.{table_name} (NUM_POSTE)"
    cursor.execute(query)
    query = f"CREATE INDEX IF NOT EXISTS idx_{table_name}_nom_usuel ON meteo.{table_name} (NOM_USUEL)"
    cursor.execute(query)
    query = (
        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_year ON meteo.{table_name}"
        f" (substring({period}::text, 1, 4))"
    )
    cursor.execute(query)
    print("CREATE INDEXES OK")
    cursor.close()


def delete_and_insert_into_pg(regex_infos, table, csv_path):
    db_params = {
        'dbname': conn.schema,
        'user': conn.login,
        'password': conn.password,
        'host': conn.host,
        'port': conn.port,
    }
    try:
        conn2 = psycopg2.connect(**db_params)
        table_name = f'{table}_{regex_infos["regex_infos"]["DEP"]}'
        conn2.autocommit = False  # Disable autocommit to start a transaction
        delete_old_data(conn2, table_name, regex_infos)
        # drop_indexes(conn2, table_name)
        load_csv_to_postgres(conn2, csv_path, table_name, regex_infos)
        # create_indexes(conn2, table_name, period)
        # Commit the transaction
        conn2.commit()
        print("Transaction committed successfully.")

    except Exception as e:
        # Rollback the transaction in case of error
        conn2.rollback()
        print(f"An error occurred: {e}")
        print("Transaction rolled back.")
        raise

    finally:
        conn2.close()


def load_csv_to_postgres(conn, csv_path, table_name, regex_infos):
    cursor = conn.cursor()
    with open(csv_path, 'r') as f:
        reader = csv.reader(f, delimiter=';')
        columns = next(reader)  # Read the column names from the first row of the CSV
        columns.append('DEP')

        # Temporary file to hold the modified CSV data
        temp_csv_path = csv_path + '.temp'
        with open(temp_csv_path, 'w', newline='') as temp_f:
            writer = csv.writer(temp_f, delimiter=';')
            writer.writerow(columns)  # Write the column headers to the temp file
            for row in reader:
                row = [None if val == '' else re.sub(r'\s+', '', val) for val in row]
                row.append(regex_infos["regex_infos"]["DEP"])
                writer.writerow(row)

        # Use COPY to load the data into PostgreSQL
        with open(temp_csv_path, 'r') as temp_f:
            cursor.copy_expert(f"COPY {SCHEMA_NAME}.{table_name} FROM STDIN WITH CSV HEADER DELIMITER ';'", temp_f)

        os.remove(temp_csv_path)
        os.remove(csv_path)

    cursor.close()
    print(f"Data from {csv_path} has been loaded into {table_name}.")


def delete_old_data(conn, table_name, regex_infos):
    cursor = conn.cursor()
    query = f"DELETE FROM {SCHEMA_NAME}.{table_name} WHERE 1 = 1 "
    for param in regex_infos["regex_infos"]:
        param_value = regex_infos["regex_infos"][param]
        split_pv = param_value.replace("latest-", "").replace("previous-", "").split("-")
        if len(split_pv) > 1:
            lowest_period = split_pv[0]
            highest_period = str(int(split_pv[1]) + 1)
            query += f"AND {param} >= '{lowest_period}' AND {param} < '{highest_period}'"
        else:
            query += f"AND {param} = '{param_value}' "
    print(query)
    cursor.execute(query)
    cursor.close()


def insert_latest_date_pg(ti):
    latest_processed_date = ti.xcom_pull(key="latest_processed_date", task_ids="download_data")
    execute_query(
        conn.host,
        conn.port,
        conn.schema,
        conn.login,
        conn.password,
        f"INSERT INTO dag_processed (processed) VALUES ('{latest_processed_date}');",
        SCHEMA_NAME,
    )
