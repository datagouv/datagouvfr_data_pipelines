import json
import requests
import os
from pathlib import Path
import gzip
import shutil
import csv
from datetime import datetime, timedelta
import re
from jinja2 import Environment, FileSystemLoader
import aiohttp
import asyncio
import asyncpg
import subprocess
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
from datagouvfr_data_pipelines.utils.download import async_download_files

ROOT_FOLDER = "datagouvfr_data_pipelines/data_processing/"
DATADIR = f"{AIRFLOW_DAG_TMP}meteo_pg/data/"
with open(f"{AIRFLOW_DAG_HOME}{ROOT_FOLDER}meteo/config/dgv.json") as fp:
    config = json.load(fp)


conn = BaseHook.get_connection("POSTGRES_METEO")

SCHEMA_NAME = 'meteo'
TIMEOUT = 60 * 60 * 2


def smart_cast(value, _type):
    try:
        return _type(value)
    except:
        return None


type_mapping = {
    'character varying': str,
    'double precision': lambda x: smart_cast(x, float),
    'integer': lambda x: smart_cast(x, int),
}

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


async def download_resource(res, dataset):
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
    await async_download_files([{
        "url": res["url"],
        "dest_path": file_path.parent.as_posix(),
        "dest_name": file_path.name,
    }], timeout=TIMEOUT)
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
    max_size: int,
    dates: Optional[list] = None,
):
    async def _process_ressource(
        pool,
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
                ]
                if (
                    res["url"].replace(
                        f"https://object.files.data.gouv.fr/meteofrance/data/synchro_ftp/{dataset_name}/",
                        ""
                    ) in files
                ):
                    file_path = await download_resource(res, dataset_name)
                else:
                    return
        else:
            file_path = await download_resource(res, dataset_name)

        if dataset_name == "BASE/QUOT":
            if "_autres" in file_path.name:
                table_name = "base_quot_autres"
            else:
                table_name = "base_quot_vent"
        else:
            table_name = config[dataset_name]["table_name"]
        regex_infos = {"name": file_path.name, "regex_infos": regex_infos}
        print("Starting with", file_path.name)
        csv_path = unzip_csv_gz(file_path)
        diff = await get_diff(
            pool=pool,
            csv_path=csv_path,
            regex_infos=regex_infos,
            table=table_name,
        )
        await delete_and_insert_into_pg(
            pool=pool,
            diff=diff,
            regex_infos=regex_infos,
            table=table_name,
        )

    db_params = {
        'database': conn.schema,
        'user': conn.login,
        'password': conn.password,
        'host': conn.host,
        'port': conn.port,
    }
    pool = await asyncpg.create_pool(**db_params, max_size=max_size)
    await asyncio.gather(*[
        _process_ressource(
            pool=pool,
            res=resource,
            dataset_name=dataset_name,
            latest_ftp_processing=latest_ftp_processing,
            dates=dates
        )
        for resource in resources
    ])


def build_query_filters(regex_infos: dict):
    filters = ""
    for param in regex_infos["regex_infos"]:
        param_value = regex_infos["regex_infos"][param]
        split_pv = param_value.replace("latest-", "").replace("previous-", "").split("-")
        if len(split_pv) > 1:
            lowest_period = split_pv[0]
            highest_period = str(int(split_pv[1]) + 1)
            filters += f"AND {param} >= '{lowest_period}' AND {param} < '{highest_period}'"
        else:
            filters += f"AND {param} = '{param_value}' "
    return filters


async def get_diff(pool, csv_path: Path, regex_infos: dict, table: str):

    def run_diff(_go, csv_path: str):
        subprocess.run([
            f'csvdiff {csv_path.replace(".csv", "_old.csv")} {csv_path} '
            f'-o json > {csv_path.replace(".csv", ".json")}'
        ], shell=True)

    async def build_modifs(pool, csv_path: str, table_name: str, dep: str):
        async with pool.acquire(timeout=TIMEOUT) as _conn:
            async with _conn.transaction():
                columns = await _conn.fetch(
                    "SELECT column_name, data_type FROM information_schema.columns "
                    f"WHERE table_name = '{table_name}' ORDER BY ordinal_position;"
                )
        columns = {
            c['column_name']: type_mapping[c['data_type']] for c in columns
        }
        with open(csv_path.replace(".csv", ".json"), "r") as f:
            diff = json.load(f)
        # additions need to have the right type
        additions = []
        for row in diff['Additions']:
            cells = row.split(';')
            if cells[0].lower() == list(columns.keys())[0].lower():
                continue
            cells.append(dep)
            additions.append(tuple(
                _type(value) for (_type, value) in zip(columns.values(), cells)
            ))
        # we'll delete based on filters on pivot columns, they are strings
        deletions = []
        for row in diff['Deletions']:
            cells = row.split(';')
            deletions.append([
                (name, _type(value)) for name, value, _type in zip(columns.keys(), cells, columns.values())
                if _type(value)
            ])
        if diff['Modifications']:
            raise NotImplementedError("We should handle row modifications")
        # print("add", additions)
        # print("del", deletions)
        return additions, deletions

    # getting columns for the query, as we can't SELECT * EXCEPT(dep)
    with open(csv_path, 'r') as f:
        reader = csv.reader(f, delimiter=';')
        columns = next(reader)
    table_name = f'{table}_{regex_infos["regex_infos"]["DEP"]}'
    # getting potentially impacted rows
    async with pool.acquire(timeout=TIMEOUT) as _conn:
        async with _conn.transaction():
            query = (
                f"SELECT {', '.join(columns)} FROM {SCHEMA_NAME}.{table_name} WHERE 1 = 1 " +
                build_query_filters(regex_infos)
            )
            _ = await _conn.copy_from_query(
                query,
                output=csv_path.replace(".csv", "_old.csv"),
                format='csv',
                header=True,
                delimiter=";",
            )
    # run csvdiff on old VS new file (order matters)
    run_diff(_, csv_path=csv_path)
    return await build_modifs(pool, csv_path, table_name, regex_infos["regex_infos"]["DEP"])


def download_data(ti, dataset_name, max_size):
    print('MAX_SIZE:', max_size)
    latest_processed_date = ti.xcom_pull(
        key="latest_processed_date",
        task_ids="retrieve_latest_processed_date"
    )
    latest_ftp_processing = ti.xcom_pull(
        key="latest_ftp_processing",
        task_ids="get_latest_ftp_processing"
    )
    dates = None
    if not latest_processed_date:
        # Process everything
        new_latest_date = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    else:
        if not re.match(r'\d{4}-\d{2}-\d{2}', latest_processed_date):
            raise ValueError(
                "You may want to check what is in the 'dag_processed' table"
            )
        # Process subset
        dates = [item for item in latest_ftp_processing if item != 'latest_update']
        dates = [item for item in dates if item > latest_processed_date]
        new_latest_date = max(dates)
    loop = asyncio.get_event_loop()
    resources = loop.run_until_complete(fetch_resources(dataset_name))
    loop.run_until_complete(
        process_resources(
            resources=resources,
            dataset_name=dataset_name,
            latest_ftp_processing=latest_ftp_processing,
            max_size=max_size,
            dates=dates
        )
    )
    ti.xcom_push(key="latest_processed_date", value=new_latest_date)


def unzip_csv_gz(file_path):
    # gzip decompression doesn't seem to be easily made async
    output_file_path = str(file_path)[:-3]
    with gzip.open(file_path, 'rb') as f_in:
        with open(output_file_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    os.remove(file_path)
    return output_file_path


async def drop_indexes(conn, table_name):
    for col in ["dep", "num_poste", "nom_usuel", "year"]:
        query = f"DROP INDEX IF EXISTS idx_{table_name}_{col}"
        await conn.execute(query + ";")
    print("DROP INDEXES OK")


async def create_indexes(conn, table_name, period):
    query = f"CREATE INDEX IF NOT EXISTS idx_{table_name}_dep ON meteo.{table_name} (DEP)"
    await conn.execute(query)
    query = f"CREATE INDEX IF NOT EXISTS idx_{table_name}_num_poste ON meteo.{table_name} (NUM_POSTE)"
    await conn.execute(query)
    query = f"CREATE INDEX IF NOT EXISTS idx_{table_name}_nom_usuel ON meteo.{table_name} (NOM_USUEL)"
    await conn.execute(query)
    query = (
        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_year ON meteo.{table_name}"
        f" (substring({period}::text, 1, 4))"
    )
    await conn.execute(query + ";")
    print("CREATE INDEXES OK")


async def delete_and_insert_into_pg(pool, diff, regex_infos, table):
    table_name = f'{table}_{regex_infos["regex_infos"]["DEP"]}'
    additions, deletions = diff
    try:
        async with pool.acquire(timeout=TIMEOUT) as _conn:
            try:
                async with _conn.transaction():
                    await delete_old_data(_conn, table_name, deletions)
                    # await drop_indexes(conn2, table_name)
                    await load_new_data(_conn, table_name, additions)
                    # await create_indexes(conn2, table_name, period)
            except Exception as e:
                print(f"An error occurred: {e}")
                print("Transaction rolled back.")
                raise
            finally:
                await _conn.close()
                print("=> Completed work for:", table_name, regex_infos["regex_infos"]["PERIOD"])
    except Exception as e:
        print(f"/!\ An error occurred: {e}\nfor {table_name}")
        print("Transaction rolled back.")


async def load_new_data(_conn, table_name, additions):
    # additions is a list of typed tuples
    await _conn.copy_records_to_table(
        table_name=table_name,
        records=additions,
        schema_name=SCHEMA_NAME,
    )
    # print("-> LOAD OK:", table_name)


async def delete_old_data(_conn, table_name, deletions):
    # deletions is a list of lists of tuples (column_name, typed value)
    for row in deletions:
        filters = []
        for name, value in row:
            if isinstance(value, str):
                # getting rid of names having an apostrophe, the num_poste col will filter anyway
                if "'" in value:
                    continue
                filters.append(f"{name}='{value}'")
            else:
                filters.append(f"{name}={value}")
        query = (
            f"DELETE FROM {SCHEMA_NAME}.{table_name} WHERE " +
            " AND ".join(filters)
        )
        # print(query)
        await _conn.execute(query + ";")
    # print("-> DEL OK:", table_name)


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
