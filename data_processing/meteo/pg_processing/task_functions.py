import json
import requests
import os
from collections import defaultdict
from pathlib import Path
import gzip
import shutil
import csv
from datetime import datetime, timedelta
import re
from jinja2 import Environment, FileSystemLoader
from typing import Optional
import pandas as pd
import psycopg2
from airflow.hooks.base import BaseHook

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_HOME,
    AIRFLOW_DAG_TMP,
    AIRFLOW_ENV,
)
from datagouvfr_data_pipelines.utils.postgres import PostgresClient
from datagouvfr_data_pipelines.utils.download import download_files
from datagouvfr_data_pipelines.utils.minio import MinIOClient
from datagouvfr_data_pipelines.utils.mattermost import send_message

ROOT_FOLDER = "datagouvfr_data_pipelines/data_processing/"
DATADIR = f"{AIRFLOW_DAG_TMP}meteo_pg/data/"
with open(f"{AIRFLOW_DAG_HOME}{ROOT_FOLDER}meteo/config/dgv.json") as fp:
    config = json.load(fp)

minio_meteo = MinIOClient(bucket='meteofrance')


SCHEMA_NAME = 'meteo'
pgclient = PostgresClient(conn_name="POSTGRES_METEO", schema=SCHEMA_NAME)
conn = BaseHook.get_connection("POSTGRES_METEO")
db_params = {
    'database': conn.schema,
    'user': conn.login,
    'password': conn.password,
    'host': conn.host,
    'port': conn.port,
}

TIMEOUT = 60 * 5


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


def get_hooked_name(file_name):
    # hooked files will change name, we have to consider the unchanged version
    # see DAG ftp_processing for more insight
    for hook in ['latest', 'previous']:
        hooked = re.findall(f"{hook}-\d+-\d+", file_name)
        if hooked:
            return file_name.replace(hooked[0], hook)
    return file_name


def build_old_file_name(file_name):
    return file_name.replace(".csv", "_old.csv")


def build_additions_file_name(file_name):
    return file_name.replace(".csv", "_additions.csv")


def build_deletions_file_name(file_name):
    return file_name.replace(".csv", "_deletions.csv")


# %%
def create_tables_if_not_exists(ti):
    ti.xcom_push(key="start", value=datetime.now().timestamp())
    file_loader = FileSystemLoader(f"{AIRFLOW_DAG_HOME}{ROOT_FOLDER}meteo/pg_processing/sql/")
    env = Environment(loader=file_loader)
    template = env.get_template('create.sql.jinja')
    output = template.render(depids=DEPIDS)
    with open(f"{DATADIR}create.sql", 'w') as file:
        file.write(output)

    pgclient.execute_sql_file(
        file={
            "source_path": DATADIR,
            "source_name": "create.sql",
        },
    )


# %%
def retrieve_latest_processed_date(ti):
    data = pgclient.execute_query("SELECT MAX(processed) FROM dag_processed;")
    print(data)
    ti.xcom_push(key="latest_processed_date", value=data[0]["max"])


# %%
def get_latest_ftp_processing(ti):
    r = requests.get("https://object.data.gouv.fr/meteofrance/data/updated_files.json")
    r.raise_for_status()
    ti.xcom_push(key="latest_ftp_processing", value=r.json())


# %%
def set_max_date(ti):
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
        dates = [item for item in dates if item >= latest_processed_date]
        new_latest_date = max(dates)

    ti.xcom_push(key="new_latest_date", value=new_latest_date)
    ti.xcom_push(key="dates", value=dates)


# %%
def download_data(ti, dataset_name):
    latest_ftp_processing = ti.xcom_pull(
        key="latest_ftp_processing",
        task_ids="get_latest_ftp_processing"
    )
    dates = ti.xcom_pull(
        key="dates",
        task_ids="set_max_date"
    )

    resources = fetch_resources(dataset_name)
    process_resources(
        resources=resources,
        dataset_name=dataset_name,
        latest_ftp_processing=latest_ftp_processing,
        dates=dates,
    )


def fetch_resources(dataset):
    r = requests.get(
        f"https://www.data.gouv.fr/api/1/datasets/{config[dataset]['dataset_id']['prod']}",
        headers={"X-fields": "resources{type,title,url,format}"},
    )
    r.raise_for_status()
    return r.json()["resources"]


def process_resources(
    resources: list[dict],
    dataset_name: str,
    latest_ftp_processing: list,
    dates: Optional[list] = None,
):
    # going through all resources of the dataset to check which ones to update
    for resource in resources:
        # only main resources
        if resource["type"] != "main":
            continue
        # regex_infos looks like this: {'DEP': '07', 'AAAAMM': 'latest-2023-2024'} (for BASE/MENS)
        regex_infos = get_regex_infos(
            config[dataset_name]["source_pattern"],
            resource["url"].split("/")[-1],
            config[dataset_name]["params"],
        )
        # within datasets, we now have "normal" and comp stations, they'll be processed separately
        if not regex_infos or regex_infos["DEP"] not in DEPIDS:
            # you can reduce DEPIDS for local dev only, to cut processing time
            continue
        if dates:
            file_path = None
            for d in dates:
                files = [
                    item["name"] for item in latest_ftp_processing[d]
                ]
                # checking whether the file has been updated on any date since last check
                if (
                    resource["url"].replace(
                        f"https://object.files.data.gouv.fr/meteofrance/data/synchro_ftp/{dataset_name}/",
                        ""
                    ) in files
                ):
                    file_path, csv_path = download_resource(resource, dataset_name)
                    # this file has been updated at least once since last check, it will be processd
                    # no need to check other dates
                    break
            if file_path is None:
                # this file has not been updated since last check, moving on
                print(resource['title'], 'has not been updated since last check')
                continue
        else:
            # no latest processing date => processing every file
            file_path, csv_path = download_resource(resource, dataset_name)

        if dataset_name == "BASE/QUOT":
            if "_autres" in file_path.name:
                table_name = "base_quot_autres"
            else:
                table_name = "base_quot_vent"
        else:
            table_name = config[dataset_name]["table_name"]
        regex_infos = {"name": file_path.name, "regex_infos": regex_infos}
        print("Starting with", file_path.name)

        _conn = psycopg2.connect(**db_params)
        _conn.autocommit = False
        _failed = False
        try:
            deletions = get_diff(
                _conn=_conn,
                csv_path=csv_path,
                regex_infos=regex_infos,
                table=table_name,
            )
            # skipping if no diff
            if (
                count_lines_in_file(build_deletions_file_name(csv_path)) == 0
                and count_lines_in_file(build_additions_file_name(csv_path)) == 0
            ):
                continue
            delete_and_insert_into_pg(
                _conn=_conn,
                deletions=deletions,
                regex_infos=regex_infos,
                table=table_name,
                csv_path=csv_path,
            )

            if AIRFLOW_ENV == "prod":
                minio_meteo.send_files(
                    list_files=[
                        {
                            # source can be hooked file name
                            "source_path": "/".join(csv_path.split("/")[:-1]) + "/",
                            "source_name": csv_path.split("/")[-1],
                            # but destination has to be the real file name
                            "dest_path": (
                                "synchro_pg/"
                                + "/".join(resource["url"].split("synchro_ftp/")[1].split("/")[:-1])
                                + "/"
                            ),
                            "dest_name": resource["url"].split("/")[-1].replace(".csv.gz", ".csv")
                        }
                    ],
                    ignore_airflow_env=True
                )
            print("=> Completed work for:", regex_infos["name"])
            _conn.commit()
        except Exception as e:
            _failed = True
            _conn.rollback()
            print(f"/!\ An error occurred: {e}")
            print("Transaction rolled back.")
            raise
        finally:
            # deleting if everything was successful, so that we can check content otherwise
            if not _failed:
                parent = file_path.parent.as_posix()
                for file in os.listdir(parent):
                    os.remove(f"{parent}/{file}")
            _conn.close()


def get_regex_infos(pattern, filename, params):
    match = re.match(pattern, filename)
    mydict = {}
    if match:
        for item in params:
            mydict[params[item]] = match.group(item)
    return mydict


def download_resource(res, dataset):
    if dataset == "BASE/QUOT":
        if "Vent" in res['title']:
            file_path = f"{DATADIR}{config[dataset]['table_name'] + '_vent'}/"
        else:
            file_path = f"{DATADIR}{config[dataset]['table_name'] + '_autres'}/"
    else:
        file_path = f"{DATADIR}{config[dataset]['table_name']}/"
    file_name = get_hooked_name(res["url"].split('/')[-1])
    file_path = Path(file_path + file_name)
    download_files([{
        "url": res["url"],
        "dest_path": file_path.parent.as_posix(),
        "dest_name": file_path.name,
    }], timeout=TIMEOUT)
    csv_path = unzip_csv_gz(file_path)
    try:
        old_file = file_path.name.replace(".csv.gz", "_old.csv")
        download_files([{
            "url": res["url"].replace("data/synchro_ftp/", "synchro_pg/").replace(".csv.gz", ".csv"),
            "dest_path": file_path.parent.as_posix(),
            "dest_name": old_file,
        }], timeout=TIMEOUT)
    except Exception as e:
        raise ValueError(f"Download error for {res['url']}: {e}")
        # this should not happen anymore, specific cases will be handled manually
        # print("> This file is not in postgres mirror, creating an empty one for diff")
        # with open(csv_path, "r") as f:
        #     columns = f.readline()
        # with open(build_old_file_name(str(csv_path)), "w") as f:
        #     f.write(columns)
    return file_path, csv_path


def unzip_csv_gz(file_path):
    output_file_path = str(file_path)[:-3]
    with gzip.open(file_path, 'rb') as f_in:
        with open(output_file_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    os.remove(file_path)
    return output_file_path


def get_diff(_conn, csv_path: Path, regex_infos: dict, table: str):

    def run_diff(csv_path: str, dep: str, _filter=None):
        old_file = build_old_file_name(csv_path)
        additions_file = build_additions_file_name(csv_path)
        deletions_file = build_deletions_file_name(csv_path)

        with open(csv_path, 'r') as new_file, open(old_file, 'r') as old_file:
            # skipping headers
            new_file.readline()
            old_file.readline()
            # removing carriage return so that if the last row doesn't have one
            # it'll not be considered new when data is appended
            new_lines = set(
                r.replace("\n", "") for r in new_file
                if _filter is None or r.startswith(_filter)
            )
            old_lines = set(
                r.replace("\n", "") for r in old_file
                if _filter is None or r.startswith(_filter)
            )

        with open(additions_file, 'a') as outFile:
            for line in new_lines:
                if line not in old_lines:
                    outFile.write(line.strip() + f";{dep}\n")

        with open(deletions_file, 'a') as outFile:
            for line in old_lines:
                if line not in new_lines:
                    outFile.write(line + "\n")

    def _build_deletions(_conn, csv_path: str, table_name: str):
        cursor = _conn.cursor()
        cursor.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            f"WHERE table_name = '{table_name}' ORDER BY ordinal_position;"
        )
        columns = cursor.fetchall()
        cursor.close()
        column_types = {
            c[0]: type_mapping[c[1]] for c in columns
        }
        # deletions will be a list of lists of tuples (column_name, typed value)
        # we are only keep primary keys: NUM_POSTE and period (AAAA...)
        with open(build_deletions_file_name(csv_path), "r") as f:
            reader = csv.reader(f, delimiter=";")
            column_names = next(reader)
            for row in reader:
                yield [
                    (col_name, column_types[col_name.lower()](value))
                    for value, col_name in zip(row, column_names)
                    if col_name.lower() == "num_poste" or col_name.lower().startswith("aaaa")
                ]

    table_name = f'{table}_{regex_infos["regex_infos"]["DEP"]}'
    # creating empty additions and deletions files, we'll fill them up
    old_file = build_old_file_name(csv_path)
    additions_file = build_additions_file_name(csv_path)
    deletions_file = build_deletions_file_name(csv_path)
    with open(csv_path, 'r') as new_file, open(old_file, 'r') as old_file:
        header = new_file.readline()
        old_header = old_file.readline()
        # if header != old_header:
        #     raise ValueError("New and old headers differ:", header, "vs", old_header)
        with open(additions_file, 'w') as outFile:
            outFile.write(header.strip() + ";DEP\n")
        with open(deletions_file, 'w') as outFile:
            outFile.write(header.strip() + "\n")

    # if MIN or HOR: files are too big to be handled at once, we build the diff iteratively
    if any(_ in csv_path for _ in ['MN_', 'H_']):
        print("> Building diff in batches...")
        # files are too big to be handled in one go, so we process them in batches
        # using the fact that the first column (NUM_POSTE) always starts with dep + numbers
        for _filter in create_filters(csv_path=csv_path, dep=regex_infos["regex_infos"]["DEP"]):
            run_diff(csv_path=csv_path, dep=regex_infos["regex_infos"]["DEP"], _filter=_filter)
    # for other files it's fine to build diff on the whole file
    else:
        run_diff(csv_path=csv_path, dep=regex_infos["regex_infos"]["DEP"])

    return _build_deletions(_conn, csv_path, table_name)


def create_filters(csv_path: str, dep: str, threshold: int = 5e6):
    # returns a list of prefixes to filter the rows
    # once we know the batches will be of a reasonable size
    postes = pd.read_csv(
        csv_path,
        sep=";",
        dtype=str,
        usecols=["NUM_POSTE"],
    )["NUM_POSTE"]
    maxes = defaultdict(int)
    k = 0
    while True:
        k += 1
        counts = postes.str.slice(len(dep), k + len(dep)).value_counts()
        maxes[max(counts)] += 1
        if max(counts) < threshold or max(maxes.values()) == 3:
            break
    print(f"> built filters of length {k} (max occurences: {max(counts)})")
    return [dep + suffix for suffix in counts.index]


def delete_and_insert_into_pg(_conn, deletions, regex_infos, table, csv_path):
    table_name = f'{table}_{regex_infos["regex_infos"]["DEP"]}'
    nb_add = count_lines_in_file(build_additions_file_name(csv_path))
    nb_del = count_lines_in_file(build_deletions_file_name(csv_path))
    threshold = 20000
    if nb_del > threshold:
        print(f"> More than {threshold} rows to delete ({nb_del}), replacing the whole period...")
        replace_whole_period(_conn, table_name, csv_path, regex_infos)
        return
    if nb_del:
        print(f'> Deleting {nb_del} rows...')
        delete_old_data(_conn, table_name, deletions)
    if nb_add:
        print(f'> Inserting {nb_add} rows...')
        load_new_data(_conn, table_name, csv_path)


def count_lines_in_file(file_path):
    with open(file_path, 'r') as file:
        # skip header
        file.readline()
        line_count = sum(1 for _ in file if _ and _ != "\n")
    return line_count


def clean_hooks(value: str):
    return value.replace("latest-", "").replace("previous-", "")


def build_query_filters(regex_infos: dict):
    filters = ""
    for param, value in regex_infos["regex_infos"].items():
        split_pv = clean_hooks(value).split("-")
        if len(split_pv) > 1:
            lowest_period = split_pv[0]
            highest_period = str(int(split_pv[1]) + 1)
            filters += (
                f"AND substring({param}, 1, 4) >= '{lowest_period}' "
                f"AND substring({param}, 1, 4) < '{highest_period}' "
            )
        else:
            filters += f"AND {param} = '{value}' "
    return filters


def replace_whole_period(_conn, table_name, csv_path, regex_infos):
    print("> Deleting period...")
    cursor = _conn.cursor()
    cursor.execute(f"DELETE FROM {SCHEMA_NAME}.{table_name} WHERE 1=1 " + build_query_filters(regex_infos))
    # the raw source file is missing the DEP column
    csv_with_dep = csv_path.replace(".csv", "_with_dep.csv")
    dep = regex_infos["regex_infos"]["DEP"]
    with open(csv_with_dep, 'w') as dep_file, open(csv_path, 'r') as file:
        for idx, line in enumerate(file.readlines()):
            if idx == 0:
                dep_file.write(line.strip() + ";DEP\n")
            else:
                dep_file.write(line.strip() + f";{dep}\n")
    nb_rows = count_lines_in_file(csv_with_dep)
    print(f"> Inserting whole file ({nb_rows} rows)...")
    with open(csv_with_dep, 'r') as f:
        cursor.copy_expert(
            f"COPY {SCHEMA_NAME}.{table_name} FROM STDIN WITH CSV HEADER DELIMITER ';'",
            f
        )
    cursor.close()


def load_new_data(_conn, table_name, csv_path):
    cursor = _conn.cursor()
    with open(build_additions_file_name(csv_path), 'r') as f:
        cursor.copy_expert(
            f"COPY {SCHEMA_NAME}.{table_name} FROM STDIN WITH CSV HEADER DELIMITER ';'",
            f
        )
    cursor.close()


def delete_old_data(_conn, table_name, deletions):
    # deletions is a list of lists of tuples (column_name, typed value)
    query = f"DELETE FROM {SCHEMA_NAME}.{table_name} WHERE 1 = 2"
    skip = True
    batch_size = 50
    batch = 0
    for row in deletions:
        if row:
            skip = False
            filters = []
            for name, value in row:
                filters.append(f"{name}='{value}'")
            query += f' OR ({" AND ".join(filters)})'
            batch += 1
        # executing deletions in batches for safety
        if batch == batch_size:
            # print(query)
            cursor = _conn.cursor()
            cursor.execute(query)
            cursor.close()
            query = f"DELETE FROM {SCHEMA_NAME}.{table_name} WHERE 1 = 2"
            batch = 0
    # print(query)
    if skip:
        return
    # deleting the last batch
    cursor = _conn.cursor()
    cursor.execute(query)
    cursor.close()


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


# %%
def insert_latest_date_pg(ti):
    new_latest_date = ti.xcom_pull(
        key="new_latest_date",
        task_ids="set_max_date"
    )
    print(new_latest_date)
    pgclient.execute_query(
        f"INSERT INTO dag_processed (processed) VALUES ('{new_latest_date}');",
    )


# %%
def send_notification(ti):
    start = ti.xcom_pull(key="start", task_ids="create_tables_if_not_exists")
    # weirdly start is pushed as a timestamp (float) but pulled as a datetime
    if isinstance(start, datetime):
        start = start.timestamp()
    duration = timedelta(seconds=round(datetime.now().timestamp() - start))
    send_message(
        text=f"##### 🌦️ Données météo mises à jour dans postgres en {duration}"
    )
