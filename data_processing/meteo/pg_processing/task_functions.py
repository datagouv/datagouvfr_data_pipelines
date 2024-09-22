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
import psycopg2
from io import StringIO
import subprocess
import numpy as np
from typing import Optional
import pandas as pd

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
from datagouvfr_data_pipelines.utils.minio import MinIOClient

ROOT_FOLDER = "datagouvfr_data_pipelines/data_processing/"
DATADIR = f"{AIRFLOW_DAG_TMP}meteo_pg/data/"
with open(f"{AIRFLOW_DAG_HOME}{ROOT_FOLDER}meteo/config/dgv.json") as fp:
    config = json.load(fp)

minio_meteo = MinIOClient(bucket='meteofrance')


conn = BaseHook.get_connection("POSTGRES_METEO")
db_params = {
    'database': conn.schema,
    'user': conn.login,
    'password': conn.password,
    'host': conn.host,
    'port': conn.port,
}

SCHEMA_NAME = 'meteo'
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


def clean_hooks(value):
    return value.replace("latest-", "").replace("previous-", "")


# %%
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


# %%
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


# %%
def get_latest_ftp_processing(ti):
    r = requests.get("https://object.data.gouv.fr/meteofrance/data/updated_files.json")
    r.raise_for_status()
    ti.xcom_push(key="latest_ftp_processing", value=r.json())


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
            return
        # regex_infos looks like this: {'DEP': '07', 'AAAAMM': 'latest-2023-2024'} (for BASE/MENS)
        regex_infos = get_regex_infos(
            config[dataset_name]["source_pattern"],
            resource["url"].split("/")[-1],
            config[dataset_name]["params"],
        )
        if regex_infos["DEP"] not in DEPIDS:
            # for local dev only, to cut processing time
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
                    file_path = download_resource(resource, dataset_name)
                    # this file has been updated at least once since last check, it will be processd
                    # no need to check other dates
                    break
            if file_path is None:
                # this file has not been updated since last check, moving on
                print(resource['title'], 'has not been updated since last check')
                continue
        else:
            # no latest processing date => processing every file
            file_path = download_resource(resource, dataset_name)

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
        unzip_csv_gz(file_path.replace(".csv", "_old.csv"))

        _conn = psycopg2.connect(**db_params)
        _conn.autocommit = False
        try:
            diff = get_diff(
                _conn=_conn,
                csv_path=csv_path,
                regex_infos=regex_infos,
                table=table_name,
            )
            delete_and_insert_into_pg(
                _conn=_conn,
                diff=diff,
                regex_infos=regex_infos,
                table=table_name,
                csv_path=csv_path,
            )
            _conn.commit()

            minio_meteo.send_files(
                list_files=[
                    {
                        "source_path": file_path.parent.as_posix(),
                        "source_name": file_path.name,
                        "dest_path": "synchro_pg/",
                        "dest_name": file_path.name,
                    }
                ],
                ignore_airflow_env=True
            )

            # deleting if everything was successful, so that we can check content otherwise
            parent = file_path.parent.as_posix()
            for file in os.listdir(parent):
                os.remove(f"{parent}/{file}")
            print("=> Completed work for:", regex_infos["name"])
        except Exception as e:
            _conn.rollback()
            print(f"An error occurred: {e}")
            print("Transaction rolled back.")
            raise
        finally:
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
    }], timeout=TIMEOUT)
    try:    
        download_files([{
            "url": res["url"].replace("data/synchro_ftp/", "synchro_pg/"),
            "dest_path": file_path.parent.as_posix(),
            "dest_name": file_path.name.replace(".csv", "_old.csv"),
        }], timeout=TIMEOUT)
    except:
        print("not existing file in postgres mirror, creating one empty")
        with open(file_path, 'r') as source, open(file_path.replace(".csv", "_old.csv"), 'w') as destination:
            first_line = source.readline()
            destination.write(first_line)
    return file_path


def unzip_csv_gz(file_path):
    output_file_path = str(file_path)[:-3]
    with gzip.open(file_path, 'rb') as f_in:
        with open(output_file_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    os.remove(file_path)
    return output_file_path


def get_diff(_conn, csv_path: Path, regex_infos: dict, table: str):

    def run_diff(csv_path: str, regex_infos):
        df1 = pd.read_csv(csv_path, sep=";", dtype=str)
        df2 = pd.read_csv(csv_path.replace(".csv", "_old.csv"), sep=";", dtype=str)
        diff = df1.merge(df2, how='left', indicator=True)
        dff = diff[diff['_merge'] == 'left_only'].drop(columns=['_merge'])
        dff["DEP"] = regex_infos["regex_infos"]["DEP"]
        dff.to_csv(csv_path.replace(".csv", "_additions.csv"), index=False)
        if dff.shape[0] > 0:
            print(f"-> inserting {dff.shape[0]} rows")
            is_additions = True
        else:
            print(f"-> no additions to insert")
            is_additions = False
        diff = df2.merge(df1, how='left', indicator=True)
        dff = diff[diff['_merge'] == 'left_only'].drop(columns=['_merge'])
        dff.to_csv(csv_path.replace(".csv", "_deletions.csv"), index=False)
        return is_additions
    

    def _build_deletions(csv_path, nb_rows):
        # deletions will be a list of lists of tuples (column_name, typed value)
        # we are only keep primary keys: NUM_POSTE and period (AAAA...)
        df = pd.read_csv(csv_path.replace(".csv", "_deletions.csv"))
        nb_deletions = df.shape[0]
        print(f"-> deleting {nb_deletions} rows")
        if (nb_deletions - nb_rows) / nb_rows > 0.5:
            raise ValueError("Was going to delete more than half of the table, aborting")
        for index, row in df.iterrows():
            yield [
                (item, row[item]) for item in row.to_dict() if item.lower() == "num_poste" or item.lower().startswith("aaaa")
            ]


    # def run_diff(csv_path: str):
    #     # cf https://github.com/aswinkarthik/csvdiff
    #     status = subprocess.run([
    #         f'csvdiff {csv_path.replace(".csv", "_old.csv")} {csv_path} '
    #         f'-o json > {csv_path.replace(".csv", ".json")}'
    #     ], shell=True)
    #     if status.returncode != 0:
    #         raise Exception("The csvdiff command wasn't successful:", status.stderr)

    # def _build_additions(diff, dep):
    #     # we'll copy_expert the additions, so we handle it as an open csv content
    #     print(f"-> inserting {len(diff['Additions'])} rows")
    #     return StringIO('\n'.join(
    #         row + f";{dep}" for row in diff['Additions']
    #     )) if diff['Additions'] else None

    # def _build_deletions(diff, columns, nb_rows):
    #     # deletions will be a list of lists of tuples (column_name, typed value)
    #     # we are only keep primary keys: NUM_POSTE and period (AAAA...)
    #     nb_deletions = len(diff['Deletions'])
    #     print(f"-> deleting {nb_deletions} rows")
    #     if (nb_deletions - nb_rows) / nb_rows > 0.5:
    #         raise ValueError("Was going to delete more than half of the table, aborting")
    #     for row in diff['Deletions']:
    #         cells = row.split(';')
    #         yield [
    #             (name, _type(value)) for name, value, _type in zip(columns.keys(), cells, columns.values())
    #             if _type(value) and (name.lower() == "num_poste" or name.lower().startswith("aaaa"))
    #         ]

    def build_modifs(_conn, csv_path: str, table_name: str, dep: str):
        cursor = _conn.cursor()
        cursor.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            f"WHERE table_name = '{table_name}' ORDER BY ordinal_position;"
        )
        columns = cursor.fetchall()
        cursor.execute(f"SELECT COUNT(*) FROM {SCHEMA_NAME}.{table_name};")
        nb_rows = cursor.fetchall()[0][0]
        cursor.close()
        columns = {
            c[0]: type_mapping[c[1]] for c in columns
        }
        # with open(csv_path.replace(".csv", ".json"), "r") as f:
        #     diff = json.load(f)
        # if diff['Modifications']:
        #     raise NotImplementedError("We should handle row modifications")
        #additions = _build_additions(diff, dep)
        deletions = _build_deletions(csv_path, nb_rows)
        # print("add", len(additions))
        # print("del", len(deletions))
        return deletions

    # getting columns for the query, as we can't SELECT * EXCEPT(dep)
    with open(csv_path, 'r') as f:
        reader = csv.reader(f, delimiter=';')
        columns = next(reader)
    table_name = f'{table}_{regex_infos["regex_infos"]["DEP"]}'
    # getting potentially impacted rows
    
    
    # query = (
    #     f"SELECT {', '.join(columns)} FROM {SCHEMA_NAME}.{table_name} WHERE 1 = 1 " +
    #     build_query_filters(regex_infos)
    # )
    # # print("QUERY:", query)
    # cursor = _conn.cursor()
    # cursor.execute(query)
    # rows = cursor.fetchall()
    # # if no previous rows, we're uploading the whole file
    # if not rows:
    #     return None, None
    # column_names = [desc[0].upper() for desc in cursor.description]
    # lat_lon_idx = (
    #     np.argwhere(np.array(column_names) == 'LAT')[0][0],
    #     np.argwhere(np.array(column_names) == 'LON')[0][0]
    # )
    # rr_idx = None

    # if "MIN_" in csv_path:
    #     rr_idx = np.argwhere(np.array(column_names) == 'RR')[0][0]
    # cursor.close()
    # with open(csv_path.replace(".csv", "_old.csv"), 'w', newline='') as csvfile:
    #     writer = csv.writer(csvfile, delimiter=';')
    #     writer.writerow(column_names)
    #     for row in rows:
    #         _row = fix_lat_lon(row, lat_lon_idx)
    #         if rr_idx:
    #             _row = fix_rr(_row, rr_idx)
    #         writer.writerow(_row)
    
    # run csvdiff on old VS new file (order matters)
    is_additions = run_diff(csv_path=csv_path, regex_infos=regex_infos)
    return is_additions, build_modifs(_conn, csv_path, table_name, regex_infos["regex_infos"]["DEP"])


def fix_lat_lon(row, lat_lon_idx):
    # latitude and longitude always have 6 decimal digits in source files, even if last ones are zeros
    # in this case they are truncated when returned from postgres, e.g: 5.669000 => 5.669
    # but the rows should be considered identical
    return [
        v if idx not in lat_lon_idx
        else str(v) + "0" * (6 - len(str(v).split(".")[-1]))
        for idx, v in enumerate(row)
    ]


def fix_rr(row, rr_idx):
    # similarly, RR in minute data should have 3 decimal digits but only has one (and zeros)
    return [
        v if idx != rr_idx
        else v if v is None
        else str(v) + "0" * (3 - len(str(v).split(".")[-1]))
        for idx, v in enumerate(row)
    ]


def build_query_filters(regex_infos: dict):
    filters = ""
    for param, value in regex_infos["regex_infos"].items():
        split_pv = clean_hooks(value).split("-")
        if len(split_pv) > 1:
            lowest_period = split_pv[0]
            highest_period = str(int(split_pv[1]) + 1)
            filters += f"AND {param} >= '{lowest_period}' AND {param} < '{highest_period}'"
        else:
            filters += f"AND {param} = '{value}' "
    return filters


def delete_and_insert_into_pg(_conn, diff, regex_infos, table, csv_path):
    table_name = f'{table}_{regex_infos["regex_infos"]["DEP"]}'
    is_additions, deletions = diff
    if is_additions is False and deletions is None:
        print("-> Inserting whole file into", table_name)
        load_whole_file(_conn, table_name, csv_path, regex_infos)
        return
    delete_old_data(_conn, table_name, deletions)
    # drop_indexes(conn2, table_name)
    if is_additions: load_new_data(_conn, table_name, csv_path)
    # create_indexes(conn2, table_name, period)


def load_whole_file(_conn, table_name, csv_path, regex_infos):
    with open(csv_path, 'r') as f:
        reader = csv.reader(f, delimiter=';')
        columns = next(reader)
        columns.append('DEP')
        # Temporary file to hold the modified CSV data (we're adding departement column)
        temp_csv_path = csv_path + '.temp'
        with open(temp_csv_path, 'w', newline='') as temp_f:
            writer = csv.writer(temp_f, delimiter=';')
            writer.writerow(columns)
            for row in reader:
                row = [None if val == '' else val for val in row]
                row.append(regex_infos["regex_infos"]["DEP"])
                writer.writerow(row)
    cursor = _conn.cursor()
    with open(temp_csv_path, 'r') as f:
        cursor.copy_expert(
            f"COPY {SCHEMA_NAME}.{table_name} FROM STDIN WITH CSV HEADER DELIMITER ';'",
            f
        )
    cursor.close()


def load_new_data(_conn, table_name, csv_path):
    cursor = _conn.cursor()
    with open(csv_path.replace(".csv", "_additions.csv"), 'r') as f:
        cursor.copy_expert(
            f"COPY {SCHEMA_NAME}.{table_name} FROM STDIN WITH CSV HEADER DELIMITER ','",
            f
        )
    cursor.close()
    # print("-> LOAD OK:", table_name)


def delete_old_data(_conn, table_name, deletions):
    # deletions is a list of lists of tuples (column_name, typed value)
    query = f"DELETE FROM {SCHEMA_NAME}.{table_name} WHERE 1 = 2"
    skip = True
    for row in deletions:
        skip = False
        filters = []
        for name, value in row:
            filters.append(f"{name}='{value}'")
        query += f' OR ({" AND ".join(filters)})'
    # print(query)
    if skip:
        return
    cursor = _conn.cursor()
    cursor.execute(query)
    cursor.close()
    # print("-> DEL OK:", table_name)


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
    execute_query(
        conn.host,
        conn.port,
        conn.schema,
        conn.login,
        conn.password,
        f"INSERT INTO dag_processed (processed) VALUES ('{new_latest_date}');",
        SCHEMA_NAME,
    )


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
