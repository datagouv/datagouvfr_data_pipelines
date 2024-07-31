from airflow.hooks.base import BaseHook

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_HOME,
    AIRFLOW_DAG_TMP,
)
from datagouvfr_data_pipelines.utils.postgres import (
    execute_sql_file,
    execute_query,
)
import json
import requests
import os
from pathlib import Path
import glob
import gzip
import shutil
import psycopg2
import csv
from datetime import datetime, timedelta
import re
from jinja2 import Environment, FileSystemLoader

ROOT_FOLDER = "datagouvfr_data_pipelines/data_processing/"
DATADIR = f"{AIRFLOW_DAG_TMP}meteo_pg/data/"
with open(f"{AIRFLOW_DAG_HOME}{ROOT_FOLDER}meteo/config/dgv.json") as fp:
    config = json.load(fp)


conn = BaseHook.get_connection("POSTGRES_METEO")

SCHEMA_NAME = 'meteo'
DATASETS_TO_PROCESS = [
    "BASE/MENS",
    "BASE/DECAD",
    "BASE/DECADAGRO",
    "BASE/QUOT",
    "BASE/HOR",
    "BASE/MIN",
]

DEPIDS = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35', '36', '37', '38', '39', '40', '41', '42', '43', '44', '45', '46', '47', '48', '49', '50', '51', '52', '53', '54', '55', '56', '57', '58', '59', '60', '61', '62', '63', '64', '65', '66', '67', '68', '69', '70', '71', '72', '73', '74', '75', '76', '77', '78', '79', '80', '81', '82', '83', '84', '85', '86', '87', '88', '89', '90', '91', '92', '93', '94', '95', '971', '972', '973', '974', '975', '984', '985', '986', '987', '988', '99']


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
    data = r.json()
    ti.xcom_push(key="latest_ftp_processing", value=data)


def fetch_resources(dataset):
    url = f"https://www.data.gouv.fr/api/1/datasets/{config[dataset]['dataset_id']['prod']}"
    response = requests.get(
        url,
       headers={"X-fields": "resources"},
    )

    response.raise_for_status()  # Ensure we notice bad responses
    return response.json()["resources"]


def download_resource(res, dataset):
    response = requests.get(res["url"], stream=True)
    if dataset == "BASE/QUOT":
        if "Vent" in res['title']:
            file_path = Path(DATADIR) / (config[dataset]["table_name"] + "_vent") / f"{res['title']}.{res['format']}"
        else:
            file_path = Path(DATADIR) / (config[dataset]["table_name"] + "_autres") / f"{res['title']}.{res['format']}"
    else:
        file_path = Path(DATADIR) / config[dataset]["table_name"] / f"{res['title']}.{res['format']}"
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with file_path.open('wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    return Path(file_path)


def get_regex_infos(pattern, filename, params):
    match = re.match(pattern, filename)
    mydict = {}
    if match:
        for item in params:
            mydict[params[item]] = match.group(item)
    return mydict


def process_resources(resources, dataset, latest_ftp_processing, dates=None):
    mydict = {}
    for res in resources:
        if res["type"] == "main":
            file_path = ""
            if dates:
                for d in dates:
                    files = [item["name"] for item in latest_ftp_processing[d] if d in latest_ftp_processing]
                    if (
                        res["url"].replace(f"https://object.files.data.gouv.fr/meteofrance/data/synchro_ftp/{dataset}/", "") in files
                    ):
                        file_path = download_resource(res, dataset)
            else:
                file_path = download_resource(res, dataset)
            if file_path != "":
                regex_infos = get_regex_infos(
                    config[dataset]["source_pattern"],
                    res["url"].split("/")[-1],
                    config[dataset]["params"],
                )
                if dataset == "BASE/QUOT":
                    if "_autres" in file_path.name:
                        table_name = "base_quot_autres"
                    else:
                        table_name = "base_quot_vent"
                else:
                    table_name = config[dataset]["table_name"]
                data = {"name": file_path.name, "regex_infos": regex_infos}
                print(file_path)
                csv_path = unzip_csv_gz(file_path)
                delete_and_insert_into_pg(data, table_name, csv_path)
    return mydict


def download_data(ti):
    latest_processed_date = ti.xcom_pull(key="latest_processed_date", task_ids="retrieve_latest_processed_date")
    latest_ftp_processing = ti.xcom_pull(key="latest_ftp_processing", task_ids="get_latest_ftp_processing")
    mydict = {}
    if not latest_processed_date:
        # Process everything
        dates = None
        new_latest_date = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
        for dataset in DATASETS_TO_PROCESS:
            resources = fetch_resources(dataset)
            mydict.update(process_resources(resources, dataset, latest_ftp_processing, dates=dates))
    else:
        # Process subset
        dates = [item for item in latest_ftp_processing if item != 'latest_update']
        dates = [item for item in dates if item > latest_processed_date]
        new_latest_date = max(dates)
        for dataset in DATASETS_TO_PROCESS:
            resources = fetch_resources(dataset)
            mydict.update(process_resources(resources, dataset, latest_ftp_processing, dates=dates))
            
    ti.xcom_push(key="latest_processed_date", value=new_latest_date)
    ti.xcom_push(key="regex_infos", value=mydict)


def unzip_csv_gz(file_path):
    output_file_path = str(file_path)[:-3]

    with gzip.open(file_path, 'rb') as f_in:
        with open(output_file_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    os.remove(file_path)
    return output_file_path


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
        conn2.autocommit = False  # Disable autocommit to start a transaction
        delete_old_data(conn2, table + "_" + regex_infos["regex_infos"]["DEP"], regex_infos)
        load_csv_to_postgres(conn2, csv_path, table + "_" + regex_infos["regex_infos"]["DEP"], regex_infos)
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
            highest_period = str(int(split_pv[1])+1)
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
