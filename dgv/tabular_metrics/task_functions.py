from datetime import datetime
import logging
import os
import re
import shutil
import tarfile

import pandas as pd

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_HOME,
    AIRFLOW_DAG_TMP,
    MINIO_BUCKET_INFRA,
)
from datagouvfr_data_pipelines.utils.filesystem import File
from datagouvfr_data_pipelines.utils.minio import MinIOClient
from datagouvfr_data_pipelines.utils.postgres import PostgresClient


DAG_FOLDER = "datagouvfr_data_pipelines/dgv/tabular_metrics/"
DATADIR = f"{AIRFLOW_DAG_TMP}tabular_metrics/"
minio_client = MinIOClient(bucket=MINIO_BUCKET_INFRA)
pgclient = PostgresClient(conn_name="POSTGRES_METRIC")
already_processed_table = "tabular_processed"
logs_folder = "prod/metrics-logs/processed/"


def create_tabular_metrics_tables() -> None:
    pgclient.execute_sql_file(
        file=File(
            source_path=f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}sql/",
            source_name="create_tables.sql",
            column_order=None,
        ),
    )


def extract_infos(file_name: str) -> tuple[str, str]:
    # to sort files properly
    # file names look like: haproxy-logs-27082025-slb-05.tar.gz
    _, _, date, _, slb = file_name.split("-")
    return (date[4:] + date[2:4] + date[:2], slb[:2])


def process_line(line: str) -> dict:
    status, method, url, timestamp = None, None, None, None
    # logs look like this (in the best case):
    # 2025-08-01T00:01:19.614400+02:00 slb-04 haproxy[345597]: 127.0.0.1:2040 [01/Aug/2025:00:01:19.373] WILDCARD~ WWW-TABULAR-API-DATAGOUVFR/www-tabular-api2 0/0/5/235/+240 200 +4896 - - ---- 707/57/4/2/0 0/0 "GET /api/resources/b363e051-9649-4879-ae78-71ef227d0cc5/data/?page=5&page_size=20 HTTP/1.1"
    try:
        status = int(re.search("\+\d+ \d+ \+\d+", line).group().split(" ")[1])
        if not (200 <= status < 600):
            status = None
    except Exception:
        pass

    try:
        method, url, _ = re.search('".+"', line).group()[1:-1].split(" ")
        url = url.replace("https://tabular-api.data.gouv.fr", "")
    except Exception:
        pass

    try:
        timestamp = datetime.fromisoformat(line.split(" ")[0])
    except Exception:
        pass

    return {"status": status, "method": method, "url": url, "timestamp": timestamp}


def get_params(url: str) -> dict[str, str] | None:
    # URLs look like this: /api/resources/5f29737c-5393-46f9-8140-2509992adc7a/data/?code_insee_commune__exact=29020
    split = url.split("?")
    if len(split) != 2:
        return
    try:
        params = split[1].split("&")
        return {k: v for k, v in [p.split("=") for p in params]}
    except Exception:
        return


def process_logs_file(file_path: str):
    tabular_lines = []
    idx = 0
    with open(file_path, "r") as f:
        while True:
            line = f.readline()
            idx += 1
            if idx % 1e6 == 0:
                logging.info(f"> {idx} lines scanned")
            if not line:
                break
            if "tabular-api" in line.lower():
                tabular_lines.append(line)

    data = []
    for line in tabular_lines:
        res = process_line(line)
        if res["url"] is not None:
            data.append(res)
    del tabular_lines
    logging.info(f"Found {len(data)} lines with tabular logs")
    if len(data) == 0:
        return

    # we have a lot of info here, keeping this level of details in case
    # we want to elaborate more on the process later
    df = pd.DataFrame(data)
    del data
    df["params"] = df["url"].apply(lambda url: get_params(url))
    df["resource_id"] = df["url"].apply(
        lambda url: searched.group()[1:-1]
        if (searched := re.search(r"/[a-f0-9\-]+/", url.split("?")[0])) is not None
        else None
    )
    # here we still may have unwanted rows due to unexpected filters syntaxes
    df = df.loc[df["resource_id"].str.len() == 36]
    # upon team decision, only keeping successful calls
    df = df.loc[df["status"].between(200, 399)]
    if len(df) == 0:
        logging.warning("No clean data to insert, skipping")
        return
    logging.info(f"Proceeding with {len(df)} successful calls")
    df["date_metric"] = df["timestamp"].dt.date
    stats = (
        df.groupby(["resource_id", "date_metric"])
        .size()
        .reset_index()
        .rename({0: "nb_calls"}, axis=1)
    )
    csv_file_path = file_path.replace(".log", ".csv")
    stats.to_csv(csv_file_path, index=False)

    # upserting data into the table
    # creating a temporary table to store the current data
    tmp_table_name = "tmp_table"
    pgclient.execute_query(
        f"""CREATE TEMP TABLE {tmp_table_name} (
            resource_id CHARACTER VARYING,
            date_metric DATE,
            nb_calls INTEGER
        ) ON COMMIT DROP;""",
        commit=False,
    )
    pgclient.copy_file(
        File(
            source_path="/".join(csv_file_path.split("/")[:-1]),
            source_name=csv_file_path.split("/")[-1],
        ),
        table=tmp_table_name,
        has_header=True,
        commit=False,
    )
    tabular_metrics_table = "calls_tabular"
    pgclient.execute_query(
        f"""INSERT INTO metric.{tabular_metrics_table} (resource_id, date_metric, nb_calls)
        SELECT resource_id, date_metric, nb_calls FROM {tmp_table_name}
        ON CONFLICT (resource_id, date_metric)
        DO UPDATE SET nb_calls = {tabular_metrics_table}.nb_calls + EXCLUDED.nb_calls;"""
    )


def process_logs():
    # fetching already processed files has to be in here instead of a separate task
    # to ensure restarting this task reloads the list of files to process
    logging.info("Retrieving processed log files from db...")
    already_processed: list[str] = [
        row["file_name"]
        for row in pgclient.execute_query(
            f"SELECT file_name from metric.{already_processed_table}"
        )
    ]
    logging.info("Retrieving existing log files in bucket...")
    all_logs = [
        file_path.split("/")[-1]
        for file_path in minio_client.get_files_from_prefix(
            prefix=logs_folder,
            ignore_airflow_env=True,
        )
    ]
    to_process = list(
        sorted(
            list(set(all_logs) - set(already_processed)),
            key=lambda x: extract_infos(x),
        )
    )
    logging.info(f"Found {len(to_process)} log files to process")
    if len(to_process) == 0:
        return
    for idx, log in enumerate(reversed(to_process)):
        logging.info(f"Processing {log} ({idx + 1}/{len(to_process)})")
        minio_client.download_files(
            list_files=[
                File(
                    source_path=logs_folder,
                    source_name=log,
                    dest_path=DATADIR,
                    dest_name=log,
                    remote_source=True,
                ),
            ],
            ignore_airflow_env=True,
        )
        folder = DATADIR + log.split(".")[0] + "/"
        with tarfile.open(DATADIR + log) as f:
            f.extractall(folder)
        os.remove(DATADIR + log)
        if len(os.listdir(folder)) > 1:
            raise ValueError(f"More than one file extracted: {os.listdir(folder)}")
        process_logs_file(folder + os.listdir(folder)[0])
        shutil.rmtree(folder)
        pgclient.execute_query(
            f"""INSERT INTO metric.{already_processed_table} (file_name, date_processed)
            VALUES ('{log}', '{datetime.today().strftime("%Y-%m-%d")}');"""
        )
