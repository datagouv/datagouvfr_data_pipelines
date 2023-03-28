
from airflow.hooks.base import BaseHook
from datetime import datetime
import gzip
import glob
import os
import pandas as pd
import re

from datagouvfr_data_pipelines.utils.datagouv import get_resource
from datagouvfr_data_pipelines.utils.minio import (
    copy_object,
    get_files,
    get_files_from_prefix,
)
from datagouvfr_data_pipelines.utils.postgres import (
    copy_file,
    execute_sql_file,
)

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
    AIRFLOW_DAG_HOME,
    MINIO_URL,
    MINIO_BUCKET_INFRA,
    SECRET_MINIO_DATA_PIPELINE_USER,
    SECRET_MINIO_DATA_PIPELINE_PASSWORD,
)

TMP_FOLDER = f"{AIRFLOW_DAG_TMP}metrics/"
DAG_FOLDER = "datagouvfr_data_pipelines/dgv/metrics/"
conn = BaseHook.get_connection("POSTGRES_DEV")


def create_metrics_tables():
    execute_sql_file(
        conn.host,
        conn.port,
        conn.schema,
        conn.login,
        conn.password,
        [
            {
                "source_path": f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}sql/",
                "source_name": "create_tables.sql",
            }
        ],
    )


def get_new_logs(ti):
    new_logs = get_files_from_prefix(
        MINIO_URL=MINIO_URL,
        MINIO_BUCKET=MINIO_BUCKET_INFRA,
        MINIO_USER=SECRET_MINIO_DATA_PIPELINE_USER,
        MINIO_PASSWORD=SECRET_MINIO_DATA_PIPELINE_PASSWORD,
        prefix="metrics-logs/new/"
    )
    ti.xcom_push(key="new_logs", value=new_logs)
    if new_logs:
        return True
    else:
        return False

def copy_log(new_logs, source_folder, target_folder):    
    for nl in new_logs:
        copy_object(
            MINIO_URL=MINIO_URL,
            MINIO_USER=SECRET_MINIO_DATA_PIPELINE_USER,
            MINIO_PASSWORD=SECRET_MINIO_DATA_PIPELINE_PASSWORD,
            MINIO_BUCKET_SOURCE=MINIO_BUCKET_INFRA,
            MINIO_BUCKET_TARGET=MINIO_BUCKET_INFRA,
            path_source=nl,
            path_target=nl.replace(source_folder, target_folder),
            remove_source_file=True,
        )

def copy_log_to_ongoing_folder(ti):
    new_logs = ti.xcom_pull(key="new_logs", task_ids="get_new_logs")
    copy_log(new_logs, "/new/", "/ongoing/")


def copy_log_to_processed_folder(ti):
    new_logs = ti.xcom_pull(key="new_logs", task_ids="get_new_logs")
    new_logs = [nl.replace("/new/", "/ongoing/") for nl in new_logs]
    copy_log(new_logs, "/ongoing/", "/processed/")



def download_catalog():
    get_resource(
        resource_id="f868cca6-8da1-4369-a78d-47463f19a9a3",
        file_to_store={
            "dest_path": TMP_FOLDER,
            "dest_name": "catalog_datasets.csv",
        }
    )
    get_resource(
        resource_id="b7bbfedc-2448-4135-a6c7-104548d396e7",
        file_to_store={
            "dest_path": TMP_FOLDER,
            "dest_name": "catalog_organizations.csv",
        }
    )
    get_resource(
        resource_id="970aafa0-3778-4d8b-b9d1-de937525e379",
        file_to_store={
            "dest_path": TMP_FOLDER,
            "dest_name": "catalog_reuses.csv",
        }
    )
    get_resource(
        resource_id="4babf5f2-6a9c-45b5-9144-ca5eae6a7a6d",
        file_to_store={
            "dest_path": TMP_FOLDER,
            "dest_name": "catalog_resources.csv",
        }
    )


def remove_files_if_exists(log):
    isExist = os.path.exists(f"{TMP_FOLDER}outputs")
    if not isExist:
        os.makedirs(f"{TMP_FOLDER}outputs")
    for obj in ["datasets", "reuses", "organizations", "resources-id", "resources-static", "resources"]:
        if os.path.isfile(f"{TMP_FOLDER}outputs/{obj}-{log}.csv"):
            os.remove(f"{TMP_FOLDER}outputs/{obj}-{log}.csv")


def get_dict(df, obj_property):
    arr = {}
    for index, row in df.iterrows():
        if (
            type(row[obj_property]) == str
            and "static.data.gouv.fr" in row[obj_property]
        ):
            arr[row[obj_property]] = row["id"]
        else:
            arr[row[obj_property]] = row["id"]
            arr[row["id"]] = row["id"]
    return arr


def get_date(a_date):
    return datetime.strptime(a_date, "%d/%b/%Y:%H:%M:%S.%f").strftime("%Y-%m-%d")


def get_id(arr, list_obj):
    new_list = []
    for lo in list_obj:
        if lo["id"] in arr:
            new_list.append({"id": arr[lo["id"]], "date": lo["date"]})
    return new_list


def append_chunk(cpt, obj_type, arr, list_obj, log):
    print(f"{obj_type} : {cpt}")
    data = get_id(arr, list_obj)
    with open(f"{TMP_FOLDER}outputs/{obj_type}-{log}.csv", 'a') as fp:
        for d in data:
            fp.write(f"{d['id']},{d['date']}\n")


def parse(lines, config, log, haproxy_re):
    cpt = {}
    for object_config in config:
        cpt[object_config["object"]] = 0
    list_obj = []
    for b_line in lines:
        try:
            re_str = re.search(haproxy_re, b_line.decode("utf-8"))
            if re_str:
                parsed_line = re_str.groupdict()
                if "DATAGOUVFR_RGS" in parsed_line["frontend_name"]:
                    url = parsed_line["http_request"].split(" ")[1]
                    for object_config in config:
                        for item in object_config["patterns"]:
                            if url.startswith(item):
                                if object_config["object"] != "resources-static":
                                    slug = url.replace(item, "").split("/")[0]
                                else:
                                    slug = f"https://static.data.gouv.fr{url}"
                                cpt[object_config["object"]] += 1
                                list_obj.append({"id": slug, "date": get_date(parsed_line["accept_date"])})
                                if cpt[object_config["object"]] % 10000 == 0:
                                    append_chunk(
                                        cpt[object_config["object"]],
                                        object_config["object"],
                                        object_config["dict"],
                                        list_obj,
                                        log,
                                    )
                                    list_obj = []
        except:
            raise Exception("Sorry, pb with line")

    for object_config in config:
        append_chunk(
            cpt[object_config["object"]],
            object_config["object"],
            object_config["dict"],
            list_obj,
            log,
        )
    


def process_log(ti):
    new_logs = ti.xcom_pull(key="new_logs", task_ids="get_new_logs")
    newlogs = [nl.replace("/new/", "/ongoing/") for nl in new_logs]
    print("------ LOADING CATALOGS ------")
    catalog_datasets = pd.read_csv(
        f"{TMP_FOLDER}catalog_datasets.csv",
        dtype=str,
        sep=";",
        usecols=["id", "slug", "organization_id"],
    )
    catalog_reuses = pd.read_csv(
        f"{TMP_FOLDER}catalog_reuses.csv",
        dtype=str,
        sep=";",
        usecols=["id", "slug", "organization_id"],
    )
    catalog_organizations = pd.read_csv(
        f"{TMP_FOLDER}catalog_organizations.csv",
        dtype=str,
        sep=";",
        usecols=["id", "slug"],
    )
    catalog_resources = pd.read_csv(
        f"{TMP_FOLDER}catalog_resources.csv",
        dtype=str,
        sep=";",
        usecols=["id", "url", "dataset.id", "dataset.organization_id"],
    )
    print("------ CATALOGS LOADED ------")

    languages = ["fr", "en", "es"]
    config = [
        {
            "object": "datasets",
            "patterns": [f"/{lang}/datasets/" for lang in languages],
            "dict": get_dict(catalog_datasets, "slug"),
        },
        {
            "object": "reuses",
            "patterns": [f"/{lang}/reuses/" for lang in languages],
            "dict": get_dict(catalog_reuses, "slug"),
        },
        {
            "object": "organizations",
            "patterns": [f"/{lang}/organizations/" for lang in languages],
            "dict": get_dict(catalog_organizations, "slug"),
        },
        {
            "object": "resources-static",
            "patterns": ["/resources/"],
            "dict": get_dict(catalog_resources, "url"),
        },
        {
            "object": "resources-id",
            "patterns": [f"/{lang}/datasets/r/" for lang in languages],
            "dict": get_dict(catalog_resources, "url"),
        },
    ]

    re_str = (
        r'(?P<client_ip>[a-fA-F\d+\.:]+):(?P<client_port>\d+)'
        r'\s+\[(?P<accept_date>.+)\]\s+(?P<frontend_name>.*)\s+'
        r'(?P<backend_name>.*)/(?P<server_name>.*)\s+(?P<tq>-?\d+)'
        r'/(?P<tw>-?\d+)/(?P<tc>-?\d+)/(?P<tr>-?\d+)/(?P<tt>\+?\d+)'
        r'\s+(?P<status_code>-?\d+)\s+(?P<bytes_read>\+?\d+)\s+.*\s+'
        r'(?P<act>\d+)/(?P<fe>\d+)/(?P<be>\d+)/(?P<srv>\d+)/'
        r'(?P<retries>\+?\d+)\s+(?P<queue_server>\d+)/'
        r'(?P<queue_backend>\d+)\s+"(?P<http_request>.*)"$'
    )
    haproxy_re = re.compile(re_str)

    print("------ PREPARE TO PROCESS LOGS ------")
    ACTIVE_LOG = 0
    for nl in newlogs:
        get_files(
            MINIO_URL=MINIO_URL,
            MINIO_BUCKET=MINIO_BUCKET_INFRA,
            MINIO_USER=SECRET_MINIO_DATA_PIPELINE_USER,
            MINIO_PASSWORD=SECRET_MINIO_DATA_PIPELINE_PASSWORD,
            list_files=[
                {
                    "source_path": "metrics-logs/ongoing/",
                    "source_name": nl.split("/")[-1],
                    "dest_path": TMP_FOLDER,
                    "dest_name": nl.split("/")[-1]
                }
            ]
        )
        ACTIVE_LOG = ACTIVE_LOG + 1
        remove_files_if_exists(ACTIVE_LOG)
        print("---------------")
        print(ACTIVE_LOG)
        file = gzip.open(f"{TMP_FOLDER}{nl.split('/')[-1]}", "rb")
        lines = file.readlines()
        print("haproxy loaded")
        parse(lines, config, ACTIVE_LOG, haproxy_re)
        
        try:
            res1 = pd.read_csv(f"{TMP_FOLDER}outputs/resources-id-{ACTIVE_LOG}.csv", dtype=str, header=None)
            res2 = pd.read_csv(f"{TMP_FOLDER}outputs/resources-static-{ACTIVE_LOG}.csv", dtype=str, header=None)
            res = pd.concat([res1, res2])
            res.to_csv(f"{TMP_FOLDER}outputs/resources-{ACTIVE_LOG}.csv", index=False, header=False)
        except pd.errors.EmptyDataError:
            print("empty data resources id or static")
        
        try:
            print("--- datasets ----")
            datasets = pd.read_csv(f"{TMP_FOLDER}outputs/datasets-{ACTIVE_LOG}.csv", dtype=str, header=None)
            datasets[3] = 1
            datasets = datasets.groupby([0, 1], as_index=False).count().sort_values(by=[3], ascending=False)
            datasets = datasets.rename(columns={0: "dataset_id", 1: "date_metric", 3: "nb_visit"})
            datasets = pd.merge(datasets, catalog_datasets[["id", "organization_id"]].rename(columns={"id": "dataset_id"}), on="dataset_id", how="left")
            datasets = datasets[["date_metric", "dataset_id", "organization_id", "nb_visit"]]
            datasets.to_csv(f"{TMP_FOLDER}outputs/datasets-{ACTIVE_LOG}.csv", index=False, header=False)
        except pd.errors.EmptyDataError:
            print("empty data datasets")

        try:
            print("--- reuses ----")
            reuses = pd.read_csv(f"{TMP_FOLDER}outputs/reuses-{ACTIVE_LOG}.csv", dtype=str, header=None)
            reuses[3] = 1
            reuses = reuses.groupby([0, 1], as_index=False).count().sort_values(by=[3], ascending=False)
            reuses = reuses.rename(columns={0: "reuse_id", 1: "date_metric", 3: "nb_visit"})
            reuses = pd.merge(reuses, catalog_reuses[["id", "organization_id"]].rename(columns={"id": "reuse_id"}), on="reuse_id", how="left")
            reuses = reuses[["date_metric", "reuse_id", "organization_id", "nb_visit"]]

            reuses.to_csv(f"{TMP_FOLDER}outputs/reuses-{ACTIVE_LOG}.csv", index=False, header=False)
        except pd.errors.EmptyDataError:
            print("empty data reuses")

        try:
            print("--- organizations ----")
            organizations = pd.read_csv(f"{TMP_FOLDER}outputs/organizations-{ACTIVE_LOG}.csv", dtype=str, header=None)
            organizations[3] = 1
            organizations = organizations.groupby([0, 1], as_index=False).count().sort_values(by=[3], ascending=False)
            organizations = organizations.rename(columns={0: "organization_id", 1: "date_metric", 3: "nb_visit"})
            organizations = organizations[["date_metric", "organization_id", "nb_visit"]]
            organizations.to_csv(f"{TMP_FOLDER}outputs/organizations-{ACTIVE_LOG}.csv", index=False, header=False)
        except pd.errors.EmptyDataError:
            print("empty data organizations")
        
        try:
            print("--- resources ----")
            resources = pd.read_csv(f"{TMP_FOLDER}outputs/resources-{ACTIVE_LOG}.csv", dtype=str, header=None)
            resources[3] = 1
            resources = resources.groupby([0, 1], as_index=False).count().sort_values(by=[3], ascending=False)
            resources = resources.rename(columns={0: "resource_id", 1: "date_metric", 3: "nb_visit"})
            resources = pd.merge(resources, catalog_resources[["id", "dataset.id", "dataset.organization_id"]].rename(columns={"id": "resource_id"}), on="resource_id", how="left")
            resources = resources.rename(columns={"dataset.id": "dataset_id", "dataset.organization_id": "organization_id"})
            resources = resources[["date_metric", "resource_id", "dataset_id", "organization_id", "nb_visit"]]
            resources.to_csv(f"{TMP_FOLDER}outputs/resources-{ACTIVE_LOG}.csv", index=False, header=False)
        except FileNotFoundError:
            print("no data resources file")


def save_to_postgres():
    config = [
        {
            "name": "datasets",
            "columns": "(date_metric, dataset_id, organization_id, nb_visit)",
        },
        {
            "name": "reuses",
            "columns": "(date_metric, reuse_id, organization_id, nb_visit)",
        },
        {
            "name": "organizations",
            "columns": "(date_metric, organization_id, nb_visit)",
        },
        {
            "name": "resources",
            "columns": "(date_metric, resource_id, dataset_id, organization_id, nb_visit)",
        },
    ]
    for obj in config: 
        list_files = glob.glob(f"{TMP_FOLDER}outputs/{obj['name']}-*")
        for lf in list_files:
            if "-id-" not in lf and "-static-" not in lf:
                copy_file(
                    PG_HOST=conn.host,
                    PG_PORT=conn.port,
                    PG_DB=conn.schema,
                    PG_TABLE=f"metrics_{obj['name']}",
                    PG_USER=conn.login,
                    PG_PASSWORD=conn.password,
                    list_files=[
                        {
                            "source_path": "/".join(lf.split("/")[:-1])+"/",
                            "source_name": lf.split("/")[-1],
                            "column_order": obj["columns"],
                            "header": False
                        }
                    ],
                )

