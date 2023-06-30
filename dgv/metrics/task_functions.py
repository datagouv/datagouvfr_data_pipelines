
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from datetime import datetime, date, timedelta
import gzip
import glob
import os
import pandas as pd
import requests
from tqdm import tqdm

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
DB_METRICS_SCHEMA = Variable.get("DB_METRICS_SCHEMA", "airflow")
tqdm.pandas(desc='pandas progress bar', mininterval=5)


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


def remove_files_if_exists(folder):
    isExist = os.path.exists(f"{TMP_FOLDER}{folder}")
    if not isExist:
        os.makedirs(f"{TMP_FOLDER}{folder}")
    files = glob.glob(f"{TMP_FOLDER}{folder}/*")
    for f in files:
        os.remove(f)


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
    return datetime.strptime(a_date, "[%d/%b/%Y:%H:%M:%S.%f]").strftime("%Y-%m-%d")


def search_pattern(patterns, value, type_object):
    for pattern in patterns:
        if pattern in value:
            slug = value.replace(pattern, "").split("/")[0].replace(";", "")
            return slug, True, type_object
    return None, False, None


def search_pattern_resource_static(pattern, value, type_object):
    if pattern in value:
        slug = f"https://static.data.gouv.fr{value}".replace(";", "")
        return slug, True, type_object
    return None, False, None


def get_info(parsed_line):
    languages = ["fr", "en", "es"]
    patterns_datasets = [f"/{lang}/datasets/" for lang in languages]
    patterns_reuses = [f"/{lang}/reuses/" for lang in languages]
    patterns_organizations = [f"/{lang}/organizations/" for lang in languages]
    patterns_resources_id = [f"/{lang}/datasets/r/" for lang in languages]
    pattern_resources_static = "/resources/"
    slug_line = None
    found = False

    if "DATAGOUVFR_RGS~" in parsed_line:
        for item in parsed_line:
            slug, found, detect = search_pattern(patterns_resources_id, item, "resources-id")
            if not found:
                slug, found, detect = search_pattern(patterns_datasets, item, "datasets")
            if not found:
                slug, found, detect = search_pattern(patterns_reuses, item, "reuses")
            if not found:
                slug, found, detect = search_pattern(patterns_organizations, item, "organizations")
            if not found:
                slug, found, detect = search_pattern_resource_static(pattern_resources_static, item,
                                                                     "resources-static")
            if slug:
                slug_line = slug
                type_detect = detect
    if slug_line:
        return slug_line, type_detect
    else:
        return None, None


def save_list_obj_type(list_obj, obj_type):
    file_object = open(f"{TMP_FOLDER}found/found_{obj_type}.csv", "a")
    for item in list_obj:
        file_object.write(f"{item['date']};{item['id']}\n")


def save_list_obj(list_obj):
    list_resources_id = []
    list_resources_static = []
    list_datasets = []
    list_organizations = []
    list_reuses = []
    for obj in list_obj:
        if obj["type"] == "resources-id":
            list_resources_id.append(obj)
        if obj["type"] == "resources-static":
            list_resources_static.append(obj)
        if obj["type"] == "datasets":
            list_datasets.append(obj)
        if obj["type"] == "organizations":
            list_organizations.append(obj)
        if obj["type"] == "reuses":
            list_reuses.append(obj)
    save_list_obj_type(list_resources_id, "resources-id")
    save_list_obj_type(list_resources_static, "resources-static")
    save_list_obj_type(list_datasets, "datasets")
    save_list_obj_type(list_organizations, "organizations")
    save_list_obj_type(list_reuses, "reuses")


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


def parse(lines, date):
    list_obj = []
    for b_line in lines:
        try:
            slug_line = None
            parsed_line = b_line.decode("utf-8").split()
            slug_line, type_detect = get_info(parsed_line)
            if slug_line:
                list_obj.append({"type": type_detect, "id": slug_line, "date": date})
                if len(list_obj) == 10000:
                    save_list_obj(list_obj)
                    list_obj = []
        except:
            raise Exception(f"Sorry, pb with line: {b_line}")

    save_list_obj(list_obj)


def get_unique_dates(first_list, second_list):
    in_first = set(first_list)
    in_second = set(second_list)
    in_second_but_not_in_first = in_second - in_first
    result = first_list + list(in_second_but_not_in_first)
    return result


def process_log(ti):
    new_logs = ti.xcom_pull(key="new_logs", task_ids="get_new_logs")
    newlogs = [nl.replace("/new/", "/ongoing/") for nl in new_logs]
    all_dates_processed = []
    print("downloading files...")
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

    remove_files_if_exists("outputs")
    remove_files_if_exists("found")
    # analyser toutes les dates différentes
    alldates = set(d.split("-")[2] for d in newlogs)
    for log_date in alldates:
        print("---------------")
        print(log_date)
        lines = []
        for file_name in glob.glob(f"{TMP_FOLDER}/*{log_date}*.txt.gz"):
            with gzip.open(file_name, "rb") as log_data:
                lines += log_data.readlines()

        print("haproxy loaded")
        print("parse lines")
        parse(lines, datetime.datetime.strptime(log_date, '%d%m%Y').date())

        try:
            print("---- datasets -----")
            df_catalog = pd.read_csv(
                f"{TMP_FOLDER}catalog_datasets.csv",
                dtype=str,
                sep=";",
                usecols=["id", "slug", "organization_id"]
            )
            catalog_dict = get_dict(df_catalog, "slug")
            df = pd.read_csv(
                f"{TMP_FOLDER}found/found_datasets.csv",
                sep=";",
                dtype=str,
                header=None
            )
            df["id"] = df[1].apply(
                lambda x: catalog_dict[x] if x in catalog_dict else None
            )
            df = df.rename(columns={0: "date_metric"})
            df = df.drop(columns=[1])
            df["nb_visit"] = 1
            df = df.groupby(
                ["date_metric", "id"],
                as_index=False
            ).count().sort_values(
                by=["nb_visit"],
                ascending=False
            )
            df = pd.merge(df, df_catalog[["id", "organization_id"]], on="id", how="left")
            df = df.rename(columns={"id": "dataset_id"})
            df[["date_metric", "dataset_id", "organization_id", "nb_visit"]].to_csv(
                f"{TMP_FOLDER}outputs/datasets-{log_date}.csv", index=False, header=False
            )
            all_dates_processed = get_unique_dates(all_dates_processed, list(df["date_metric"].unique()))
        except pd.errors.EmptyDataError:
            print("empty data datasets")

        try:
            print("---- organizations -----")
            df_catalog = pd.read_csv(
                f"{TMP_FOLDER}catalog_organizations.csv",
                dtype=str,
                sep=";",
                usecols=["id", "slug"]
            )
            catalog_dict = get_dict(df_catalog, "slug")
            df = pd.read_csv(
                f"{TMP_FOLDER}found/found_organizations.csv",
                sep=";",
                dtype=str,
                header=None
            )
            df["id"] = df[1].apply(
                lambda x: catalog_dict[x] if x in catalog_dict else None
            )
            df = df.rename(columns={0: "date_metric"})
            df = df.drop(columns=[1])
            df["nb_visit"] = 1
            df = df.groupby(
                ["date_metric", "id"],
                as_index=False
            ).count().sort_values(
                by=["nb_visit"],
                ascending=False
            )
            df = pd.merge(df, df_catalog[["id"]], on="id", how="left")
            df = df.rename(columns={"id": "organization_id"})
            df[["date_metric", "organization_id", "nb_visit"]].to_csv(
                f"{TMP_FOLDER}outputs/organizations-{log_date}.csv", index=False, header=False
            )
            all_dates_processed = get_unique_dates(all_dates_processed, list(df["date_metric"].unique()))
        except pd.errors.EmptyDataError:
            print("empty data organizations")

        try:
            print("---- reuses -----")
            df_catalog = pd.read_csv(
                f"{TMP_FOLDER}catalog_reuses.csv",
                dtype=str,
                sep=";",
                usecols=["id", "slug", "organization_id"]
            )
            catalog_dict = get_dict(df_catalog, "slug")
            df = pd.read_csv(
                f"{TMP_FOLDER}found/found_reuses.csv",
                sep=";",
                dtype=str,
                header=None
            )
            df["id"] = df[1].apply(
                lambda x: catalog_dict[x] if x in catalog_dict else None
            )
            df = df.rename(columns={0: "date_metric"})
            df = df.drop(columns=[1])
            df["nb_visit"] = 1
            df = df.groupby(
                ["date_metric", "id"],
                as_index=False
            ).count().sort_values(
                by=["nb_visit"],
                ascending=False
            )
            df = pd.merge(df, df_catalog[["id", "organization_id"]], on="id", how="left")
            df = df.rename(columns={"id": "reuse_id"})
            df[["date_metric", "reuse_id", "organization_id", "nb_visit"]].to_csv(
                f"{TMP_FOLDER}outputs/reuses-{log_date}.csv", index=False, header=False
            )
            all_dates_processed = get_unique_dates(all_dates_processed, list(df["date_metric"].unique()))
        except pd.errors.EmptyDataError:
            print("empty data reuses")

        try:
            print("--- resources ----")
            df_catalog = pd.read_csv(
                f"{TMP_FOLDER}catalog_resources.csv",
                dtype=str,
                sep=";",
                usecols=["id", "url", "dataset.id", "dataset.organization_id"]
            )
            res1 = pd.read_csv(f"{TMP_FOLDER}found/found_resources-id.csv", dtype=str, header=None, sep=";")
            # remove resource when static
            res1 = res1.rename(columns={0: "date_metric", 1: "id"})
            res1 = pd.merge(res1, df_catalog[["id", "url"]], on="id", how="left")
            res1["is_static"] = res1["url"].apply(
                lambda x: True if "static.data.gouv.fr" in str(x) else False)
            print("shape", res1.shape[0])
            res1 = res1[res1["is_static"] is False]
            res1 = res1[["date_metric", "id"]]
            print("shape", res1.shape[0])

            res2 = pd.read_csv(f"{TMP_FOLDER}found/found_resources-static.csv",
                               dtype=str, header=None, sep=";")
            res2 = res2.rename(columns={0: "date_metric", 1: "url"})
            res2 = pd.merge(res2, df_catalog[["id", "url"]], on="url", how="left")
            res2 = res2[res2["id"].notna()][["date_metric", "id"]]

            resources = pd.concat([res1, res2])
            resources["nb_visit"] = 1
            resources = resources.groupby(["date_metric", "id"], as_index=False).count().sort_values(
                by=["nb_visit"], ascending=False)
            resources = pd.merge(resources, df_catalog[["id", "dataset.id", "dataset.organization_id"]],
                                 on="id", how="left")
            resources = resources.rename(columns={"id": "resource_id", "dataset.id": "dataset_id",
                                                  "dataset.organization_id": "organization_id"})
            resources = resources[["date_metric", "resource_id", "dataset_id", "organization_id",
                                   "nb_visit"]]
            resources.to_csv(f"{TMP_FOLDER}outputs/resources-{log_date}.csv", index=False, header=False)
            all_dates_processed = get_unique_dates(all_dates_processed, list(df["date_metric"].unique()))
        except FileNotFoundError:
            print("no data resources file")
        except pd.errors.EmptyDataError:
            print("empty data resources id or static")
    ti.xcom_push(key="all_dates_processed", value=all_dates_processed)


def get_matomo_outlinks(model, slug, target, metric_date):
    matomo_url = 'https://stats.data.gouv.fr/index.php'
    params = {
        "module": "API",
        "method": "Actions.getOutlinks",
        "actionType": "url",
        "segment": f"actionUrl==https://www.data.gouv.fr/fr/{model}/{slug}/",
        "format": "JSON",
        "token_auth": "anonymous",
        "idSite": 109,
        "period": "day",
        "date": metric_date.isoformat()
    }
    matomo_res = requests.get(matomo_url, params=params)
    matomo_res.raise_for_status()
    return sum(outlink['nb_hits'] for outlink in matomo_res.json() if outlink["label"] in target)


def sum_outlinks_by_orga(df_orga, df_outlinks, model):
    df_outlinks = df_outlinks.groupby('organization_id', as_index=False).sum()
    df_outlinks = df_outlinks.rename(columns={"outlinks": f"{model}_outlinks"})
    df_orga = pd.merge(df_orga, df_outlinks, on="organization_id", how="left").fillna(0)
    df_orga[f"{model}_outlinks"] = df_orga[f"{model}_outlinks"].astype(int)
    return df_orga


def process_matomo():
    '''
    Fetch matomo metrics for external links for datasets, reuses and sum these by orga
    '''
    if not os.path.exists(f'{TMP_FOLDER}matomo-outputs/'):
        os.makedirs(f'{TMP_FOLDER}matomo-outputs/')

    df_orga = pd.read_csv(
        f"{TMP_FOLDER}catalog_organizations.csv",
        dtype=str,
        sep=";",
        usecols=["id", "slug"]
    )
    df_orga = df_orga.rename(columns={"id": "organization_id"})

    # Which timespan to target?
    yesterday = date.today() - timedelta(days=1)
    for model in ['reuses']:  # datasets?
        print(f"get matamo outlinks for {model}")
        df_catalog = pd.read_csv(
            f"{TMP_FOLDER}catalog_{model}.csv",
            dtype=str,
            sep=";",
            usecols=["id", "slug", "remote_url", "organization_id"]
        )
        df_catalog['outlinks'] = df_catalog.progress_apply(
            lambda x: get_matomo_outlinks(model, x.slug, x.remote_url, yesterday), axis=1)
        df_catalog['date_metric'] = yesterday.isoformat()
        df_catalog.to_csv(f'{TMP_FOLDER}matomo-outputs/{model}-outlinks.csv',
                          columns=['date_metric', 'id', 'organization_id', 'outlinks'], index=False,
                          header=False)

        df_orga = sum_outlinks_by_orga(df_orga, df_catalog, model)

    df_orga['date_metric'] = yesterday.isoformat()
    df_orga = df_orga.rename(columns={"reuses_outlinks": "outlinks"})
    df_orga = df_orga.rename(columns={"organization_id": "id"})
    df_orga.to_csv(f'{TMP_FOLDER}matomo-outputs/organizations-outlinks.csv',
                   columns=['date_metric', 'id', 'outlinks'], index=False, header=False)
    print("Done")


def save_metrics_to_postgres(ti):
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
        for lf in glob.glob(f"{TMP_FOLDER}outputs/{obj['name']}-*"):
            if "-id-" not in lf and "-static-" not in lf:
                copy_file(
                    PG_HOST=conn.host,
                    PG_PORT=conn.port,
                    PG_DB=conn.schema,
                    PG_TABLE=f"{DB_METRICS_SCHEMA}.visits_{obj['name']}",
                    PG_USER=conn.login,
                    PG_PASSWORD=conn.password,
                    list_files=[
                        {
                            "source_path": "/".join(lf.split("/")[:-1]) + "/",
                            "source_name": lf.split("/")[-1],
                            "column_order": obj["columns"],
                        }
                    ],
                    has_header=False
                )


def save_matomo_to_postgres():
    config = [
        {
            "name": "reuses",
            "columns": "(date_metric, reuse_id, organization_id, nb_outlink)",
        },
        {
            "name": "organizations",
            "columns": "(date_metric, organization_id, nb_outlink)",
        },
    ]
    for obj in config:
        for lf in glob.glob(f"{TMP_FOLDER}matomo-outputs/{obj['name']}-*"):
            copy_file(
                PG_HOST=conn.host,
                PG_PORT=conn.port,
                PG_DB=conn.schema,
                PG_TABLE=f"{DB_METRICS_SCHEMA}.matomo_{obj['name']}",
                PG_USER=conn.login,
                PG_PASSWORD=conn.password,
                list_files=[
                    {
                        "source_path": "/".join(lf.split("/")[:-1]) + "/",
                        "source_name": lf.split("/")[-1],
                        "column_order": obj["columns"],
                    }
                ],
                has_header=False
            )
