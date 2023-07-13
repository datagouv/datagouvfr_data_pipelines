from datagouvfr_data_pipelines.utils.datagouv import (
    get_all_from_api_query,
    DATAGOUV_URL
)
from datagouvfr_data_pipelines.utils.minio import send_files
from datagouvfr_data_pipelines.utils.mattermost import send_message
from typing import List, Optional, Dict
import pandas as pd
import numpy as np
import requests
import json
from json import JSONDecodeError
import os
import yaml
from datetime import datetime
import time
from pathlib import Path
import chardet
import pickle
import emails
import shutil

# for local dev in order not to mess up with production
# DATAGOUV_URL = 'https://data.gouv.fr'
# DATAGOUV_SECRET_API_KEY = 'non'

VALIDATA_BASE_URL = (
    "https://validata-api.app.etalab.studio/validate?schema={schema_url}&url={rurl}"
)
MINIMUM_VALID_RESOURCES_TO_CONSOLIDATE = 5
api_url = f"{DATAGOUV_URL}/api/1/"
schema_url_base = api_url + "datasets/?schema={schema_name}"
tag_url_base = api_url + "datasets/?tag={tag}"
search_url_base = api_url + "datasets/?q={search_word}"


def remove_old_schemas(
    config_dict,
    schemas_catalogue_list,
    single_schema=False
):
    schemas_list = [schema["name"] for schema in schemas_catalogue_list]
    mydict = {}
    # Remove old schemas still in yaml
    for schema_name in config_dict.keys():
        if schema_name in schemas_list:
            mydict[schema_name] = config_dict[schema_name]
        else:
            if not single_schema:
                print(schema_name, "- Remove old schema not anymore in catalogue")
    return mydict


def get_schema_dict(
    schema_name: str, schemas_catalogue_list: List[dict]
) -> Optional[dict]:
    """Get the dictionnary with information on the schema (when schemas catalogue list already loaded)"""
    for schema in schemas_catalogue_list:
        if schema["name"] == schema_name:
            return schema
    print(f"No schema named '{schema_name}' found.")
    return None


def add_most_recent_valid_version(df_ref: pd.DataFrame) -> pd.DataFrame:
    """Based on validation columns by version, adds a column to the ref_table that shows the most recent version of the schema for which the resource is valid"""
    version_cols_list = [col for col in df_ref.columns if col.startswith("is_valid_v_")]

    df_ref["most_recent_valid_version"] = ""

    for col in sorted(version_cols_list, reverse=True):
        df_ref.loc[
            (df_ref["most_recent_valid_version"] == ""),
            "most_recent_valid_version",
        ] = df_ref.loc[(df_ref["most_recent_valid_version"] == ""), col].apply(
            lambda x: x * col.replace("is_valid_v_", "")
        )

    df_ref.loc[
        (df_ref["most_recent_valid_version"] == ""),
        "most_recent_valid_version",
    ] = np.nan

    return df_ref


# Add the schema default configuration in the configuration YAML file
def add_schema_default_config(
    schema_name: str, config_path: str, schemas_catalogue_list: List[dict]
) -> None:
    schema_dict = get_schema_dict(schema_name, schemas_catalogue_list)
    schema_title = schema_dict["title"]

    default_schema_config_dict = {
        "consolidate": False,
        "search_words": [
            schema_title
        ],  # setting schema title as a default search keyword for resources
    }

    if os.path.exists(config_path):
        with open(config_path, "r") as infile:
            config_dict = yaml.safe_load(infile)

    else:
        config_dict = {}

    config_dict[schema_name] = default_schema_config_dict

    with open(config_path, "w") as outfile:
        yaml.dump(config_dict, outfile, default_flow_style=False)


# API parsing to get resources infos based on schema metadata, tags and search keywords
def parse_api(url: str, api_url: str, schema_name: str) -> pd.DataFrame:
    all_datasets = get_all_from_api_query(url)
    arr = []
    # when using api/2, the resources are not directly accessible, so we use api/1 to get them
    if 'api/2' in url:
        all_datasets = [requests.get(api_url + "datasets/" + d["id"]).json() for d in all_datasets]
    for dataset in all_datasets:
        for res in dataset["resources"]:
            if res["schema"].get("name", "") == schema_name:
                if "format=csv" in res["url"]:
                    filename = res["url"].split("/")[-3] + ".csv"
                else:
                    filename = res["url"].split("/")[-1]
                ext = filename.split(".")[-1]
                detected_mime = res.get("extras", {}).get("check:headers:content-type", "").split(";")[0].strip()
                obj = {}
                obj["dataset_id"] = dataset["id"]
                obj["dataset_title"] = dataset["title"]
                obj["dataset_slug"] = dataset["slug"]
                obj["dataset_page"] = dataset["page"]
                obj["resource_id"] = res["id"]
                obj["resource_title"] = res["title"]
                obj["resource_url"] = res["url"]
                obj["resource_last_modified"] = res["last_modified"]
                appropriate_extension = ext in ["csv", "xls", "xlsx"]
                mime_dict = {
                    "text/csv": "csv",
                    "application/vnd.ms-excel": "xls"
                }
                appropriate_mime = detected_mime in mime_dict.keys()
                if appropriate_extension:
                    obj["resource_extension"] = ext
                elif appropriate_mime:
                    obj["resource_extension"] = mime_dict[detected_mime]
                else:
                    obj["resource_extension"] = ext
                if not (appropriate_extension or appropriate_mime):
                    obj["error_type"] = "wrong-file-format"
                else:
                    if not dataset["organization"] and not dataset["owner"]:
                        obj["error_type"] = "orphan-dataset"
                    else:
                        obj["organization_or_owner"] = (
                            dataset["organization"]["slug"]
                            if dataset["organization"]
                            else dataset["owner"]["slug"]
                        )
                        obj["error_type"] = None
                arr.append(obj)
    df = pd.DataFrame(arr)
    return df


# Make the validation report based on the resource url, schema url and validation url
def make_validata_report(rurl, schema_url, validata_base_url=VALIDATA_BASE_URL):
    r = requests.get(validata_base_url.format(schema_url=schema_url, rurl=rurl))
    time.sleep(0.5)
    return r.json()


# Returns if a resource is valid or not regarding a schema (version)
def is_validata_valid(rurl, schema_url, validata_base_url=VALIDATA_BASE_URL):
    try:
        report = make_validata_report(rurl, schema_url, validata_base_url)
        try:
            res = report["report"]["valid"]
        except:
            print(
                "{} ---- 🔴 No info in validata report for resource: {}".format(
                    datetime.today(), rurl
                )
            )
            res = False
            report = None
    except JSONDecodeError:
        print(
            "{} ---- 🔴 Could not make JSON from validata report for resource: {}".format(
                datetime.today(), rurl
            )
        )
        res = False
        report = None
    return res, report


def save_validata_report(
    res,
    report,
    version,
    schema_name,
    dataset_id,
    resource_id,
    validata_reports_path,
):
    save_report = {}
    save_report["validation-report:schema_name"] = schema_name
    save_report["validation-report:schema_version"] = version
    save_report["validation-report:schema_type"] = "tableschema"
    save_report["validation-report:validator"] = "validata"
    save_report["validation-report:valid_resource"] = res
    try:
        nb_errors = (
            report["report"]["stats"]["errors"]
            if report["report"]["stats"]["errors"] < 100
            else 100
        )
    except:
        nb_errors = None
    save_report["validation-report:nb_errors"] = nb_errors
    try:
        keys = "cells"
        errors = [
            {y: x[y] for y in x if y not in keys}
            for x in report["report"]["tasks"][0]["errors"][:100]
        ]

    except:
        errors = None
    save_report["validation-report:errors"] = errors
    save_report["validation-report:validation_date"] = str(datetime.now())

    with open(
        str(validata_reports_path)
        + "/"
        + schema_name.replace("/", "_")
        + "_"
        + dataset_id
        + "_"
        + resource_id
        + "_"
        + version
        + ".json",
        "w",
    ) as f:
        json.dump(save_report, f)


# Returns if a resource is valid based on its "ref_table" row
def is_validata_valid_row(row, schema_url, version, schema_name, validata_reports_path):
    if row["error_type"] is None:  # if no error
        rurl = row["resource_url"]
        res, report = is_validata_valid(rurl, schema_url)
        if report:
            save_validata_report(
                res,
                report,
                version,
                schema_name,
                row["dataset_id"],
                row["resource_id"],
                validata_reports_path,
            )
        return res
    else:
        return False


# Gets the current metadata of schema version of a resource (based of ref_table row)
def get_resource_schema_version(row: pd.Series, api_url: str):
    dataset_id = row["dataset_id"]
    resource_id = row["resource_id"]

    url = api_url + "datasets/{}/resources/{}/".format(dataset_id, resource_id)
    r = requests.get(url)
    if r.status_code == 200:
        r_json = r.json()
        if "schema" in r_json.keys():
            if "version" in r_json["schema"].keys():
                return r_json["schema"]["version"]
            else:
                return np.nan
        else:
            return np.nan
    else:
        return np.nan


def get_schema_report(
    schemas_catalogue_url,
    config_path,
    schema_name=None,
    list_schema_skip=[]
):
    """
    For single schema processing (e.g IRVE): specify schema_name as string
    For general case: specify list_schema_skip as a list of schemas to ignore
    """

    # always specify one of these two, depending on the case
    assert schema_name or len(list_schema_skip) > 1

    schemas_report_dict = {}

    schemas_catalogue_dict = requests.get(schemas_catalogue_url).json()
    print("Schema catalogue URL:", schemas_catalogue_dict["$schema"])
    print("Version:", schemas_catalogue_dict["version"])

    if not schema_name:
        schemas_catalogue_list = [
            schema
            for schema in schemas_catalogue_dict["schemas"]
            if schema["schema_type"] == "tableschema"
            and schema["name"] not in list_schema_skip
        ]
        print("Total number of schemas:", len(schemas_catalogue_list))

        for schema in schemas_catalogue_list:
            print("- {} ({} versions)".format(schema["name"], len(schema["versions"])))
            schemas_report_dict[schema["name"]] = {"nb_versions": len(schema["versions"])}

        # Creating/updating config file with missing schemas
        if os.path.exists(config_path):
            with open(config_path, "r") as f:
                config_dict = yaml.safe_load(f)
            config_dict = remove_old_schemas(config_dict, schemas_catalogue_list)
        else:
            config_dict = {}

    else:
        schemas_catalogue_list = [
            schema
            for schema in schemas_catalogue_dict["schemas"]
            if schema["schema_type"] == "tableschema"
            and schema["name"] == schema_name
        ]
        print(
            "- {} ({} versions)".format(schemas_catalogue_list[0]["name"],
            len(schemas_catalogue_list[0]["versions"]))
        )
        schemas_report_dict[schema_name] = {"nb_versions": len(schemas_catalogue_list[0]["versions"])}
        # Creating/updating config file with missing schemas
        if os.path.exists(config_path):
            with open(config_path, "r") as f:
                config_dict = yaml.safe_load(f)
            config_dict = remove_old_schemas(
                config_dict,
                schemas_catalogue_list,
                single_schema=True
            )
        else:
            config_dict = {}

    for schema in schemas_catalogue_list:
        if schema["name"] not in config_dict.keys():
            add_schema_default_config(
                schema["name"], config_path, schemas_catalogue_list
            )
            schemas_report_dict[schema["name"]]["new_config_created"] = True
            print(
                "{} - ➕ Schema {} added to config file.".format(
                    datetime.today(), schema["name"]
                )
            )
        else:
            schemas_report_dict[schema["name"]]["new_config_created"] = False
            print(
                "{} - 🆗 Schema {} already in config file.".format(
                    datetime.today(), schema["name"]
                )
            )
    return schemas_report_dict, schemas_catalogue_list



def build_reference_table(
    config_dict,
    schema_name,
    schemas_catalogue_list,
    schemas_report_dict,
    validata_reports_path,
    ref_tables_path,
    should_succeed=False
):
    print("{} - ℹ️ STARTING SCHEMA: {}".format(datetime.now(), schema_name))

    schema_config = config_dict[schema_name]

    if schema_config["consolidate"]:
        # Schema official specification (in catalogue)
        schema_dict = get_schema_dict(schema_name, schemas_catalogue_list)

        # Datasets to exclude (from config)
        datasets_to_exclude = []
        if "consolidated_dataset_id" in schema_config.keys():
            datasets_to_exclude += [schema_config["consolidated_dataset_id"]]
        if "exclude_dataset_ids" in schema_config.keys():
            if type(schema_config["exclude_dataset_ids"]) == list:
                datasets_to_exclude += schema_config["exclude_dataset_ids"]

        # Tags and search words to use to get resources that could match schema (from config)
        tags_list = []
        if "tags" in schema_config.keys():
            tags_list += schema_config["tags"]

        search_words_list = []
        if "search_words" in schema_config.keys():
            search_words_list = schema_config["search_words"]

        # Schema versions not to consolidate
        drop_versions = []
        if "drop_versions" in schema_config.keys():
            drop_versions += schema_config["drop_versions"]

        schemas_report_dict[schema_name]["nb_versions_to_drop_in_config"] = len(
            drop_versions
        )

        # PARSING API TO GET ALL ELIGIBLE RESOURCES FOR CONSOLIDATION

        df_list = []

        # Listing resources by schema request
        df_schema = parse_api(
            schema_url_base.format(schema_name=schema_name),
            api_url,
            schema_name
        )
        schemas_report_dict[schema_name]["nb_resources_found_by_schema"] = len(
            df_schema
        )
        if len(df_schema) > 0:
            df_schema["resource_found_by"] = "1 - schema request"
            df_schema["initial_version_name"] = df_schema.apply(
                lambda row: get_resource_schema_version(row, api_url),
                axis=1,
            )
            df_list += [df_schema]

        # Listing resources by tag requests
        schemas_report_dict[schema_name]["nb_resources_found_by_tags"] = 0
        for tag in tags_list:
            df_tag = parse_api(
                tag_url_base.format(tag=tag),
                api_url,
                schema_name
            )
            schemas_report_dict[schema_name]["nb_resources_found_by_tags"] += len(
                df_tag
            )
            if len(df_tag) > 0:
                df_tag["resource_found_by"] = "2 - tag request"
                df_list += [df_tag]

        # Listing resources by search (keywords) requests
        schemas_report_dict[schema_name]["nb_resources_found_by_search_words"] = 0
        for search_word in search_words_list:
            df_search_word = parse_api(
                search_url_base.format(search_word=search_word),
                api_url,
                schema_name
            )
            schemas_report_dict[schema_name][
                "nb_resources_found_by_search_words"
            ] += len(df_search_word)
            if len(df_search_word) > 0:
                df_search_word["resource_found_by"] = "3 - search request"
                df_list += [df_search_word]

        if len(df_list) > 0:
            df = pd.concat(df_list, ignore_index=True)
            df = df[~(df["dataset_id"].isin(datasets_to_exclude))]
            df = df.sort_values("resource_found_by")
            df = df.drop_duplicates(subset=["resource_id"], keep="first")

            print(
                "{} -- 🔢 {} resource(s) found for this schema.".format(
                    datetime.now(), len(df)
                )
            )

            if (
                "initial_version_name" not in df.columns
            ):  # in case there is no resource found by schema request
                df["initial_version_name"] = np.nan

            # FOR EACH RESOURCE AND SCHEMA VERSION, CHECK IF RESOURCE MATCHES THE SCHEMA VERSION

            # Apply validata check for each version that is not explicitly dropped in config file
            version_names_list = []

            for version in schema_dict["versions"]:
                version_name = version["version_name"]
                if version_name not in drop_versions:
                    schema_url = version["schema_url"]
                    df["is_valid_v_{}".format(version_name)] = df.apply(
                        lambda row: is_validata_valid_row(
                            row,
                            schema_url,
                            version_name,
                            schema_name,
                            validata_reports_path,
                        ),
                        axis=1,
                    )
                    version_names_list += [version_name]
                    print(
                        "{} --- ☑️ Validata check done for version {}".format(
                            datetime.now(), version_name
                        )
                    )
                else:
                    print(
                        "{} --- ❌ Version {} to drop according to config file".format(
                            datetime.now(), version_name
                        )
                    )

            if len(version_names_list) > 0:
                # Check if resources are at least matching one schema version (only those matching will be downloaded in next step)
                df["is_valid_one_version"] = (
                    sum(
                        [
                            df["is_valid_v_{}".format(version_name)]
                            for version_name in version_names_list
                        ]
                    )
                    > 0
                )
                schemas_report_dict[schema_name]["nb_valid_resources"] = df[
                    "is_valid_one_version"
                ].sum()
                df = add_most_recent_valid_version(df)
                df.to_csv(
                    os.path.join(
                        ref_tables_path,
                        "ref_table_{}.csv".format(schema_name.replace("/", "_")),
                    ),
                    index=False,
                )
                print(
                    "{} -- ✅ Validata check done for {}.".format(
                        datetime.now(), schema_name
                    )
                )

            else:
                schemas_report_dict[schema_name]["nb_valid_resources"] = 0
                print(
                    "{} -- ❌ All possible versions for this schema were dropped by config file.".format(
                        datetime.now()
                    )
                )
                if should_succeed:
                    return False

        else:
            print(
                "{} -- ⚠️ No resource found for {}.".format(
                    datetime.now(), schema_name
                )
            )
            if should_succeed:
                return False

    else:
        print(
            "{} -- ❌ Schema not to consolidate according to config file.".format(
                datetime.now()
            )
        )
        if should_succeed:
            return False

    return True


def download_schema_files(
    schema_name,
    ref_tables_path,
    data_path,
    should_succeed=False
):
    print("{} - ℹ️ STARTING SCHEMA: {}".format(datetime.now(), schema_name))

    ref_table_path = os.path.join(
        ref_tables_path,
        "ref_table_{}.csv".format(schema_name.replace("/", "_")),
    )

    if os.path.exists(ref_table_path):
        df_ref = pd.read_csv(ref_table_path)
        df_ref["is_downloaded"] = False

        if len(df_ref[df_ref["is_valid_one_version"]]) > 0:
            schema_data_path = Path(data_path) / schema_name.replace("/", "_")
            schema_data_path.mkdir(exist_ok=True)

            for index, row in df_ref[
                df_ref["is_valid_one_version"]
            ].iterrows():
                rurl = row["resource_url"]
                r = requests.get(rurl, allow_redirects=True)

                if r.status_code == 200:
                    p = Path(schema_data_path) / row["dataset_slug"]
                    p.mkdir(exist_ok=True)
                    file_extension = row["resource_extension"]
                    written_filename = f"{row['resource_id']}.{file_extension}"

                    with open("{}/{}".format(p, written_filename), "wb") as f:
                        f.write(r.content)

                    df_ref.loc[
                        (df_ref["resource_id"] == row["resource_id"]),
                        "is_downloaded",
                    ] = True

                    print(
                        "{} --- ⬇️✅ downloaded file [{}] {}".format(
                            datetime.now(), row["resource_title"], rurl
                        )
                    )
                else:
                    print(
                        "{} --- ⬇️❌ File could not be downloaded: [{}] {}".format(
                            datetime.now(), row["resource_title"], rurl
                        )
                    )

        else:
            print(
                "{} -- ⚠️ No valid resource for this schema".format(datetime.now())
            )
            if should_succeed:
                return False

        if should_succeed and len(df_ref) == 0:
            return False
        df_ref.to_csv(ref_table_path, index=False)

    else:
        print(
            "{} -- ❌ No reference table made for this schema (schema not to consolidate, no version to consolidate or no resource found).".format(
                datetime.now()
            )
        )
        if should_succeed:
            return False

    return True


def consolidate_data(
    data_path,
    schema_name,
    consolidated_data_path,
    ref_tables_path,
    schemas_catalogue_list,
    consolidation_date_str,
    tmp_path,
    schemas_report_dict,
    should_succeed=False
):
    print("{} - ℹ️ STARTING SCHEMA: {}".format(datetime.now(), schema_name))

    schema_data_path = Path(data_path) / schema_name.replace("/", "_")

    if os.path.exists(schema_data_path):
        schema_consolidated_data_path = Path(
            consolidated_data_path
        ) / schema_name.replace("/", "_")
        schema_consolidated_data_path.mkdir(exist_ok=True)

        ref_table_path = os.path.join(
            ref_tables_path,
            "ref_table_{}.csv".format(schema_name.replace("/", "_")),
        )
        df_ref = pd.read_csv(
            ref_table_path
        )  # (This file necessarily exists if data folder exists)

        # We will test if downloaded files are empty or not (so we set default values)
        df_ref["is_empty"] = np.nan
        df_ref.loc[(df_ref["is_downloaded"]), "is_empty"] = False

        schema_dict = get_schema_dict(schema_name, schemas_catalogue_list)

        version_names_list = [
            col.replace("is_valid_v_", "")
            for col in df_ref.columns
            if col.startswith("is_valid_v_")
        ]

        for version in schema_dict["versions"]:
            version_name = version["version_name"]
            if version_name in version_names_list:
                df_ref_v = df_ref[
                    (df_ref["is_valid_v_" + version_name])
                    & (df_ref["is_downloaded"])
                ]

                if len(df_ref_v) > 0:
                    # Get schema version parameters for ddup
                    version_dict = requests.get(version["schema_url"]).json()
                    version_all_cols_list = [
                        field_dict["name"] for field_dict in version_dict["fields"]
                    ]
                    version_required_cols_list = [
                        field_dict["name"] for field_dict in version_dict["fields"]
                        if field_dict.get("constraints", {}).get("required")
                    ]

                    if "primaryKey" in version_dict.keys():
                        primary_key = version_dict["primaryKey"]
                    else:
                        primary_key = None

                    df_r_list = []

                    for index, row in df_ref_v.iterrows():
                        file_extension = row["resource_extension"]
                        file_path = os.path.join(
                            schema_data_path,
                            row["dataset_slug"],
                            f"{row['resource_id']}.{file_extension}",
                        )

                        try:
                            if file_path.endswith(".csv"):
                                with open(file_path, "rb") as f:
                                    encoding = chardet.detect(f.read()).get(
                                        "encoding"
                                    )
                                if encoding == "Windows-1254":
                                    encoding = "iso-8859-1"

                                df_r = pd.read_csv(
                                    file_path,
                                    sep=None,
                                    engine="python",
                                    dtype="str",
                                    encoding=encoding,
                                    na_filter=False,
                                    keep_default_na=False,
                                )
                            else:
                                df_r = pd.read_excel(
                                    file_path,
                                    dtype="str",
                                    na_filter=False,
                                    keep_default_na=False,
                                    engine="openpyxl",
                                )

                            # Remove potential blanks in column names
                            df_r.columns = [c.replace(' ', '') for c in df_r.columns]
                            # Remove potential unwanted characters
                            # (eg https://www.data.gouv.fr/fr/datasets/r/67ed303d-1b3a-49d1-afb4-6c0e4318cc20)
                            for c in df_r.columns:
                                df_r[c] = df_r[c].apply(
                                    lambda s: s.replace('\n', '').replace('\r', '')
                                    if isinstance(s, str) else s
                                )
                            if len(df_r) > 0:  # Keeping only non empty files
                                # Discard columns that are not in the current schema version
                                df_r = df_r[
                                    [
                                        col
                                        for col in version_all_cols_list
                                        if col in df_r.columns
                                    ]
                                ]
                                # Assert all required columns are in the file
                                # Add optional columns to fit the schema
                                # /!\ THIS REQUIRES THAT COLUMNS CONSTRAINTS ARE PROPERLY SET UPSTREAM
                                if all(
                                    [rq_col in df_r.columns for rq_col in version_required_cols_list]
                                ):
                                    for col in version_all_cols_list:
                                        if col not in df_r.columns:
                                            df_r[col] = np.nan
                                    df_r["last_modified"] = row[
                                        "resource_last_modified"
                                    ]
                                    df_r["datagouv_dataset_id"] = row["dataset_id"]
                                    df_r["datagouv_resource_id"] = row["resource_id"]
                                    df_r["datagouv_organization_or_owner"] = row[
                                        "organization_or_owner"
                                    ]
                                    # Discard rows where any of the required columns is empty
                                    # (NaN or empty string)
                                    df_r = df_r.loc[
                                        (~(df_r[version_required_cols_list].isna().any(axis=1)))
                                        & (~((df_r[version_required_cols_list] == '').any(axis=1)))
                                    ]
                                    df_r_list += [df_r]
                                else:
                                    print('This file is missing required columns: ', file_path)
                                    df_ref.loc[
                                        (df_ref["resource_id"] == row["resource_id"]),
                                        "columns_issue",
                                    ] = True
                            else:
                                df_ref.loc[
                                    (df_ref["resource_id"] == row["resource_id"]),
                                    "is_empty",
                                ] = True
                        except:
                            print("Pb on reading resource - {}".format(file_path))

                    if len(df_r_list) >= MINIMUM_VALID_RESOURCES_TO_CONSOLIDATE:
                        df_conso = pd.concat(df_r_list, ignore_index=True)

                        # Sorting by most recent (resource last modification date at the moment)
                        df_conso = df_conso.sort_values(
                            "last_modified", ascending=False
                        )

                        # Deduplication
                        if primary_key is not None:
                            ddup_cols = primary_key
                        else:
                            ddup_cols = version_all_cols_list

                        df_conso = df_conso.drop_duplicates(
                            ddup_cols, keep="first"
                        ).reset_index(drop=True)

                        df_conso.to_csv(
                            os.path.join(
                                schema_consolidated_data_path,
                                "consolidation_{}_v_{}_{}.csv".format(
                                    schema_name.replace("/", "_"),
                                    version_name,
                                    consolidation_date_str,
                                ),
                            ),
                            index=False,
                            encoding="utf-8",
                        )
                        print(
                            "{} -- ✅ DONE: {} version {}".format(
                                datetime.today(), schema_name, version_name
                            )
                        )

                    else:
                        print(
                            "{} -- ⚠️ Less than {} (non-empty) valid resources for version {} : consolidation file is not built".format(
                                datetime.today(),
                                MINIMUM_VALID_RESOURCES_TO_CONSOLIDATE,
                                version_name
                            )
                        )
                        if should_succeed:
                            return False

                else:
                    print(
                        "{} -- ⚠️ No valid resource for version {} of this schema".format(
                            datetime.today(), version_name
                        )
                    )
                    if should_succeed:
                        return False

        if should_succeed and len(df_ref) == 0:
            return False
        df_ref.to_csv(ref_table_path, index=False)

    else:
        print(
            "{} -- ❌ No data downloaded for this schema.".format(datetime.today())
        )
        if should_succeed:
            return False

    with open(tmp_path / "schemas_report_dict.pickle", "wb") as f:
        pickle.dump(schemas_report_dict, f, pickle.HIGHEST_PROTOCOL)

    return True


# Creates a dataset on data.gouv.fr for consolidation files (used only if does not exist yet in config file)
def create_schema_consolidation_dataset(
    schema_name, schemas_catalogue_list, api_url, headers
):
    global datasets_description_template, datasets_title_template

    schema_title = get_schema_dict(schema_name, schemas_catalogue_list)["title"]

    response = requests.post(
        api_url + "datasets/",
        json={
            "title": datasets_title_template.format(schema_title=schema_title),
            "description": datasets_description_template.format(
                schema_name=schema_name
            ),
            "organization": "534fff75a3a7292c64a77de4",
            "license": "lov2",
        },
        headers=headers,
    )

    return response


# Generic function to update a field (key) in the config file
def update_config_file(schema_name, key, value, config_path):
    with open(config_path, "r") as f:
        config_dict = yaml.safe_load(f)

    config_dict[schema_name][key] = value

    with open(config_path, "w") as outfile:
        yaml.dump(config_dict, outfile, default_flow_style=False)


# Adds the resource ID of the consolidated file for a given schema version in the config file
def update_config_version_resource_id(schema_name, version_name, r_id, config_path):
    with open(config_path, "r") as f:
        config_dict = yaml.safe_load(f)

    if "latest_resource_ids" not in config_dict[schema_name]:
        config_dict[schema_name]["latest_resource_ids"] = {version_name: r_id}
    else:
        config_dict[schema_name]["latest_resource_ids"][version_name] = r_id

    with open(config_path, "w") as outfile:
        yaml.dump(config_dict, outfile, default_flow_style=False)


# Returns if resource schema (version) metadata should be updated or not based on what we know about the resource
def is_schema_version_to_update(row):
    initial_version_name = row["initial_version_name"]
    most_recent_valid_version = row["most_recent_valid_version"]
    resource_found_by = row["resource_found_by"]

    return (
        (resource_found_by == "1 - schema request")
        and (most_recent_valid_version == most_recent_valid_version)
        and (initial_version_name != most_recent_valid_version)
    )


# Returns if resource schema (version) metadata should be added or not based on what we know about the resource
def is_schema_to_add(row):
    resource_found_by = row["resource_found_by"]
    is_valid_one_version = row["is_valid_one_version"]

    return (resource_found_by != "1 - schema request") and is_valid_one_version


# Returns if resource schema (version) metadata should be deleted or not based on what we know about the resource
def is_schema_to_drop(row):
    resource_found_by = row["resource_found_by"]
    is_valid_one_version = row["is_valid_one_version"]

    return (resource_found_by == "1 - schema request") and (
        is_valid_one_version == False
    )


# Function that adds a schema (version) metadata on a resource
def add_resource_schema(
    api_url: str,
    dataset_id: str,
    resource_id: str,
    schema_name: str,
    version_name: str,
    headers: Dict[str, str],
) -> bool:
    schema = {"name": schema_name, "version": version_name}

    try:
        url = api_url + "datasets/{}/resources/{}/".format(dataset_id, resource_id)
        r = requests.get(url, headers=headers)
        extras = r.json()["extras"]
    except:
        extras = {}

    extras["consolidation_schema:add_schema"] = schema_name

    obj = {"schema": schema, "extras": extras}

    url = api_url + "datasets/{}/resources/{}/".format(dataset_id, resource_id)
    response = requests.put(url, json=obj, headers=headers)

    if response.status_code != 200:
        print(
            "🔴 Schema could not be added on resource. Dataset ID: {} - Resource ID: {}".format(
                dataset_id, resource_id
            )
        )

    return response.status_code == 200


# Function that updates a schema (version) metadata on a resource
def update_resource_schema(
    api_url: str,
    dataset_id: str,
    resource_id: str,
    schema_name: str,
    version_name: str,
    headers: Dict[str, str],
) -> bool:
    schema = {"name": schema_name, "version": version_name}

    try:
        url = api_url + "datasets/{}/resources/{}/".format(dataset_id, resource_id)
        r = requests.get(url, headers=headers)
        extras = r.json()["extras"]
    except:
        extras = {}

    extras["consolidation_schema:update_schema"] = schema_name

    obj = {"schema": schema, "extras": extras}

    url = api_url + "datasets/{}/resources/{}/".format(dataset_id, resource_id)
    response = requests.put(url, json=obj, headers=headers)

    if response.status_code != 200:
        print(
            "🔴 Resource schema could not be updated. Dataset ID: {} - Resource ID: {}".format(
                dataset_id, resource_id
            )
        )

    return response.status_code == 200


# Function that deletes a schema (version) metadata on a resource
def delete_resource_schema(
    api_url: str,
    dataset_id: str,
    resource_id: str,
    schema_name: str,
    headers: Dict[str, str],
) -> bool:
    schema = {}

    try:
        url = api_url + "datasets/{}/resources/{}/".format(dataset_id, resource_id)
        r = requests.get(url, headers=headers)
        extras = r.json()["extras"]
    except:
        extras = {}

    extras["consolidation_schema:remove_schema"] = schema_name

    obj = {"schema": schema, "extras": extras}

    url = api_url + "datasets/{}/resources/{}/".format(dataset_id, resource_id)
    response = requests.put(url, json=obj, headers=headers)

    if response.status_code != 200:
        print(
            "🔴 Resource schema could not be deleted. Dataset ID: {} - Resource ID: {}".format(
                dataset_id, resource_id
            )
        )

    return response.status_code == 200


# Get the (list of) e-mail address(es) of the owner or of the admin(s) of the owner organization of a dataset
def get_owner_or_admin_mails(dataset_id, api_url):
    r = requests.get(api_url + "datasets/{}/".format(dataset_id))
    r_dict = r.json()

    if r_dict["organization"] is not None:
        org_id = r_dict["organization"]["id"]
    else:
        org_id = None

    if r_dict["owner"] is not None:
        owner_id = r_dict["owner"]["id"]
    else:
        owner_id = None

    mails_type = None
    mails_list = []

    if org_id is not None:
        mails_type = "organisation_admins"
        r_org = requests.get(api_url + "organizations/{}/".format(org_id))
        members_list = r_org.json()["members"]
        for member in members_list:
            if member["role"] == "admin":
                user_id = member["user"]["id"]
                r_user = requests.get(
                    api_url + "users/{}/".format(user_id), headers=HEADER
                )
                user_mail = r_user.json()["email"]
                mails_list += [user_mail]

    else:
        if owner_id is not None:
            mails_type = "owner"
            r_user = requests.get(
                api_url + "users/{}/".format(owner_id), headers=HEADER
            )
            user_mail = r_user.json()["email"]
            mails_list += [user_mail]

    return (mails_type, mails_list)


# Function to send a e-mail
def send_email(
    subject, message, mail_from, mail_to, smtp_host, smtp_user, smtp_password
):
    message = emails.html(
        html="<p>%s</p>" % message, subject=subject, mail_from=mail_from
    )
    smtp = {
        "host": smtp_host,
        "port": 587,
        "tls": True,
        "user": smtp_user,
        "password": smtp_password,
    }

    _ = message.send(to=mail_to, smtp=smtp)

    return _


# Function to post a comment on a dataset
def post_comment_on_dataset(dataset_id, title, comment, api_url):
    global HEADER

    post_object = {
        "title": title,
        "comment": comment,
        "subject": {"class": "Dataset", "id": dataset_id},
    }

    _ = requests.post(api_url + "discussions/", json=post_object, headers=HEADER)

    return _


def add_validation_extras(
    dataset_id,
    resource_id,
    validata_report_path,
    api_url,
    headers,
    schema_name,
    should_succeed
):
    if os.path.isfile(validata_report_path):
        with open(validata_report_path) as out:
            validation_report = json.load(out)

        try:
            url = api_url + f"datasets/{dataset_id}/resources/{resource_id}/"
            r = requests.get(url, headers=headers)
            extras = r.json()["extras"]
            schema = r.json()["schema"]
            if schema and "name" in schema and schema["name"] != schema_name:
                if should_succeed:
                    return False
                else:
                    return True
        except:
            print("abnormal exception")
            extras = {}
            if should_succeed:
                return False

        extras = {**extras, **validation_report}

        obj = {"extras": extras}

        url = api_url + f"datasets/{dataset_id}/resources/{resource_id}/"
        response = requests.put(url, json=obj, headers=headers)

        if response.status_code != 200:
            print(
                "🔴 Schema could not be added on resource. Dataset ID: {} - Resource ID: {}".format(
                    dataset_id, resource_id
                )
            )
            if should_succeed:
                return False

        return response.status_code == 200


def upload_geojson(
    api_url: str,
    api_key: str,
    config_path: str,
    schema_consolidated_data_path: str,
    consolidation_date_str: str,
    schema_name: str,
    should_succeed=False
) -> bool:
    headers = {
        "X-API-KEY": api_key,
    }

    with open(config_path, "r") as f:
        config_dict = yaml.safe_load(f)

    geojson_version_names_list = sorted(
        [
            filename.replace(
                "consolidation_" + schema_name.replace("/", "_") + "_v_",
                "",
            ).replace("_" + consolidation_date_str + ".json", "")
            for filename in os.listdir(schema_consolidated_data_path)
            if filename.endswith(".json") and not filename.startswith(".")
        ]
    )
    if len(geojson_version_names_list) > 1:
        print("Warning: multiple versions of GeoJSON found for {}".format(schema_name))

    geojson_path = os.path.join(
        schema_consolidated_data_path,
        "consolidation_{}_v_{}_{}.json".format(
            schema_name.replace("/", "_"),
            geojson_version_names_list[-1],
            consolidation_date_str,
        ),
    )

    # Uploading file
    consolidated_dataset_id = config_dict[schema_name]["consolidated_dataset_id"]
    r_id = config_dict[schema_name]["geojson_resource_id"]
    url = (
        api_url
        + "datasets/"
        + consolidated_dataset_id
        + "/resources/"
        + r_id
        + "/upload/"
    )
    expected_status_code = 200

    with open(geojson_path, "rb") as file:
        files = {"file": (geojson_path.split("/")[-1], file.read())}

    response = requests.post(url, files=files, headers=headers)

    if response.status_code != expected_status_code:
        print("{} --- ⚠️: GeoJSON file could not be uploaded.".format(datetime.today()))
        if should_succeed:
            return False
    else:
        obj = {}
        obj["type"] = "main"
        obj["title"] = "Export au format geojson"
        obj["format"] = "json"

        r_url = api_url + "datasets/{}/resources/{}/".format(
            consolidated_dataset_id, r_id
        )
        r_response = requests.put(r_url, json=obj, headers=headers)

        if r_response.status_code == 200:
            print(
                "{} --- ✅ Successfully updated GeoJSON file with metadata.".format(
                    datetime.today()
                )
            )
        else:
            print(
                "{} --- ⚠️: file uploaded but metadata could not be updated.".format(
                    datetime.today()
                )
            )
            if should_succeed:
                return False
    return True


def upload_consolidated(
    schema_name,
    consolidated_data_path,
    config_dict,
    schemas_catalogue_list,
    config_path,
    schemas_report_dict,
    consolidation_date_str,
    single_schema,
    api_key,
    bool_upload_geojson,
    should_succeed=False
):
    headers = {
        "X-API-KEY": api_key,
    }
    print("{} - ℹ️ STARTING SCHEMA: {}".format(datetime.now(), schema_name))

    schema_consolidated_data_path = Path(
        consolidated_data_path
    ) / schema_name.replace("/", "_")
    if os.path.exists(schema_consolidated_data_path):
        # Check if dataset_id is in config. If not, create a dataset on datagouv
        schema_config = config_dict[schema_name]
        if ("publication" in schema_config.keys()) and schema_config[
            "publication"
        ]:
            if "consolidated_dataset_id" not in schema_config.keys():
                response = create_schema_consolidation_dataset(
                    schema_name, schemas_catalogue_list, api_url, headers
                )
                if response.status_code == 201:
                    consolidated_dataset_id = response.json()["id"]
                    update_config_file(
                        schema_name,
                        "consolidated_dataset_id",
                        consolidated_dataset_id,
                        config_path,
                    )
                    print(
                        "{} -- 🟢 No consolidation dataset for this schema - Successfully created (id: {})".format(
                            datetime.today(), consolidated_dataset_id
                        )
                    )
                else:
                    print(
                        "{} -- 🔴 No consolidation dataset for this schema - Failed to create one".format(
                            datetime.today()
                        )
                    )
            else:
                consolidated_dataset_id = schema_config["consolidated_dataset_id"]

            schemas_report_dict[schema_name][
                "consolidated_dataset_id"
            ] = consolidated_dataset_id

            # Creating last consolidation resources
            version_names_list = [
                filename.replace(
                    "consolidation_" + schema_name.replace("/", "_") + "_v_",
                    "",
                ).replace("_" + consolidation_date_str + ".csv", "")
                for filename in os.listdir(schema_consolidated_data_path)
                if filename.endswith(".csv") and not filename.startswith(".")
            ]

            for version_name in sorted(version_names_list):
                with open(config_path, "r") as f:
                    config_dict = yaml.safe_load(f)
                    config_dict = remove_old_schemas(
                        config_dict, schemas_catalogue_list, single_schema
                    )

                schema = {"name": schema_name, "version": version_name}
                obj = {}
                obj["schema"] = schema
                obj["type"] = "main"
                obj[
                    "title"
                ] = "Dernière version consolidée (v{} du schéma) - {}".format(
                    version_name, consolidation_date_str
                )
                obj["format"] = "csv"

                file_path = os.path.join(
                    schema_consolidated_data_path,
                    "consolidation_{}_v_{}_{}.csv".format(
                        schema_name.replace("/", "_"),
                        version_name,
                        consolidation_date_str,
                    ),
                )

                # Uploading file (creating a new resource if version was not there before)
                try:
                    r_id = config_dict[schema_name]["latest_resource_ids"][
                        version_name
                    ]
                    url = (
                        api_url
                        + "datasets/"
                        + consolidated_dataset_id
                        + "/resources/"
                        + r_id
                        + "/upload/"
                    )
                    r_to_create = False
                    expected_status_code = 200

                except KeyError:
                    url = (
                        api_url + "datasets/" + consolidated_dataset_id + "/upload/"
                    )
                    r_to_create = True
                    expected_status_code = 201

                with open(file_path, "rb") as file:
                    files = {"file": (file_path.split("/")[-1], file.read())}

                response = requests.post(url, files=files, headers=headers)

                if response.status_code == expected_status_code:
                    if r_to_create:
                        r_id = response.json()["id"]
                        update_config_version_resource_id(
                            schema_name, version_name, r_id, config_path
                        )
                        print(
                            "{} --- ➕ New latest resource ID created for {} v{} (id: {})".format(
                                datetime.today(),
                                schema_name,
                                version_name,
                                r_id,
                            )
                        )
                else:
                    r_id = None
                    print(
                        "{} --- ⚠️ Version {}: file could not be uploaded.".format(
                            datetime.today(), version_name
                        )
                    )
                    if should_succeed:
                        return False

                if r_id is not None:
                    r_url = api_url + "datasets/{}/resources/{}/".format(
                        consolidated_dataset_id, r_id
                    )
                    r_response = requests.put(r_url, json=obj, headers=headers)

                    if r_response.status_code == 200:
                        if r_to_create:
                            print(
                                "{} --- ✅ Version {}: Successfully created consolidated file.".format(
                                    datetime.today(), version_name
                                )
                            )
                        else:
                            print(
                                "{} --- ✅ Version {}: Successfully updated consolidated file.".format(
                                    datetime.today(), version_name
                                )
                            )
                    else:
                        print(
                            "{} --- ⚠️ Version {}: file uploaded but metadata could not be updated.".format(
                                datetime.today(), version_name
                            )
                        )
                        if should_succeed:
                            return False

            # Update GeoJSON file (e.g IRVE)
            if bool_upload_geojson:
                upload_success = upload_geojson(
                    api_url,
                    api_key,
                    config_path,
                    schema_consolidated_data_path,
                    consolidation_date_str,
                    schema_name,
                    should_succeed
                )
        else:
            schemas_report_dict[schema_name]["consolidated_dataset_id"] = np.nan
            print(
                "{} -- ❌ No publication for this schema.".format(datetime.today())
            )
            if should_succeed:
                return False

    else:
        schemas_report_dict[schema_name]["consolidated_dataset_id"] = np.nan
        print(
            "{} -- ❌ No consolidated file for this schema.".format(datetime.today())
        )
        if should_succeed:
            return False
    return upload_success


def update_reference_table(
    ref_tables_path,
    schema_name,
    should_succeed=False
):
    ref_table_path = os.path.join(
        ref_tables_path,
        "ref_table_{}.csv".format(schema_name.replace("/", "_")),
    )

    if os.path.isfile(ref_table_path):
        df_ref = pd.read_csv(ref_table_path)

        df_ref = add_most_recent_valid_version(df_ref)
        df_ref["is_schema_version_to_update"] = df_ref.apply(
            is_schema_version_to_update, axis=1
        )
        df_ref["is_schema_to_add"] = df_ref.apply(is_schema_to_add, axis=1)
        df_ref["is_schema_to_drop"] = df_ref.apply(is_schema_to_drop, axis=1)

        df_ref.to_csv(ref_table_path, index=False)

        print(
            "{} - ✅ Infos added for schema {}".format(datetime.today(), schema_name)
        )

    else:
        print(
            "{} - ❌ No reference table for schema {}".format(
                datetime.today(), schema_name
            )
        )
        if should_succeed:
            return False
    return True


def update_resource_send_mail_producer(
    ref_tables_path,
    schema_name,
    api_key,
    should_succeed=False
):
    headers = {
        "X-API-KEY": api_key,
    }
    ref_table_path = os.path.join(
        ref_tables_path,
        "ref_table_{}.csv".format(schema_name.replace("/", "_")),
    )

    if os.path.isfile(ref_table_path):
        df_ref = pd.read_csv(ref_table_path)
        df_ref["resource_schema_update_success"] = np.nan
        df_ref["producer_notification_success"] = np.nan

        for idx, row in df_ref.iterrows():
            if row["is_schema_version_to_update"]:
                resource_update_success = update_resource_schema(
                    api_url,
                    row["dataset_id"],
                    row["resource_id"],
                    schema_name,
                    row["most_recent_valid_version"],
                    headers,
                )
                df_ref.loc[
                    (df_ref["resource_id"] == row["resource_id"]),
                    "resource_schema_update_success",
                ] = resource_update_success

                if resource_update_success:
                    title = "Mise à jour de la version de la métadonnée schéma"
                    comment = updated_schema_comment_template.format(
                        resource_title=row["resource_title"],
                        schema_name=schema_name,
                        initial_version_name=row["initial_version_name"],
                        most_recent_valid_version=row["most_recent_valid_version"],
                    )
                    # comment_post = post_comment_on_dataset(dataset_id=row['dataset_id'],
                    #                                       title=title,
                    #                                       comment=comment,
                    #                                       api_url=api_url
                    #                                      )
                    #
                    # producer_notification_success = (comment_post.status_code == 201)

                    # df_ref.loc[(df_ref['resource_id'] == row['resource_id']), 'producer_notification_success'] = producer_notification_success
                    # No notification at the moment:
                    df_ref.loc[
                        (df_ref["resource_id"] == row["resource_id"]),
                        "producer_notification_success",
                    ] = False

            elif row["is_schema_to_add"]:
                resource_update_success = add_resource_schema(
                    api_url,
                    row["dataset_id"],
                    row["resource_id"],
                    schema_name,
                    row["most_recent_valid_version"],
                    headers,
                )
                df_ref.loc[
                    (df_ref["resource_id"] == row["resource_id"]),
                    "resource_schema_update_success",
                ] = resource_update_success

                if resource_update_success:
                    title = "Ajout de la métadonnée schéma"
                    comment = added_schema_comment_template.format(
                        resource_title=row["resource_title"],
                        schema_name=schema_name,
                        most_recent_valid_version=row["most_recent_valid_version"],
                    )
                    # comment_post = post_comment_on_dataset(dataset_id=row['dataset_id'],
                    #                                       title=title,
                    #                                       comment=comment,
                    #                                       api_url=api_url
                    #                                      )
                    #
                    # producer_notification_success = (comment_post.status_code == 201)
                    # df_ref.loc[(df_ref['resource_id'] == row['resource_id']), 'producer_notification_success'] = producer_notification_success
                    # No notification at the moment:
                    df_ref.loc[
                        (df_ref["resource_id"] == row["resource_id"]),
                        "producer_notification_success",
                    ] = False

            # Right now, we don't drop schema and do no notification
            elif row["is_schema_to_drop"]:
                #    resource_update_success = delete_resource_schema(api_url, row['dataset_id'], row['resource_id'], schema_name, headers)
                #    df_ref.loc[(df_ref['resource_id'] == row['resource_id']), 'resource_schema_update_success'] = resource_update_success
                #
                #    if resource_update_success:
                #        title = 'Suppression de la métadonnée schéma'
                #
                #        mails_type, mails_list = get_owner_or_admin_mails(row['dataset_id'], api_url)
                #
                #        if len(mails_list) > 0 : #If we found some email addresses, we send mails
                #
                #            if mails_type == 'organisation_admins' :
                #                message = deleted_schema_mail_template_org.format(organisation_name=row['organization_or_owner'],
                #                                                                  dataset_title=row['dataset_title'],
                #                                                                  resource_title=row['resource_title'],
                #                                                                  schema_name=schema_name,
                #                                                                  schema_url=get_schema_dict(schema_name, schemas_catalogue_list)['schema_url'],
                #                                                                  resource_url=row['resource_url']
                #                                                                 )
                #            elif mails_type == 'owner' :
                #                message = deleted_schema_mail_template_own.format(dataset_title=row['dataset_title'],
                #                                                                  resource_title=row['resource_title'],
                #                                                                  schema_name=schema_name,
                #                                                                  schema_url=get_schema_dict(schema_name, schemas_catalogue_list)['schema_url'],
                #                                                                  resource_url=row['resource_url']
                #                                                                 )
                #
                #
                #            #Sending mail
                #
                #            producer_notification_success_list = []
                #            print('- {} | {}:'.format(row['dataset_title'], row['resource_title']))
                #            for mail_to in mails_list :
                #                #mail_send = send_email(subject=title,
                #                #                       message=message,
                #                #                       mail_from=mail_from,
                #                #                       mail_to=mail_to,
                #                #                       smtp_host=smtp_host,
                #                #                       smtp_user=smtp_user,
                #                #                       smtp_password=smtp_password)

                #                #producer_notification_success_list += [(mail_send.status_code == 250)]
                #
                #            #producer_notification_success = any(producer_notification_success_list) # Success if at least one person receives the mail
                #
                #        else : #If no mail address, we post a comment on dataset
                #            comment = deleted_schema_comment_template.format(resource_title=row['resource_title'],
                #                                                             schema_name=schema_name,
                #                                                             schema_url=get_schema_dict(schema_name, schemas_catalogue_list)['schema_url'],
                #                                                             resource_url=row['resource_url']
                #                                                            )
                #
                #            #comment_post = post_comment_on_dataset(dataset_id=row['dataset_id'],
                #            #                                       title=title,
                #            #                                       comment=comment,
                #            #                                       api_url=api_url
                #            #                                      )
                #
                #            #producer_notification_success = (comment_post.status_code == 201)
                #
                #        #df_ref.loc[(df_ref['resource_id'] == row['resource_id']), 'producer_notification_success'] = producer_notification_success

                # TO DROP when schema will be deleted and producer notified:
                df_ref.loc[
                    (df_ref["resource_id"] == row["resource_id"]),
                    "resource_schema_update_success",
                ] = False
                df_ref.loc[
                    (df_ref["resource_id"] == row["resource_id"]),
                    "producer_notification_success",
                ] = False

        df_ref.to_csv(ref_table_path, index=False)

        print(
            "{} - ✅ Resources updated for schema {}".format(
                datetime.today(), schema_name
            )
        )

    else:
        print(
            "{} - ❌ No reference table for schema {}".format(
                datetime.today(), schema_name
            )
        )
        if should_succeed:
            return False
    return True


def add_validata_report(
    ref_tables_path,
    validata_reports_path,
    schema_name,
    api_key,
    should_succeed=True
):
    headers = {
        "X-API-KEY": api_key,
    }
    ref_table_path = os.path.join(
        ref_tables_path,
        "ref_table_{}.csv".format(schema_name.replace("/", "_")),
    )

    if os.path.isfile(ref_table_path):
        df_ref = pd.read_csv(ref_table_path)
        df_ref["resource_schema_update_success"] = np.nan
        df_ref["producer_notification_success"] = np.nan

        for idx, row in df_ref.iterrows():
            validata_report_path = (
                str(validata_reports_path)
                + "/"
                + schema_name.replace("/", "_")
                + "_"
                + row["dataset_id"]
                + "_"
                + row["resource_id"]
                + "_"
            )

            # If there is a valid version, put validata report from it
            if row["most_recent_valid_version"] == row["most_recent_valid_version"]:
                validata_report_path += row["most_recent_valid_version"] + ".json"
            # Else, check if declarative version
            else:
                # If so, put validation report from it
                if row["initial_version_name"] == row["initial_version_name"]:
                    validata_report_path += row["initial_version_name"] + ".json"
                # If not, put validation report from latest version
                else:
                    validata_report_path += (
                        max(
                            [
                                x.replace("is_valid_v_", "")
                                for x in list(row.keys())
                                if "is_valid_v_" in x
                            ]
                        )
                        + ".json"
                    )

            success = add_validation_extras(
                row["dataset_id"],
                row["resource_id"],
                validata_report_path,
                api_url,
                headers,
                schema_name,
                should_succeed
            )
    return success


def update_consolidation_documentation_report(
    schema_name,
    ref_tables_path,
    config_path,
    schemas_catalogue_list,
    consolidation_date_str,
    single_schema,
    api_key,
    should_succeed=False
):
    headers = {
        "X-API-KEY": api_key,
    }
    ref_table_path = os.path.join(
        ref_tables_path,
        "ref_table_{}.csv".format(schema_name.replace("/", "_")),
    )
    print(config_path)
    with open(config_path, "r") as f:
        config_dict = yaml.safe_load(f)
        config_dict = remove_old_schemas(config_dict, schemas_catalogue_list, single_schema)

    print("{} - ℹ️ STARTING SCHEMA: {}".format(datetime.now(), schema_name))

    schema_config = config_dict[schema_name]
    if ("publication" in schema_config.keys()) and schema_config[
        "publication"
    ]:
        if os.path.isfile(ref_table_path):
            if "consolidated_dataset_id" in schema_config.keys():
                consolidated_dataset_id = schema_config["consolidated_dataset_id"]

                obj = {}
                obj["type"] = "documentation"
                obj["title"] = "Documentation sur la consolidation - {}".format(
                    consolidation_date_str
                )

                # Uploading documentation file (creating a new resource if version was not there before)
                try:
                    doc_r_id = config_dict[schema_name]["documentation_resource_id"]
                    url = (
                        api_url
                        + "datasets/"
                        + consolidated_dataset_id
                        + "/resources/"
                        + doc_r_id
                        + "/upload/"
                    )
                    doc_r_to_create = False
                    expected_status_code = 200

                except KeyError:
                    url = (
                        api_url + "datasets/" + consolidated_dataset_id + "/upload/"
                    )
                    doc_r_to_create = True
                    expected_status_code = 201

                with open(ref_table_path, "rb") as file:
                    files = {
                        "file": (
                            ref_table_path.split("/")[-1],
                            file.read(),
                        )
                    }

                response = requests.post(url, files=files, headers=headers)

                if response.status_code == expected_status_code:
                    if doc_r_to_create:
                        doc_r_id = response.json()["id"]
                        update_config_file(
                            schema_name,
                            "documentation_resource_id",
                            doc_r_id,
                            config_path,
                        )
                        print(
                            "{} --- ➕ New documentation resource ID created for {} (id: {})".format(
                                datetime.today(), schema_name, doc_r_id
                            )
                        )
                else:
                    doc_r_id = None
                    print(
                        "{} --- ⚠️ Documentation file could not be uploaded.".format(
                            datetime.today()
                        )
                    )
                    if should_succeed:
                        return False

                if doc_r_id is not None:
                    doc_r_url = api_url + "datasets/{}/resources/{}/".format(
                        consolidated_dataset_id, doc_r_id
                    )
                    doc_r_response = requests.put(
                        doc_r_url, json=obj, headers=headers
                    )
                    if doc_r_response.status_code == 200:
                        if doc_r_to_create:
                            print(
                                "{} --- ✅ Successfully created documentation file.".format(
                                    datetime.today()
                                )
                            )
                        else:
                            print(
                                "{} --- ✅ Successfully updated documentation file.".format(
                                    datetime.today()
                                )
                            )
                    else:
                        print(
                            "{} --- ⚠️ Documentation file uploaded but metadata could not be updated.".format(
                                datetime.today()
                            )
                        )
                        if should_succeed:
                            return False

            else:
                print(
                    "{} -- ❌ No consolidation dataset ID for this schema.".format(
                        datetime.today()
                    )
                )
                if should_succeed:
                    return False

        else:
            print(
                "{} -- ❌ No reference table for this schema.".format(
                    datetime.today()
                )
            )
            if should_succeed:
                return False

    else:
        print("{} -- ❌ No publication for this schema.".format(datetime.today()))
        if should_succeed:
            return False
    return True


def append_stats_list(
    ref_tables_path,
    schema_name,
    stats_df_list
):
    ref_table_path = os.path.join(
        ref_tables_path,
        "ref_table_{}.csv".format(schema_name.replace("/", "_")),
    )

    if os.path.isfile(ref_table_path):
        df_ref = pd.read_csv(ref_table_path)
        df_ref["schema_name"] = schema_name
        df_ref["is_schema_version_updated"] = (
            df_ref["is_schema_version_to_update"]
            & df_ref["resource_schema_update_success"]
        )
        df_ref["is_schema_added"] = (
            df_ref["is_schema_to_add"] & df_ref["resource_schema_update_success"]
        )
        df_ref["is_schema_dropped"] = (
            df_ref["is_schema_to_drop"] & df_ref["resource_schema_update_success"]
        )
        df_ref["resource_schema_update_success"] = False
        df_ref.to_csv(ref_table_path, index=False)
        stats_df_list += [
            df_ref[
                [
                    "schema_name",
                    "is_schema_version_to_update",
                    "is_schema_to_add",
                    "is_schema_to_drop",
                    "resource_schema_update_success",
                    "is_schema_version_updated",
                    "is_schema_added",
                    "is_schema_dropped",
                ]
            ]
            .fillna(False)
            .groupby("schema_name")
            .sum()
            .reset_index()
        ]


def create_detailed_report(
    ref_tables_path,
    schema_name,
    report_tables_path,
    should_succeed=False
):
    ref_table_path = os.path.join(
        ref_tables_path,
        "ref_table_{}.csv".format(schema_name.replace("/", "_")),
    )

    if os.path.isfile(ref_table_path):
        df_ref = pd.read_csv(ref_table_path)

        df_ref["total_nb_resources"] = 1
        df_ref["error_type"].fillna("no-error", inplace=True)

        cols_to_sum = ["total_nb_resources"]
        cols_to_sum += [col for col in df_ref.columns if col.startswith("is_")]
        df_report = (
            df_ref.groupby(["resource_found_by", "error_type"])
            .agg({col: sum for col in cols_to_sum})
            .reset_index()
        )

        df_report.to_excel(
            os.path.join(
                report_tables_path,
                "report_table_{}.xlsx".format(schema_name.replace("/", "_")),
            ),
            index=False,
        )

        print(
            "{} - ✅ Report done for schema {}".format(datetime.today(), schema_name)
        )

    else:
        print(
            "{} - ❌ No reference table for schema {}".format(
                datetime.today(), schema_name
            )
        )
        if should_succeed:
            return False
    return True


def final_directory_clean_up(
    tmp_folder,
    output_data_folder
):
    shutil.move(tmp_folder + "consolidated_data", output_data_folder)
    shutil.move(tmp_folder + "ref_tables", output_data_folder)
    shutil.move(tmp_folder + "report_tables", output_data_folder)


def notification_synthese(
    MINIO_URL,
    MINIO_BUCKET_DATA_PIPELINE_OPEN,
    TMP_FOLDER,
    SECRET_MINIO_DATA_PIPELINE_USER,
    SECRET_MINIO_DATA_PIPELINE_PASSWORD,
    MATTERMOST_DATAGOUV_SCHEMA_ACTIVITE,
    schema_name=False,
    **kwargs
):
    """
    For single schema processing (e.g IRVE): specify schema_name as string
    For general case: specify list_schema_skip as a list of schemas to ignore
    """
    templates_dict = kwargs.get("templates_dict")
    last_conso = templates_dict["TODAY"]
    r = requests.get("https://schema.data.gouv.fr/schemas/schemas.json")
    schemas = r.json()["schemas"]

    message = (
        ":mega: *Rapport sur la consolidation des données répondant à un schéma.*\n"
    )

    if schema_name:
        schemas = [s for s in schemas if s['name'] == schema_name]

    for s in schemas:
        if s["schema_type"] == "tableschema":
            try:
                filename = (
                    f"https://{MINIO_URL}/{MINIO_BUCKET_DATA_PIPELINE_OPEN}/schema/schemas_consolidation/"
                    f"{last_conso}/output/ref_tables/ref_table_{s['name'].replace('/','_')}.csv"
                )
                df = pd.read_csv(filename)
                nb_declares = df[df["resource_found_by"] == "1 - schema request"].shape[0]
                nb_suspectes = df[df["resource_found_by"] != "1 - schema request"].shape[0]
                nb_valides = df[df["is_valid_one_version"]].shape[0]
                df = df[~(df["is_valid_one_version"])]
                df = df[
                    [
                        "dataset_id",
                        "resource_id",
                        "dataset_title",
                        "resource_title",
                        "dataset_page",
                        "resource_url",
                        "resource_found_by",
                    ]
                ]
                df["schema_name"] = s["title"]
                df["schema_id"] = s["name"]
                df["validata_report"] = (
                    "https://validata.etalab.studio/table-schema?input=url&url="
                    f"{df['resource_url']}&schema_url={s['schema_url']}"
                )
                df.to_csv(
                    f"{TMP_FOLDER}liste_erreurs-{s['name'].replace('/', '_')}.csv"
                )

                send_files(
                    MINIO_URL=MINIO_URL,
                    MINIO_BUCKET=MINIO_BUCKET_DATA_PIPELINE_OPEN,
                    MINIO_USER=SECRET_MINIO_DATA_PIPELINE_USER,
                    MINIO_PASSWORD=SECRET_MINIO_DATA_PIPELINE_PASSWORD,
                    list_files=[
                        {
                            "source_path": f"{TMP_FOLDER}",
                            "source_name": f"liste_erreurs-{s['name'].replace('/', '_')}.csv",
                            "dest_path": "schema/schemas_consolidation/liste_erreurs/",
                            "dest_name": f"liste_erreurs-{s['name'].replace('/', '_')}.csv",
                        }
                    ],
                )

                message += f"\n- Schéma ***{s['title']}***\n - Ressources déclarées : {nb_declares}"

                if nb_suspectes != 0:
                    message += f"\n - Ressources suspectées : {nb_suspectes}"

                message += (
                    f"\n - Ressources valides : {nb_valides} \n - [Liste des ressources non valides]"
                    f"(https://{MINIO_URL}/{MINIO_BUCKET_DATA_PIPELINE_OPEN}/schema/schemas_consolidation/"
                    f"{last_conso}/liste_erreurs/liste_erreurs-{s['name'].replace('/', '_')}.csv)\n"
                )
            except: # noqa
                print("No report for {}".format(s["name"]))
                pass
    send_message(message, MATTERMOST_DATAGOUV_SCHEMA_ACTIVITE)


# Template for consolidation datasets title
datasets_title_template = (
    'Fichiers consolidés des données respectant le schéma "{schema_title}"'
)

# Template for consolidation datasets description (Markdown)

datasets_description_template = """
Ceci est un jeu de données généré automatiquement par Etalab. Il regroupe les données qui respectent le schéma {schema_name}, par version du schéma.

La fiche présentant le schéma et ses caractéristiques est disponible sur [https://schema.data.gouv.fr/{schema_name}/latest.html](https://schema.data.gouv.fr/{schema_name}/latest.html)

### Qu'est-ce qu'un schéma ?

Les schémas de données permettent de décrire des modèles de données : quels sont les différents champs, comment sont représentées les données, quelles sont les valeurs possibles, etc.

Vous pouvez retrouver l'ensemble des schémas au référentiel sur le site schema.data.gouv.fr

### Comment sont produites ces données ?

Ces données sont produites à partir des ressources publiées sur le site [data.gouv.fr](http://data.gouv.fr) par différents producteurs. Etalab détecte automatiquement les ressources qui obéissent à un schéma et concatène l'ensemble des données en un seul fichier, par version de schéma.

Ces fichiers consolidés permettent aux réutilisateurs de manipuler un seul fichier plutôt qu'une multitude de ressources et contribue ainsi à améliorer la qualité de l'open data.

### Comment intégrer mes données dans ces fichiers consolidés ?

Si vous êtes producteurs de données et que vous ne retrouvez pas vos données dans ces fichiers consolidés, c'est probablement parce que votre ressource sur [data.gouv.fr](http://data.gouv.fr) n'est pas conforme au schéma. Vous pouvez vérifier la conformité de votre ressource via l'outil [https://publier.etalab.studio/upload?schema={schema_name}](https://publier.etalab.studio/upload?schema={schema_name})

En cas de problème persistant, vous pouvez contacter le support data.gouv [lien vers [https://support.data.gouv.fr/](https://support.data.gouv.fr/)].

### Comment produire des données conformes ?

Un certain nombre d'outils existent pour accompagner les producteurs de données. Vous pouvez notamment vous connecter sur le site [https://publier.etalab.studio/select?schema={schema_name}](https://publier.etalab.studio/select?schema={schema_name}) pour pouvoir saisir vos données selon trois modes :

- upload de fichier existant
- saisie via formulaire
- saisie via tableur
"""

# Template for mail/comment (added, updated and deleted schema)

added_schema_comment_template = """
Bonjour,

Vous recevez ce message car suite à un contrôle automatique de vos données par notre robot de validation, nous constatons que le fichier {resource_title} de ce jeu de données est conforme au schéma {schema_name} (version {most_recent_valid_version}).
Nous avons donc automatiquement ajouté à ce fichier la métadonnée de schéma correspondante, ce qui atteste de la qualité des données que vous avez publiées.

Une question ? Écrivez à validation@data.gouv.fr en incluant l'URL du jeu de données concerné.
"""

updated_schema_comment_template = """
Bonjour,

Vous recevez ce message car suite à un contrôle automatique de vos données par notre robot de validation, nous constatons que le fichier {resource_title} de ce jeu de données (qui respecte le schéma {schema_name}) n'avait pas dans ses métadonnées la version de schéma la plus récente qu'il respecte.
Nous avons donc automatiquement mis à jour les métadonnées du fichier en indiquant la version adéquate du schéma.

Version précédemment indiquée : {initial_version_name}
Version mise à jour : {most_recent_valid_version}

Une question ? Écrivez à validation@data.gouv.fr en incluant l'URL du jeu de données concerné.
"""

deleted_schema_mail_template_org = """
Bonjour,<br />
<br />
Vous recevez ce message automatique car vous êtes admin de l'organisation {organisation_name} sur data.gouv.fr. Votre organisation a publié le jeu de données {dataset_title}, dont le fichier {resource_title} se veut conforme au schéma {schema_name}.<br />
Cependant, suite à un contrôle automatique de vos données par notre robot de validation, il s'avère que ce fichier ne respecte aucune version de ce schéma.<br />
Nous avons donc automatiquement supprimé la métadonnée de schéma associée à ce fichier.<br />
<br />
Vous pouvez consulter le [rapport de validation](https://validata.etalab.studio/table-schema?input=url&schema_url={schema_url}&url={resource_url}&repair=true) pour vous aider à corriger les erreurs (ce rapport est relatif à la version la plus récente du schéma, mais votre fichier a bien été testé vis-à-vis de toutes les versions possibles du schéma).<br />
<br />
Vous pourrez alors restaurer la métadonnée de schéma une fois un fichier valide publié.<br />
<br />
Une question ? Écrivez à validation@data.gouv.fr en incluant l'URL du jeu de données concerné.<br />
<br />
Cordialement,<br />
<br />
L'équipe de data.gouv.fr
"""

deleted_schema_mail_template_own = """
Bonjour,<br />
<br />
Vous recevez ce message automatique car vous avez publié sur data.gouv.fr le jeu de données {dataset_title}, dont le fichier {resource_title} se veut conforme au schéma {schema_name}.<br />
Cependant, suite à un contrôle automatique de vos données par notre robot de validation, il s'avère que ce fichier ne respecte aucune version de ce schéma.<br />
Nous avons donc automatiquement supprimé la métadonnée de schéma associée à ce fichier.<br />
<br />
Vous pouvez consulter le [rapport de validation](https://validata.etalab.studio/table-schema?input=url&schema_url={schema_url}&url={resource_url}&repair=true) pour vous aider à corriger les erreurs (ce rapport est relatif à la version la plus récente du schéma, mais votre fichier a bien été testé vis-à-vis de toutes les versions possibles du schéma).<br />
<br />
Vous pourrez alors restaurer la métadonnée de schéma une fois un fichier valide publié.<br />
<br />
Une question ? Écrivez à validation@data.gouv.fr en incluant l'URL du jeu de données concerné.<br />
<br />
Cordialement,<br />
<br />
L'équipe de data.gouv.fr
"""

deleted_schema_comment_template = """
Bonjour,

Vous recevez ce message car suite à un contrôle automatique de vos données par notre robot de validation, nous constatons que le fichier {resource_title} de ce jeu de données se veut conforme au schéma {schema_name} alors qu'il ne respecte aucune version de ce schéma.
Nous avons donc automatiquement supprimé la métadonnée de schéma associée à ce fichier.

Vous pouvez consulter le [rapport de validation](https://validata.etalab.studio/table-schema?input=url&schema_url={schema_url}&url={resource_url}&repair=true) pour vous aider à corriger les erreurs (ce rapport est relatif à la version la plus récente du schéma, mais votre fichier a bien été testé vis-à-vis de toutes les versions possibles du schéma).

Vous pourrez alors restaurer la métadonnée de schéma une fois un fichier valide publié.

Une question ? Écrivez à validation@data.gouv.fr en incluant l'URL du jeu de données concerné.
"""
