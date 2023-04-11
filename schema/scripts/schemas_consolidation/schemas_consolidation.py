from typing import List, Optional
import pandas as pd
import numpy as np
import requests
import json
from json import JSONDecodeError
import chardet
from pathlib import Path
import pickle
import os
import yaml
from datetime import datetime
import time


VALIDATA_BASE_URL = (
    "https://validata-api.app.etalab.studio/validate?schema={schema_url}&url={rurl}"
)


def remove_old_schemas(config_dict, schemas_catalogue_list):
    schemas_list = [schema["name"] for schema in schemas_catalogue_list]
    mydict = {}
    # Remove old schemas still in yaml
    for schema_name in config_dict.keys():
        if schema_name in schemas_list:
            mydict[schema_name] = config_dict[schema_name]
        else:
            print("{} - Remove old schema not anymore in catalog".format(schema_name))
    return mydict


def get_schema_dict(
    schema_name: str, schemas_catalogue_list: List[dict]
) -> Optional[dict]:
    """Get the dictionnary with information on the schema (when schemas catalogue list already loaded)"""
    res = None
    for schema in schemas_catalogue_list:
        if schema["name"] == schema_name:
            res = schema

    if res is None:
        print("No schema named '{}' found.".format(schema_name))

    return res


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
def parse_api_search(url: str, api_url: str, schema_name: str) -> pd.DataFrame:
    r = requests.get(url)
    data = r.json()
    nb_pages = int(data["total"] / data["page_size"]) + 1
    arr = []
    for i in range(1, nb_pages + 1):
        r = requests.get(url + "&page=" + str(i))
        data = r.json()
        if "data" in data:
            for dataset_result in data["data"]:
                r2 = requests.get(api_url + "datasets/" + dataset_result["id"])
                dataset = r2.json()

                for res in dataset["resources"]:
                    should_add_resource = True
                    if "name" in res["schema"] and res["schema"]["name"] != schema_name:
                        should_add_resource = False
                    if should_add_resource:
                        if "format=csv" in res["url"]:
                            filename = res["url"].split("/")[-3] + ".csv"
                        else:
                            filename = res["url"].split("/")[-1]
                        ext = filename.split(".")[-1]
                        obj = {}
                        obj["dataset_id"] = dataset["id"]
                        obj["dataset_title"] = dataset["title"]
                        obj["dataset_slug"] = dataset["slug"]
                        obj["dataset_page"] = dataset["page"]
                        obj["resource_id"] = res["id"]
                        obj["resource_title"] = res["title"]
                        obj["resource_url"] = res["url"]
                        obj["resource_last_modified"] = res["last_modified"]
                        if ext not in ["csv", "xls", "xlsx"]:
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


# API parsing to get resources infos based on schema metadata, tags and search keywords
def parse_api(url: str, schema_name: str) -> pd.DataFrame:
    r = requests.get(url)
    data = r.json()
    nb_pages = int(data["total"] / data["page_size"]) + 1
    arr = []
    for i in range(1, nb_pages + 1):
        r = requests.get(url + "&page=" + str(i))
        data = r.json()
        if "data" in data:
            for dataset in data["data"]:
                for res in dataset["resources"]:
                    should_add_resource = True
                    if "name" in res["schema"] and res["schema"]["name"] != schema_name:
                        should_add_resource = False
                    if should_add_resource:
                        if "format=csv" in res["url"]:
                            filename = res["url"].split("/")[-3] + ".csv"
                        else:
                            filename = res["url"].split("/")[-1]
                        ext = filename.split(".")[-1]
                        obj = {}
                        obj["dataset_id"] = dataset["id"]
                        obj["dataset_title"] = dataset["title"]
                        obj["dataset_slug"] = dataset["slug"]
                        obj["dataset_page"] = dataset["page"]
                        obj["resource_id"] = res["id"]
                        obj["resource_title"] = res["title"]
                        obj["resource_url"] = res["url"]
                        obj["resource_last_modified"] = res["last_modified"]
                        if ext not in ["csv", "xls", "xlsx"]:
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


def run_schemas_consolidation(
    api_url: str,
    working_dir: str,
    tmp_folder: str,
    date_airflow: str,
    schema_catalog: str,
    tmp_config_file: str,
) -> None:
    tmp_path = Path(tmp_folder)
    config_path = Path(tmp_config_file)
    schemas_list_url = schema_catalog
    schema_url_base = api_url + "datasets/?schema={schema_name}"
    tag_url_base = api_url + "datasets/?tag={tag}"
    search_url_base = (
        "/".join(api_url.split("/")[:-2]) + "/2/datasets/search/?q={search_word}"
    )

    MINIMUM_VALID_RESOURCES_TO_CONSOLIDATE = 5

    consolidation_date_str = date_airflow.replace("-", "")
    print(consolidation_date_str)

    data_path = tmp_path / "data"
    data_path.mkdir(parents=True, exist_ok=True)

    consolidated_data_path = tmp_path / "consolidated_data"
    consolidated_data_path.mkdir(parents=True, exist_ok=True)

    ref_tables_path = tmp_path / "ref_tables"
    ref_tables_path.mkdir(parents=True, exist_ok=True)

    report_tables_path = tmp_path / "report_tables"
    report_tables_path.mkdir(parents=True, exist_ok=True)

    output_path = tmp_path / "output"
    output_path.mkdir(parents=True, exist_ok=True)

    validata_reports_path = tmp_path / "validata_reports"
    validata_reports_path.mkdir(parents=True, exist_ok=True)

    # Keeping track of schema info
    schemas_report_dict = {}

    schemas_catalogue_dict = requests.get(schemas_list_url).json()
    print("Schema catalogue URL: {}".format(schemas_catalogue_dict["$schema"]))
    print("Version: {}".format(schemas_catalogue_dict["version"]))

    schemas_catalogue_list = [
        schema
        for schema in schemas_catalogue_dict["schemas"]
        if schema["schema_type"] == "tableschema"
    ]
    nb_schemas = len(schemas_catalogue_list)
    print("Total number of schemas: {}".format(nb_schemas))

    for schema in schemas_catalogue_list:
        print("- {} ({} versions)".format(schema["name"], len(schema["versions"])))
        schemas_report_dict[schema["name"]] = {"nb_versions": len(schema["versions"])}

    # ### Creating/updating config file with missing schemas
    if os.path.exists(config_path):
        with open(config_path, "r") as f:
            config_dict = yaml.safe_load(f)
            config_dict = remove_old_schemas(config_dict, schemas_catalogue_list)
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

    # **⚠️⚠️⚠️ EDIT CONFIG FILE IF NEEDED (especially for new schemas) ⚠️⚠️⚠️**
    #
    # Then, reload config file:

    with open(config_path, "r") as f:
        config_dict = yaml.safe_load(f)
        config_dict = remove_old_schemas(config_dict, schemas_catalogue_list)

    # ## Building reference tables (parsing and listing resources + Validata check)
    for schema_name in config_dict.keys():
        print("{} - ℹ️ STARTING SCHEMA: {}".format(datetime.now(), schema_name))

        # NEEDED PARAMETERS

        # Schema description and consolidation configuration
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
            df_schema = parse_api(schema_url_base.format(schema_name=schema_name), schema_name)
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
                df_tag = parse_api(tag_url_base.format(tag=tag), schema_name)
                schemas_report_dict[schema_name]["nb_resources_found_by_tags"] += len(
                    df_tag
                )
                if len(df_tag) > 0:
                    df_tag["resource_found_by"] = "2 - tag request"
                    df_list += [df_tag]

            # Listing resources by search (keywords) requests
            schemas_report_dict[schema_name]["nb_resources_found_by_search_words"] = 0
            for search_word in search_words_list:
                df_search_word = parse_api_search(
                    search_url_base.format(search_word=search_word), api_url, schema_name
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

            else:
                print(
                    "{} -- ⚠️ No resource found for {}.".format(
                        datetime.now(), schema_name
                    )
                )

        else:
            print(
                "{} -- ❌ Schema not to consolidate according to config file.".format(
                    datetime.now()
                )
            )

    # ## Downloading valid data
    # We download only data that is valid for at least one version of the schema.

    for schema_name in config_dict.keys():
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
                        file_extension = os.path.splitext(row["resource_url"])[1]
                        written_filename = f"{row['resource_id']}{file_extension}"

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

            df_ref.to_csv(ref_table_path, index=False)

        else:
            print(
                "{} -- ❌ No reference table made for this schema (schema not to consolidate, no version to consolidate or no resource found).".format(
                    datetime.now()
                )
            )

    # ## Consolidation
    for schema_name in config_dict.keys():
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
                            if (
                                "constraints" in field_dict.keys()
                                and "required" in field_dict["constraints"].keys()
                                and field_dict["constraints"]["required"]
                            )
                        ]

                        if "primaryKey" in version_dict.keys():
                            primary_key = version_dict["primaryKey"]
                        else:
                            primary_key = None

                        df_r_list = []

                        for index, row in df_ref_v.iterrows():
                            file_extension = os.path.splitext(row["resource_url"])[1]
                            file_path = os.path.join(
                                schema_data_path,
                                row["dataset_slug"],
                                f"{row['resource_id']}{file_extension}",
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

                    else:
                        print(
                            "{} -- ⚠️ No valid resource for version {} of this schema".format(
                                datetime.today(), version_name
                            )
                        )

            df_ref.to_csv(ref_table_path, index=False)

        else:
            print(
                "{} -- ❌ No data downloaded for this schema.".format(datetime.today())
            )

    with open(tmp_path / "schemas_report_dict.pickle", "wb") as f:
        pickle.dump(schemas_report_dict, f, pickle.HIGHEST_PROTOCOL)
