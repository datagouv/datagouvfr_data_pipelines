import json
import logging
import os
import pickle
import shutil
import time
from datetime import date, datetime
from json import JSONDecodeError
from pathlib import Path

import chardet
import emails
import numpy as np
import pandas as pd
import pytz
import requests
import yaml
from tqdm import tqdm

from datagouvfr_data_pipelines.utils.datagouv import (
    ORGA_REFERENCE,
    VALIDATA_BASE_URL,
    local_client,
)
from datagouvfr_data_pipelines.utils.mattermost import send_message
from datagouvfr_data_pipelines.utils.minio import File, MinIOClient
from datagouvfr_data_pipelines.utils.retry import simple_connection_retry

pd.set_option("display.max_columns", None)
tqdm.pandas(desc="pandas progress bar", mininterval=30)

VALIDATA_BASE_URL = VALIDATA_BASE_URL + "/validate?schema={schema_url}&url={rurl}"
MINIMUM_VALID_RESOURCES_TO_CONSOLIDATE = 5
api_url = f"{local_client.base_url}/api/1/"
schema_endpoint_base = "api/1/datasets/?schema={schema_name}"
tag_endpoint_base = "api/1/datasets/?tag={tag}"
search_endpoint_base = "api/2/datasets/search/?q={search_word}"
local_timezone = pytz.timezone("Europe/Paris")
ignored_datasets = ["5b598be088ee387c0c353714"]
forced_validation_day = date(2023, 12, 6)
forced_validation = False
if datetime.today().date().month == forced_validation_day.month:
    if datetime.today().date().day == forced_validation_day.day:
        forced_validation = True


def load_config(config_path):
    if os.path.exists(config_path):
        with open(config_path, "r") as infile:
            return yaml.safe_load(infile)
    return {}


def build_ref_table_name(schema_name):
    return "ref_table_{}.csv".format(schema_name.replace("/", "_"))


def build_consolidation_name(
    schema_name, version_name, consolidation_date_str, extension="csv"
):
    return "consolidation_{}_v_{}_{}.{}".format(
        schema_name.replace("/", "_"),
        version_name,
        consolidation_date_str,
        extension,
    )


def build_report_prefix(
    validata_reports_path,
    schema_name,
    dataset_id,
    resource_id,
):
    return (
        str(validata_reports_path)
        + "/"
        + schema_name.replace("/", "_")
        + f"_{dataset_id}_{resource_id}_"
    )


def comparer_versions(version: str) -> list[int | float]:
    return [int(part) if part.isnumeric() else np.inf for part in version.split(".")]


def drop_versions_sample(versions, nb_to_keep=5, level=0):
    _versions = sorted(versions, key=comparer_versions)
    majors = set(".".join(v.split(".")[0 : level + 1]) for v in _versions)
    # if only one major: consider each minor as a major and run again
    if len(majors) == 1:
        if level == 2:
            return []
        return drop_versions_sample(versions, level=level + 1)
    latest_each_major = sorted(
        [
            sorted(
                [v for v in _versions if ".".join(v.split(".")[0 : level + 1]) == m],
                key=comparer_versions,
            )[-1]
            for m in majors
        ],
        key=comparer_versions,
    )
    # if more than nb_to_keep majors: keep latest of each of the nb_to_keep latest majors
    if len(latest_each_major) >= nb_to_keep:
        return [v for v in versions if v not in latest_each_major[-nb_to_keep:]]
    # if 1 < len(majors) < nb_to_keep: keep latest of each major and fill with latest of the latest major
    for v in reversed(_versions):
        if len(latest_each_major) == nb_to_keep:
            break
        if v not in latest_each_major:
            latest_each_major.append(v)
    return [v for v in versions if v not in latest_each_major]


def remove_old_schemas(config_dict, schemas_catalogue_list, single_schema=False):
    schemas_list = [schema["name"] for schema in schemas_catalogue_list]
    mydict = {}
    # Remove old schemas still in yaml
    for schema_name in config_dict.keys():
        if schema_name in schemas_list:
            mydict[schema_name] = config_dict[schema_name]
        else:
            if not single_schema:
                logging.info(
                    f"{schema_name} - Remove old schema not anymore in catalogue"
                )
    return mydict


def get_schema_dict(
    schema_name: str,
    schemas_catalogue_list: list[dict],
) -> dict:
    """Get the dictionnary with information on the schema (when schemas catalogue list already loaded)"""
    for schema in schemas_catalogue_list:
        if schema["name"] == schema_name:
            return schema
    raise ValueError(f"No schema named '{schema_name}' found.")


def add_most_recent_valid_version(df_ref: pd.DataFrame) -> pd.DataFrame:
    """
    Based on validation columns by version, adds a column to
    the ref_table that shows the most recent version
    of the schema for which the resource is valid
    """
    version_cols_list = [col for col in df_ref.columns if col.startswith("is_valid_v_")]

    df_ref["most_recent_valid_version"] = ""

    for col in sorted(version_cols_list, reverse=True, key=comparer_versions):
        df_ref.loc[
            df_ref["most_recent_valid_version"] == "",
            "most_recent_valid_version",
        ] = df_ref.loc[df_ref["most_recent_valid_version"] == "", col].apply(
            lambda x: x * col.replace("is_valid_v_", "")
        )

    return df_ref


# Add the schema default configuration in the configuration YAML file
def add_schema_default_config(
    schema_name: str,
    config_path: Path,
    schemas_catalogue_list: list[dict],
) -> None:
    schema_dict = get_schema_dict(schema_name, schemas_catalogue_list)

    default_schema_config_dict = {
        "consolidate": False,
        "search_words": [
            schema_dict["title"]
        ],  # setting schema title as a default search keyword for resources
    }

    config_dict = load_config(config_path)
    config_dict[schema_name] = default_schema_config_dict

    with open(config_path, "w") as outfile:
        yaml.dump(config_dict, outfile, default_flow_style=False)


# API parsing to get resources infos based on schema metadata, tags and search keywords
def parse_api(
    url: str,
    api_url: str,
) -> pd.DataFrame:
    fields = "id,title,slug,page,organization,owner,private,"
    fields += "resources{schema,url,id,title,last_modified,created_at,"
    fields += "extras{check:headers:content-type,check:available}}"
    all_datasets = local_client.get_all_from_api_query(
        url, mask=f"data{{{fields}}}" if "api/2" not in url else None
    )
    # when using api/2, the resources are not directly accessible, so we use api/1 to get them
    if "api/2" in url:
        session = requests.Session()
        session.headers.update({"X-fields": fields})
        tmp = []
        for d in all_datasets:
            r = session.get(api_url + "datasets/" + d["id"])
            r.raise_for_status()
            tmp.append(r.json())
        session.close()
        all_datasets = tmp
    arr = []
    for dataset in all_datasets:
        if dataset["id"] in ignored_datasets or dataset.get("private"):
            continue
        for res in dataset["resources"]:
            if "format=csv" in res["url"]:
                filename = res["url"].split("/")[-3] + ".csv"
            else:
                filename = res["url"].split("/")[-1]
            ext = filename.split(".")[-1]
            detected_mime = (
                res.get("extras", {})
                .get("check:headers:content-type", "")
                .split(";")[0]
                .strip()
            )
            obj = {
                "dataset_id": dataset["id"],
                "dataset_title": dataset["title"],
                "dataset_slug": dataset["slug"],
                "dataset_page": dataset["page"],
                "resource_id": res["id"],
                "resource_title": res["title"],
                "resource_url": res["url"],
                "resource_last_modified": res["last_modified"],
                "resource_created_at": res["created_at"],
                "publish_source": res.get("extras", {}).get("publish_source", ""),
                "error_type": (
                    "hydra-unavailable-resource"
                    if not res.get("extras", {}).get("check:available", True)
                    else None
                ),
            }
            appropriate_extension = ext in ["csv", "xls", "xlsx"]
            mime_dict = {
                "text/csv": "csv",
                "application/vnd.ms-excel": "xls",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "xlsx",
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
                    obj["is_orga"] = bool(dataset["organization"])
            arr.append(obj)
    df = pd.DataFrame(arr)
    return df


# Make the validation report based on the resource url, the resource API-url,
# the schema url and validation url
@simple_connection_retry
def make_validata_report(
    rurl, schema_url, resource_api_url, validata_base_url=VALIDATA_BASE_URL
):
    # saves time by not pinging Validata for unchanged resources
    data = requests.get(resource_api_url)
    if not data.ok:
        return {"report": {"error": "ressource not available", "valid": False}}
    data = data.json()
    extras = data["extras"]
    if (
        extras.get("analysis:error") == "File too large to download"
        or (data["filesize"] or 0) > 1e8
    ):
        # too large resources will make validata crash
        logging.warning(f"Skipping validation, file is too large: {resource_api_url}")
        return {"report": {"error": "ressource is too large", "valid": False}}
    if data["filetype"] == "file":
        # check if hydra says the resources is not available, if no check then proceed
        if not extras.get("check:available", True):
            logging.warning(
                f"Skipping validation, resource unavailable: {resource_api_url}"
            )
            return {"report": {"error": "ressource not available", "valid": False}}
        # once a year we force scan every file, to compensate for potential anomalies
        if forced_validation:
            logging.info(f"forced validation for {resource_api_url}")
            r = requests.get(validata_base_url.format(schema_url=schema_url, rurl=rurl))
            time.sleep(0.5)
            return r.json()
        # check if resource has never been validated
        if "validation-report:validation_date" not in extras:
            logging.info(f"no validation yet: validation for {resource_api_url}")
            r = requests.get(validata_base_url.format(schema_url=schema_url, rurl=rurl))
            time.sleep(0.5)
            return r.json()
        # if it has, check whether it has changed since last validation
        last_modification_date = datetime.fromisoformat(data["last_modified"])
        last_validation_date = datetime.fromisoformat(extras["validation-report:validation_date"])
        # progressively switching to timezone-aware dates
        if not last_validation_date.tzinfo:
            last_validation_date = local_timezone.localize(last_validation_date)
        if last_modification_date > last_validation_date:
            logging.info(f"recent change: validation for {resource_api_url}")
            # resource has been changed since last validation: validate again
            r = requests.get(validata_base_url.format(schema_url=schema_url, rurl=rurl))
            time.sleep(0.5)
            return r.json()
        else:
            # resource has not changed since last validation, validation report from metadata
            # NB: only recreating the keys required for downstream processes
            logging.info(f"no recent change: no validation for {resource_api_url}")
            return {
                "report": {
                    "stats": {"errors": extras.get("validation-report:nb_errors")},
                    "valid": extras.get("validation-report:valid_resource"),
                    "tasks": [{"errors": extras.get("validation-report:errors")}],
                    "date": extras.get("validation-report:validation_date"),
                    "from_metadata": True,
                }
            }
    else:
        logging.info(f"remote resource: validation for {resource_api_url}")
        r = requests.get(validata_base_url.format(schema_url=schema_url, rurl=rurl))
        time.sleep(0.5)
        return r.json()


# Returns if a resource is valid or not regarding a schema (version)
def is_validata_valid(
    rurl, schema_url, resource_api_url, validata_base_url=VALIDATA_BASE_URL
):
    try:
        report = make_validata_report(
            rurl, schema_url, resource_api_url, validata_base_url
        )
        try:
            res = report["report"]["valid"]
        except KeyError:
            logging.warning(f"---- 🔴 No info in validata report for resource: {rurl}")
            res = False
            report = None
    except JSONDecodeError:
        logging.warning(
            f"---- 🔴 Could not make JSON from validata report for resource: {rurl}"
        )
        res = False
        report = None
    return res, report


def save_validata_report(
    res,
    full_report,
    version,
    schema_name,
    dataset_id,
    resource_id,
    validata_reports_path,
):
    _report = full_report.get("report", {})

    if not _report:
        return

    save_report = {
        "validation-report:schema_name": schema_name,
        "validation-report:schema_version": version,
        "validation-report:schema_type": "tableschema",
        "validation-report:validator": "validata",
        "validation-report:valid_resource": res,
    }

    if _report.get("from_metadata"):
        save_report["from_metadata"] = True

    try:
        nb_errors = nb_errors = min(_report["stats"]["errors"], 100)
    except KeyError:
        nb_errors = None
    save_report["validation-report:nb_errors"] = nb_errors

    try:
        errors = _report["errors"][:100]
    except KeyError:
        errors = None
    save_report["validation-report:errors"] = errors

    if "date" in full_report.keys():
        save_report["validation-report:validation_date"] = full_report["date"]
    else:
        # progressively switching to timezone-aware dates
        save_report["validation-report:validation_date"] = str(
            datetime.now(local_timezone)
        )

    with open(
        build_report_prefix(
            validata_reports_path,
            schema_name,
            dataset_id,
            resource_id,
        )
        + version
        + ".json",
        "w",
    ) as f:
        json.dump(save_report, f)


# Returns if a resource is valid based on its "ref_table" row
def is_validata_valid_row(row, schema_url, version, schema_name, validata_reports_path):
    if row["error_type"] is None:  # if no error
        rurl = row["resource_url"]
        resource_api_url = f"{local_client.base_url}/api/1/datasets/{row['dataset_id']}/resources/{row['resource_id']}/"
        res, report = is_validata_valid(rurl, schema_url, resource_api_url)
        if report and not report.get("report", {}).get("error", False):
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
@simple_connection_retry
def get_resource_schema_version(row: pd.Series, api_url: str):
    url = api_url + f"datasets/{row['dataset_id']}/resources/{row['resource_id']}/"
    r = requests.get(url, headers={"X-fields": "schema"})
    if r.status_code == 200:
        r_json = r.json()
        if r_json.get("schema") and r_json.get("schema").get("version", False):
            return r_json["schema"]["version"]
        else:
            return np.nan
    else:
        return np.nan


def get_schema_report(
    schemas_catalogue_url: str,
    config_path: Path,
    schema_name: str | None = None,
    list_schema_skip: list = [],
) -> tuple[dict, list]:
    """
    For single schema processing (e.g IRVE): specify schema_name as string
    For general case: specify list_schema_skip as a list of schemas to ignore
    """

    # always specify one of these two, depending on the case
    assert schema_name or len(list_schema_skip) > 0

    schemas_report_dict = {}

    schemas_catalogue_response = requests.get(schemas_catalogue_url)
    schemas_catalogue_response.raise_for_status()
    schemas_catalogue_dict = schemas_catalogue_response.json()
    logging.info(f"Schema catalogue URL: {schemas_catalogue_dict['$schema']}")
    logging.info(f"Version: {schemas_catalogue_dict['version']}")

    if not schema_name:
        schemas_catalogue_list = [
            schema
            for schema in schemas_catalogue_dict["schemas"]
            if schema["schema_type"] == "tableschema"
            and schema["name"] not in list_schema_skip
        ]
        logging.info(f"Total number of schemas: {len(schemas_catalogue_list)}")

        for schema in schemas_catalogue_list:
            logging.info(f"- {schema['name']} ({len(schema['versions'])} versions)")
            schemas_report_dict[schema["name"]] = {
                "nb_versions": len(schema["versions"])
            }

        # Creating/updating config file with missing schemas
        config_dict = load_config(config_path)
        if config_dict:
            config_dict = remove_old_schemas(config_dict, schemas_catalogue_list)

    else:
        schemas_catalogue_list = [
            schema
            for schema in schemas_catalogue_dict["schemas"]
            if schema["schema_type"] == "tableschema" and schema["name"] == schema_name
        ]
        logging.info(
            f"- {schemas_catalogue_list[0]['name']} ({len(schemas_catalogue_list[0]['versions'])} versions)"
        )
        schemas_report_dict[schema_name] = {
            "nb_versions": len(schemas_catalogue_list[0]["versions"])
        }
        # Creating/updating config file with missing schemas
        config_dict = load_config(config_path)
        if config_dict:
            config_dict = remove_old_schemas(
                config_dict, schemas_catalogue_list, single_schema=True
            )

    for schema in schemas_catalogue_list:
        if schema["name"] not in config_dict.keys():
            add_schema_default_config(
                schema["name"], config_path, schemas_catalogue_list
            )
            schemas_report_dict[schema["name"]]["new_config_created"] = True
            logging.info(f"- ➕ Schema {schema['name']} added to config file.")
        else:
            schemas_report_dict[schema["name"]]["new_config_created"] = False
            logging.info(f"- 🆗 Schema {schema['name']} already in config file.")
    return schemas_report_dict, schemas_catalogue_list


def build_reference_table(
    config_dict: dict,
    schema_name: str,
    schemas_catalogue_list: list,
    schemas_report_dict: dict,
    validata_reports_path: Path,
    ref_tables_path: Path,
    should_succeed: bool = False,
) -> bool:
    logging.info(f"- ℹ️ STARTING SCHEMA: {schema_name}")
    if forced_validation:
        logging.info("🎂 Today is forced validation day!")

    # only info about consolidation and publication
    schema_config = config_dict[schema_name]

    if schema_config["consolidate"]:
        logging.info("This schema will be consolidated.")
    else:
        logging.info("This schema will NOT be consolidated.")
        logging.info("Building ref table to fill the resources' extras.")
    # Schema official specification (in catalogue)
    schema_dict = get_schema_dict(schema_name, schemas_catalogue_list)

    # Datasets to exclude (from config)
    datasets_to_exclude = []
    if "consolidated_dataset_id" in schema_config.keys():
        datasets_to_exclude += [schema_config["consolidated_dataset_id"]]
    if "exclude_dataset_ids" in schema_config.keys():
        if isinstance(schema_config["exclude_dataset_ids"], list):
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
    else:
        # we only consider certain versions to save processing time
        # see the func for insights
        drop_versions = drop_versions_sample(
            [v["version_name"] for v in schema_dict["versions"]]
        )

    schemas_report_dict[schema_name]["nb_versions_to_drop_in_config"] = len(
        drop_versions
    )

    # PARSING API TO GET ALL ELIGIBLE RESOURCES FOR CONSOLIDATION

    df_list = []

    # Listing resources by schema request
    logging.info("Listing from schema...")
    df_schema = parse_api(
        schema_endpoint_base.format(schema_name=schema_name.replace("/", "%2F")),
        api_url,
    )
    logging.info(f"{len(df_schema)} resources found.")
    schemas_report_dict[schema_name]["nb_resources_found_by_schema"] = len(df_schema)
    if len(df_schema) > 0:
        df_schema["resource_found_by"] = "1 - schema request"
        df_schema["initial_version_name"] = df_schema.apply(
            lambda row: get_resource_schema_version(row, api_url),
            axis=1,
        )
        df_list += [df_schema]

    # Listing resources by tag requests
    logging.info("Listing from tags...")
    schemas_report_dict[schema_name]["nb_resources_found_by_tags"] = 0
    for tag in tags_list:
        df_tag = parse_api(
            tag_endpoint_base.format(tag=tag),
            api_url,
        )
        logging.info(f"{len(df_tag)} resources found with tag '{tag}'")
        schemas_report_dict[schema_name]["nb_resources_found_by_tags"] += len(df_tag)
        if len(df_tag) > 0:
            df_tag["resource_found_by"] = "2 - tag request"
            df_list += [df_tag]

    # Listing resources by search (keywords) requests
    logging.info("Listing from keywords...")
    schemas_report_dict[schema_name]["nb_resources_found_by_search_words"] = 0
    for search_word in search_words_list:
        df_search_word = parse_api(
            search_endpoint_base.format(search_word=search_word),
            api_url,
        )
        logging.info(
            f"{len(df_search_word)} resources found with keyword '{search_word}'"
        )
        schemas_report_dict[schema_name]["nb_resources_found_by_search_words"] += len(
            df_search_word
        )
        # if the keyword is not restrictive enough, skipping
        if len(df_search_word) > 0 and len(df_search_word) < 1000:
            df_search_word["resource_found_by"] = "3 - search request"
            df_list += [df_search_word]

    if len(df_list) > 0:
        df = pd.concat(df_list, ignore_index=True)
        df = df[~(df["dataset_id"].isin(datasets_to_exclude))]
        df = df.sort_values("resource_found_by")
        df = df.drop_duplicates(subset=["resource_id"], keep="first")

        logging.info(
            f"🔢 {len(df)} resource(s) found for this schema, "
            f"{len(df.loc[df['error_type'].isna()])} with no inherent error."
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
                df[f"is_valid_v_{version_name}"] = df.apply(
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
                logging.info(f"--- ☑️ Validata check done for version {version_name}")
            else:
                logging.info(
                    f"--- ❌ Version {version_name} to drop according to config file"
                )

        if len(version_names_list) > 0:
            # Check if resources are at least matching one schema version
            # (only those matching will be downloaded in next step)
            df["is_valid_one_version"] = (
                sum(
                    [
                        df[f"is_valid_v_{version_name}"]
                        for version_name in version_names_list
                    ]
                )
                > 0
            )
            schemas_report_dict[schema_name]["nb_valid_resources"] = df[
                "is_valid_one_version"
            ].sum()
            df = add_most_recent_valid_version(df)
            logging.info(df)
            df.to_csv(
                os.path.join(
                    ref_tables_path,
                    f"ref_table_{schema_name.replace('/', '_')}.csv",
                ),
                index=False,
            )
            logging.info(f"-- ✅ Validata check done for {schema_name}.")

        else:
            schemas_report_dict[schema_name]["nb_valid_resources"] = 0
            logging.info(
                "-- ❌ All possible versions for this schema were dropped by config file."
            )
            if should_succeed:
                return False

    else:
        logging.info(f"-- ⚠️ No resource found for {schema_name}.")
        if should_succeed:
            return False
    return True


def download_schema_files(
    schema_name, ref_tables_path, data_path, should_succeed=False
):
    logging.info(f"- ℹ️ STARTING SCHEMA: {schema_name}")

    ref_table_path = os.path.join(
        ref_tables_path,
        build_ref_table_name(schema_name),
    )

    if os.path.exists(ref_table_path):
        df_ref = pd.read_csv(ref_table_path)
        df_ref["is_downloaded"] = False

        if len(df_ref[df_ref["is_valid_one_version"]]) > 0:
            schema_data_path = Path(data_path) / schema_name.replace("/", "_")
            schema_data_path.mkdir(exist_ok=True)

            session = requests.Session()
            for index, row in df_ref[df_ref["is_valid_one_version"]].iterrows():
                rurl = row["resource_url"]
                r = session.get(rurl, allow_redirects=True)

                if r.status_code == 200:
                    p = Path(schema_data_path) / row["dataset_slug"]
                    p.mkdir(exist_ok=True)
                    file_extension = row["resource_extension"]
                    written_filename = f"{row['resource_id']}.{file_extension}"

                    with open(f"{p}/{written_filename}", "wb") as f:
                        f.write(r.content)

                    df_ref.loc[
                        (df_ref["resource_id"] == row["resource_id"]),
                        "is_downloaded",
                    ] = True

                    logging.info(
                        f"--- ⬇️✅ downloaded file [{row['resource_title']}] {rurl}"
                    )
                else:
                    logging.info(
                        f"--- ⬇️❌ File could not be downloaded: [{row['resource_title']}] {rurl}"
                    )
                    logging.info(f"Error looks like this: {r.text}")
            session.close()

        else:
            logging.info("-- ⚠️ No valid resource for this schema")
            if should_succeed:
                return False

        if should_succeed and len(df_ref) == 0:
            return False
        df_ref.to_csv(ref_table_path, index=False)

    else:
        logging.info(
            "-- ❌ No reference table made for this schema (schema not to consolidate, "
            "no version to consolidate or no resource found)."
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
    should_succeed=False,
):
    logging.info(f"- ℹ️ STARTING SCHEMA: {schema_name}")

    schema_data_path = Path(data_path) / schema_name.replace("/", "_")

    if os.path.exists(schema_data_path):
        schema_consolidated_data_path = Path(
            consolidated_data_path
        ) / schema_name.replace("/", "_")
        schema_consolidated_data_path.mkdir(exist_ok=True)

        ref_table_path = os.path.join(
            ref_tables_path,
            build_ref_table_name(schema_name),
        )
        df_ref = pd.read_csv(
            ref_table_path
        )  # (This file necessarily exists if data folder exists)

        # We will test if downloaded files are empty or not (so we set default values)
        df_ref["is_empty"] = np.nan
        df_ref.loc[df_ref["is_downloaded"], "is_empty"] = False

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
                    (df_ref["is_valid_v_" + version_name]) & (df_ref["is_downloaded"])
                ]

                if len(df_ref_v) > 0:
                    # Get schema version parameters for ddup
                    version_dict = requests.get(version["schema_url"])
                    version_dict.raise_for_status()
                    version_dict = version_dict.json()
                    version_all_cols_list = [
                        field_dict["name"] for field_dict in version_dict["fields"]
                    ]
                    version_required_cols_list = [
                        field_dict["name"]
                        for field_dict in version_dict["fields"]
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
                                    encoding = chardet.detect(f.read()).get("encoding")
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
                        except Exception as e:
                            logging.warning(f"Pb on reading resource: {file_path}")
                            logging.warning(e)

                        try:
                            # Remove potential blanks in column names, assert there's no duplicate
                            df_r.columns = [
                                c.replace(" ", "")
                                if c.replace(" ", "") not in df_r.columns
                                else c
                                for c in df_r.columns
                            ]
                            # Remove potential unwanted characters
                            # (eg https://www.data.gouv.fr/fr/datasets/r/67ed303d-1b3a-49d1-afb4-6c0e4318cc20)
                            for c in df_r.columns:
                                df_r[c] = df_r[c].apply(
                                    lambda s: s.replace("\n", "").replace("\r", "")
                                    if isinstance(s, str)
                                    else s
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
                                    [
                                        rq_col in df_r.columns
                                        for rq_col in version_required_cols_list
                                    ]
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
                                    df_r["is_orga"] = row["is_orga"]
                                    df_r["created_at"] = row["resource_created_at"]
                                    # Discard rows where any of the required columns is empty
                                    # (NaN or empty string)
                                    df_r = df_r.loc[
                                        (
                                            ~(
                                                df_r[version_required_cols_list]
                                                .isna()
                                                .any(axis=1)
                                            )
                                        )
                                        & (
                                            ~(
                                                (
                                                    df_r[version_required_cols_list]
                                                    == ""
                                                ).any(axis=1)
                                            )
                                        )
                                    ]
                                    df_r_list += [df_r]
                                else:
                                    logging.info(
                                        f"This file is missing required columns: {file_path}"
                                    )
                                    missing_columns = [
                                        rq_col
                                        for rq_col in version_required_cols_list
                                        if rq_col not in df_r.columns
                                    ]
                                    logging.info(f"> {missing_columns}")
                                    df_ref.loc[
                                        (df_ref["resource_id"] == row["resource_id"]),
                                        "columns_issue",
                                    ] = True
                            else:
                                df_ref.loc[
                                    (df_ref["resource_id"] == row["resource_id"]),
                                    "is_empty",
                                ] = True
                        except Exception as e:
                            logging.warning(f"Pb on cleaning resource: {file_path}")
                            logging.warning(e)

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
                                build_consolidation_name(
                                    schema_name, version_name, consolidation_date_str
                                ),
                            ),
                            index=False,
                            encoding="utf-8",
                        )
                        logging.info(
                            f"-- ✅ DONE: {schema_name} version {version_name}"
                        )

                    else:
                        logging.info(
                            f"-- ⚠️ Less than {MINIMUM_VALID_RESOURCES_TO_CONSOLIDATE}"
                            f" (non-empty) valid resources for version {version_name} :"
                            " consolidation file is not built"
                        )
                        if should_succeed:
                            return False

                else:
                    logging.info(
                        f"-- ⚠️ No valid resource for version {version_name} of this schema"
                    )
                    if should_succeed:
                        return False

        if should_succeed and len(df_ref) == 0:
            return False
        df_ref.to_csv(ref_table_path, index=False)

    else:
        logging.info("-- ❌ No data downloaded for this schema.")
        if should_succeed:
            return False

    with open(tmp_path / "schemas_report_dict.pickle", "wb") as f:
        pickle.dump(schemas_report_dict, f, pickle.HIGHEST_PROTOCOL)

    return True


# Creates a dataset on data.gouv.fr for consolidation files (used only if does not exist yet in config file)
def create_schema_consolidation_dataset(
    schema_name,
    schemas_catalogue_list,
):
    global datasets_description_template, datasets_title_template

    schema_title = get_schema_dict(schema_name, schemas_catalogue_list)["title"]

    dataset = local_client.dataset().create(
        payload={
            "title": datasets_title_template.format(schema_title=schema_title),
            "description": datasets_description_template.format(
                schema_name=schema_name
            ),
            "organization": ORGA_REFERENCE,
            "license": "lov2",
        },
    )

    return dataset


# Generic function to update a field (key) in the config file
def update_config_file(schema_name, key, value, config_path):
    config_dict = load_config(config_path)

    config_dict[schema_name][key] = value

    with open(config_path, "w") as outfile:
        yaml.dump(config_dict, outfile, default_flow_style=False)


# Adds the resource ID of the consolidated file for a given schema version in the config file
def update_config_version_resource_id(schema_name, version_name, r_id, config_path):
    config_dict = load_config(config_path)

    if "latest_resource_ids" not in config_dict[schema_name]:
        config_dict[schema_name]["latest_resource_ids"] = {version_name: r_id}
    else:
        config_dict[schema_name]["latest_resource_ids"][version_name] = r_id

    with open(config_path, "w") as outfile:
        yaml.dump(config_dict, outfile, default_flow_style=False)


# Returns if resource schema (version) metadata should
# be updated or not based on what we know about the resource
def is_schema_version_to_update(row):
    initial_version_name = row["initial_version_name"]
    most_recent_valid_version = row["most_recent_valid_version"]
    resource_found_by = row["resource_found_by"]

    return (resource_found_by == "1 - schema request") and (
        initial_version_name != most_recent_valid_version
    )


# Returns if resource schema (version) metadata should
# be deleted or not based on what we know about the resource
def is_schema_to_drop(row):
    resource_found_by = row["resource_found_by"]
    is_valid_one_version = row["is_valid_one_version"]

    return (resource_found_by == "1 - schema request") and (
        is_valid_one_version is False
    )


def update_resource_metadata(
    schema_name,
    version_name,
    dataset_id,
    resource_id,
    validata_report_path,
    api_url,
    update_version,
    should_succeed,
):
    """
    What we are doing here:
    - if validation_report is made from the resource's metadata (see make_validata_report
    for the logic with hydra), we don't touch anything
    - if the report is from validata:
        * if the resource already has a schema in its metadata:
            > if it's not the same schema as the one we're currently processing (can happen if keywords
            are too vague or for datapackages for instance), we don't touch anything
            > if it's the same schema:
                ¤ if the resource is valid for at least one version, we put the highest valid version
                in schema metadata and the corresponding report in extras
                ¤ if the resource is not valid for any version, we don't touch the schema metadata
                (so no version), and we update the extras with the validation report from the latest version
        * if the resource doesn't have a schema metadata:
            > if the resource is valid for at least one version, we put the highest valid version in
            schema metadata and the corresponding report in extras
            > if the resource is not valid for any version, we don't touch anything
    """
    logging.info("_____________________________")
    if os.path.isfile(validata_report_path):
        schema_from_consolidation = {"name": schema_name, "version": version_name}
        with open(validata_report_path) as out:
            validation_report = json.load(out)
        logging.info(
            f"Updating metadata for {api_url}datasets/{dataset_id}/resources/{resource_id}/"
        )
        if validation_report.get("from_metadata"):
            logging.info("report is made from metadata and unchanged, passing")
            return True

        try:
            url = api_url + f"datasets/{dataset_id}/resources/{resource_id}/"
            r = requests.get(url, headers={"X-fields": "schema"})
            r.raise_for_status()
            schema_from_resource = r.json()["schema"]
            # if the resource already has a schema mentionned in its metadata, we don't
            # change it, we display how it is, but don't throw an error anymore
            if (
                schema_from_resource
                and schema_from_resource.get("name") is not None
                and schema_from_resource.get("name") != schema_name
            ):
                logging.info(
                    f"For resource {api_url}datasets/{dataset_id}/resources/{resource_id}/"
                )
                logging.info(
                    f"Schema metadata error, it looks like this: {schema_from_resource}"
                )
                logging.info(f"while this report is about {schema_name}")
                return True
        except Exception as e:
            logging.warning(
                "abnormal exception (or you're in dev mode (mismatch datagouv URL and ids)? 🧑‍💻)"
            )
            logging.warning(f"Schema: {schema_name}")
            logging.warning(f"URL: {url}")
            logging.warning(f"Error: {e}")
            if should_succeed:
                return False

        reponse_extras = local_client.resource(
            id=resource_id,
            dataset_id=dataset_id,
            fetch=False,
        ).update_extras(payload=validation_report)
        if reponse_extras.status_code != 200:
            logging.warning("🔴 Schema could not be added to extras")
            if should_succeed:
                return False
        else:
            logging.info(f"Schema extras updated with: {validation_report}")

        # not touching metadata if nothing changes
        if (
            update_version
            and schema_from_resource
            and version_name != schema_from_resource.get("version")
        ):
            obj = {"schema": schema_from_consolidation}

            response = local_client.resource(
                id=resource_id,
                dataset_id=dataset_id,
                fetch=False,
            ).update(payload=obj)

            if response.status_code != 200:
                logging.warning("🔴 Schema could not be added on resource")
                if should_succeed:
                    return False
            else:
                logging.info(f"Schema metadata updated with: {obj}")
        else:
            logging.info("Not updating schema metadata")

        return reponse_extras.status_code == 200
    else:
        logging.info(
            f"Validation report for v{version_name} doesn't exist for "
            f"{api_url}datasets/{dataset_id}/resources/{resource_id}/"
        )
    return True


def upload_geojson(
    schema_consolidated_data_path: Path,
    consolidation_date_str: str,
    schema_name: str,
    latest_version: str,
    config_dict: dict,
    should_succeed=False,
) -> bool:
    geojson_version_names_list = sorted(
        [
            filename.replace(
                "consolidation_" + schema_name.replace("/", "_") + "_v_",
                "",
            ).replace("_" + consolidation_date_str + ".geojson", "")
            for filename in os.listdir(schema_consolidated_data_path)
            if filename.endswith(".geojson") and not filename.startswith(".")
        ]
    )
    if len(geojson_version_names_list) > 1:
        logging.warning(f"Multiple versions of GeoJSON found for {schema_name}")

    # Uploading file
    consolidated_dataset_id = config_dict[schema_name]["consolidated_dataset_id"]
    r_id = config_dict[schema_name]["geojson_resource_id"]

    response = local_client.resource(
        id=r_id,
        dataset_id=consolidated_dataset_id,
        fetch=False,
        _from_response={
            "filetype": "file"
        },  # to be able to update the file without fetching
    ).update(
        file_to_upload=File(
            source_path=schema_consolidated_data_path.as_posix(),
            source_name=build_consolidation_name(
                schema_name,
                geojson_version_names_list[-1],
                consolidation_date_str,
                extension="geojson",
            ),
        ).full_source_path,
        payload={
            "type": "main",
            "title": f"Export au format geojson (v{latest_version})",
        },
    )

    if not response.ok:
        logging.warning("--- ⚠️: GeoJSON file could not be uploaded.")
        logging.warning(response.text)
        if should_succeed:
            return False
    else:
        logging.info("--- ✅ Successfully updated GeoJSON file with metadata.")
    return True


def upload_consolidated(
    schema_name,
    consolidated_data_path,
    config_dict,
    schemas_catalogue_list,
    config_path,
    schemas_report_dict,
    consolidation_date_str,
    bool_upload_geojson,
    should_succeed=False,
):
    logging.info(f"- ℹ️ STARTING SCHEMA: {schema_name}")

    schema_consolidated_data_path = Path(consolidated_data_path) / schema_name.replace(
        "/", "_"
    )
    if os.path.exists(schema_consolidated_data_path):
        # Check if dataset_id is in config. If not, create a dataset on datagouv
        schema_config = config_dict[schema_name]
        if schema_config.get("publication"):
            if "consolidated_dataset_id" not in schema_config.keys():
                dataset = create_schema_consolidation_dataset(
                    schema_name,
                    schemas_catalogue_list,
                )
                consolidated_dataset_id = dataset.id
                update_config_file(
                    schema_name,
                    "consolidated_dataset_id",
                    consolidated_dataset_id,
                    config_path,
                )
                logging.info(
                    "-- 🟢 No consolidation dataset for this schema"
                    f" - Successfully created (id: {consolidated_dataset_id})"
                )
            else:
                consolidated_dataset_id = schema_config["consolidated_dataset_id"]

            schemas_report_dict[schema_name]["consolidated_dataset_id"] = (
                consolidated_dataset_id
            )

            # Creating last consolidation resources
            version_names_list = [
                filename.replace(
                    "consolidation_" + schema_name.replace("/", "_") + "_v_",
                    "",
                ).replace("_" + consolidation_date_str + ".csv", "")
                for filename in os.listdir(schema_consolidated_data_path)
                if filename.endswith(".csv") and not filename.startswith(".")
            ]
            if not version_names_list:
                logging.warning(
                    "-- ⚠️ No consolidated file was created for the schema, see consolidate_resources logs"
                )
                return not should_succeed
            sorted_version = sorted(version_names_list, key=comparer_versions)
            latest_mapping = {"latest": sorted_version[-1]}
            sorted_version.append("latest")
            for version_name in sorted_version:
                time.sleep(2)

                # Uploading file (creating a new resource if version was not there before)
                kwargs = {
                    "file_to_upload": File(
                        source_path=schema_consolidated_data_path.as_posix(),
                        source_name=build_consolidation_name(
                            schema_name,
                            latest_mapping.get(version_name, version_name),
                            consolidation_date_str,
                        ),
                    ).full_source_path,
                    "payload": {
                        "schema": {
                            "name": schema_name,
                            "version": latest_mapping.get(version_name, version_name),
                        },
                        "type": "main",
                        "title": (
                            "Consolidation de la v{} du schéma - {}".format(
                                version_name, consolidation_date_str
                            )
                            if version_name != "latest"
                            else "Consolidation de la dernière version à date du schéma (v{}) - {}".format(
                                latest_mapping["latest"], consolidation_date_str
                            )
                        ),
                        "format": "csv",
                    },
                }
                try:
                    r_id = config_dict[schema_name]["latest_resource_ids"][version_name]
                    local_client.resource(
                        id=r_id,
                        dataset_id=consolidated_dataset_id,
                        fetch=False,
                        _from_response={
                            "filetype": "file"
                        },  # to be able to update the file without fetching
                    ).update(**kwargs)
                    logging.info(
                        f"--- ✅ Updated consolidation for {schema_name} v{version_name} (id: {r_id})"
                    )
                except KeyError:
                    kwargs["dataset_id"] = consolidated_dataset_id
                    resource = local_client.resource().create_static(**kwargs)
                    update_config_version_resource_id(
                        schema_name, version_name, resource.id, config_path
                    )
                    logging.info(
                        f"--- ➕ New latest resource ID created for {schema_name} v{version_name} (id: {resource.id})"
                    )

            # Upload GeoJSON file (e.g IRVE)
            if bool_upload_geojson:
                upload_success = upload_geojson(
                    schema_consolidated_data_path,
                    consolidation_date_str,
                    schema_name,
                    latest_mapping["latest"],
                    config_dict,
                    should_succeed,
                )
        else:
            schemas_report_dict[schema_name]["consolidated_dataset_id"] = np.nan
            logging.info("-- ❌ No publication for this schema.")
            if should_succeed:
                return False

    else:
        schemas_report_dict[schema_name]["consolidated_dataset_id"] = np.nan
        logging.info("-- ❌ No consolidated file for this schema.")
        if should_succeed:
            return False
    if should_succeed:
        return upload_success
    return True


def update_reference_table(ref_tables_path, schema_name, should_succeed=False):
    # this is done after table is uploaded, in order not to publish these columns
    ref_table_path = os.path.join(
        ref_tables_path,
        build_ref_table_name(schema_name),
    )

    if os.path.isfile(ref_table_path):
        df_ref = pd.read_csv(ref_table_path)

        df_ref["is_schema_version_to_update"] = df_ref.apply(
            is_schema_version_to_update, axis=1
        )
        df_ref["is_schema_to_drop"] = df_ref.apply(is_schema_to_drop, axis=1)

        df_ref.to_csv(ref_table_path, index=False)

        logging.info(f"- ✅ Infos added for schema {schema_name}")

    else:
        logging.info(f"- ❌ No reference table for schema {schema_name}")
        if should_succeed:
            return False
    return True


# Get the (list of) e-mail address(es) of the owner or of the admin(s) of the owner organization of a dataset
def get_owner_or_admin_mails(dataset_id, api_url, headers):
    r = requests.get(api_url + f"datasets/{dataset_id}/")
    r.raise_for_status()
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

    session = requests.Session()
    if org_id is not None:
        mails_type = "organisation_admins"
        r_org = session.get(api_url + f"organizations/{org_id}/")
        r_org.raise_for_status()
        members_list = r_org.json()["members"]
        for member in members_list:
            if member["role"] == "admin":
                user_id = member["user"]["id"]
                r_user = session.get(api_url + f"users/{user_id}/", headers=headers)
                r_user.raise_for_status()
                user_mail = r_user.json()["email"]
                mails_list += [user_mail]

    else:
        if owner_id is not None:
            mails_type = "owner"
            r_user = session.get(api_url + f"users/{owner_id}/", headers=headers)
            r_user.raise_for_status()
            user_mail = r_user.json()["email"]
            mails_list += [user_mail]
    session.close()
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


def update_resource_send_mail_producer(
    ref_tables_path,
    schema_name,
    validata_reports_path,
    should_succeed=False,
    send_mails=False,
):
    logging.info(f"- ℹ️ STARTING SCHEMA: {schema_name}")

    ref_table_path = os.path.join(
        ref_tables_path,
        build_ref_table_name(schema_name),
    )

    if os.path.isfile(ref_table_path):
        df_ref = pd.read_csv(ref_table_path)
        df_ref["resource_schema_update_success"] = np.nan
        df_ref["producer_notification_success"] = np.nan

        for idx, row in df_ref.iterrows():
            validata_report_path = build_report_prefix(
                validata_reports_path,
                schema_name,
                row["dataset_id"],
                row["resource_id"],
            )
            # whether or not to update the version in schema metadata
            update_version = False
            # If there is a valid version, put validata report from it
            if isinstance(row["most_recent_valid_version"], str):
                validata_report_path += row["most_recent_valid_version"] + ".json"
                update_version = True
            # Else, check if declarative version ; not setting the version in metadata
            else:
                # If so, put validation report from it, if it exists
                if isinstance(row["initial_version_name"], str) and os.path.isfile(
                    validata_report_path + row["initial_version_name"] + ".json"
                ):
                    validata_report_path += row["initial_version_name"] + ".json"
                # If not, put validation report from latest version
                else:
                    validata_report_path += (
                        max(
                            [
                                x.replace("is_valid_v_", "")
                                for x in list(row.keys())
                                if "is_valid_v_" in x
                            ],
                            key=comparer_versions,
                        )
                        + ".json"
                    )
            if row["is_schema_version_to_update"] or row["is_valid_one_version"]:
                resource_update_success = update_resource_metadata(
                    schema_name=schema_name,
                    version_name=row["most_recent_valid_version"],
                    dataset_id=row["dataset_id"],
                    resource_id=row["resource_id"],
                    validata_report_path=validata_report_path,
                    api_url=api_url,
                    update_version=update_version,
                    should_succeed=should_succeed,
                )
                df_ref.loc[
                    (df_ref["resource_id"] == row["resource_id"]),
                    "resource_schema_update_success",
                ] = resource_update_success

            # for now no email is sent
            if send_mails:
                if resource_update_success and (
                    row["is_schema_version_to_update"] or row["is_valid_one_version"]
                ):
                    # title = "Mise à jour de la version de la métadonnée schéma"
                    # comment = updated_schema_comment_template.format(
                    #     resource_title=row["resource_title"],
                    #     schema_name=schema_name,
                    #     initial_version_name=row["initial_version_name"],
                    #     most_recent_valid_version=row["most_recent_valid_version"],
                    # )
                    # comment_post = post_comment_on_dataset(dataset_id=row['dataset_id'],
                    #                                       title=title,
                    #                                       comment=comment,
                    #                                       api_url=api_url
                    #                                      )
                    #
                    # producer_notification_success = (comment_post.status_code == 201)

                    # df_ref.loc[
                    #     (df_ref['resource_id'] == row['resource_id']),
                    #     'producer_notification_success'
                    # ] = producer_notification_success
                    # No notification at the moment:
                    df_ref.loc[
                        (df_ref["resource_id"] == row["resource_id"]),
                        "producer_notification_success",
                    ] = False

                    # title = "Ajout de la métadonnée schéma"
                    # comment = added_schema_comment_template.format(
                    #     resource_title=row["resource_title"],
                    #     schema_name=schema_name,
                    #     most_recent_valid_version=row["most_recent_valid_version"],
                    # )
                    # comment_post = post_comment_on_dataset(dataset_id=row['dataset_id'],
                    #                                       title=title,
                    #                                       comment=comment,
                    #                                       api_url=api_url
                    #                                      )
                    #
                    # producer_notification_success = (comment_post.status_code == 201)
                    # df_ref.loc[
                    #     (df_ref['resource_id'] == row['resource_id']),
                    #     'producer_notification_success'
                    # ] = producer_notification_success
                    # No notification at the moment:
                    df_ref.loc[
                        (df_ref["resource_id"] == row["resource_id"]),
                        "producer_notification_success",
                    ] = False

                # Right now, we don't drop schema and do no notification
                # elif row["is_schema_to_drop"]:
                #    resource_update_success = delete_resource_schema(
                #        api_url,
                #        row['dataset_id'],
                #        row['resource_id'],
                #        schema_name,
                #        headers
                #     )
                #    df_ref.loc[
                #        (df_ref['resource_id'] == row['resource_id']),
                #        'resource_schema_update_success'
                #     ] = resource_update_success
                #
                #    if resource_update_success:
                #        title = 'Suppression de la métadonnée schéma'
                #
                #       mails_type, mails_list = get_owner_or_admin_mails(
                #           row['dataset_id'],
                #           api_url,
                #           headers
                #        )
                #
                #        if len(mails_list) > 0 : #If we found some email addresses, we send mails
                #
                #            if mails_type == 'organisation_admins' :
                #               message = deleted_schema_mail_template_org.format(
                #                    organisation_name=row['organization_or_owner'],
                #                    dataset_title=row['dataset_title'],
                #                    resource_title=row['resource_title'],
                #                    schema_name=schema_name,
                #                    schema_url=get_schema_dict(
                #                        schema_name,
                #                        schemas_catalogue_list
                #                    )['schema_url'],
                #                    resource_url=row['resource_url']
                #                )
                #            elif mails_type == 'owner' :
                #               message = deleted_schema_mail_template_own.format(
                #                    dataset_title=row['dataset_title'],
                #                    resource_title=row['resource_title'],
                #                    schema_name=schema_name,
                #                    schema_url=get_schema_dict(
                #                        schema_name,
                #                        schemas_catalogue_list
                #                    )['schema_url'],
                #                    resource_url=row['resource_url']
                #                )
                #
                #
                #            #Sending mail
                #
                #            producer_notification_success_list = []
                #            logging.info('- {} | {}:'.format(row['dataset_title'], row['resource_title']))
                #            for mail_to in mails_list :
                #                #mail_send = send_email(subject=title,
                #                #                       message=message,
                #                #                       mail_from=mail_from,
                #                #                       mail_to=mail_to,
                #                #                       smtp_host=smtp_host,
                #                #                       smtp_user=smtp_user,
                #                #                       smtp_password=smtp_password)

                #                #producer_notification_success_list += [(mail_send.status_code == 250)]
                #            # Success if at least one person receives the mail
                #            #producer_notification_success = any(producer_notification_success_list)
                #
                #        else : #If no mail address, we post a comment on dataset
                #           comment = deleted_schema_comment_template.format(
                #                resource_title=row['resource_title'],
                #                schema_name=schema_name,
                #                schema_url=get_schema_dict(
                #                    schema_name,
                #                    schemas_catalogue_list
                #                )['schema_url'],
                #                resource_url=row['resource_url']
                #            )
                #
                #           comment_post = post_comment_on_dataset(
                #                dataset_id=row['dataset_id'],
                #                title=title,
                #                comment=comment,
                #                api_url=api_url
                #            )
                #
                #            #producer_notification_success = (comment_post.status_code == 201)
                #
                #       df_ref.loc[
                #           (df_ref['resource_id'] == row['resource_id']),
                #           'producer_notification_success'
                #        ] = producer_notification_success

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

        logging.info(f"- ✅ Resources updated for schema {schema_name}")

    else:
        logging.info(f"- ❌ No reference table for schema {schema_name}")
        if should_succeed:
            return False
    return True


def update_consolidation_documentation_report(
    schema_name,
    ref_tables_path,
    config_path,
    consolidation_date_str,
    config_dict,
    should_succeed=False,
):
    ref_table_path = os.path.join(
        ref_tables_path,
        build_ref_table_name(schema_name),
    )

    logging.info(f"- ℹ️ STARTING SCHEMA: {schema_name}")

    schema_config = config_dict[schema_name]
    if schema_config.get("publication"):
        if os.path.isfile(ref_table_path):
            if "consolidated_dataset_id" in schema_config.keys():
                consolidated_dataset_id = schema_config["consolidated_dataset_id"]
                kwargs = {
                    "file_to_upload": File(
                        source_path=ref_tables_path,
                        source_name=build_ref_table_name(schema_name),
                    ).full_source_path,
                    "payload": {
                        "type": "documentation",
                        "title": f"Documentation sur la consolidation - {consolidation_date_str}",
                    },
                }
                # Uploading documentation file (creating a new resource if version was not there before)
                try:
                    doc_r_id = config_dict[schema_name]["documentation_resource_id"]
                    response = local_client.resource(
                        id=doc_r_id,
                        dataset_id=consolidated_dataset_id,
                        fetch=False,
                        _from_response={
                            "filetype": "file"
                        },  # to be able to update the file without fetching
                    ).update(**kwargs)
                    if response.ok:
                        logging.info(
                            f"--- ✅ Updated documentation resource  for {schema_name} (id: {doc_r_id})"
                        )
                    else:
                        logging.warning(
                            "--- ⚠️ Documentation file could not be uploaded."
                        )
                        if should_succeed:
                            return False
                except KeyError:
                    kwargs["dataset_id"] = consolidated_dataset_id
                    resource = local_client.resource().create_static(**kwargs)
                    update_config_file(
                        schema_name,
                        "documentation_resource_id",
                        resource.id,
                        config_path,
                    )
                    logging.info(
                        f"--- ➕ New documentation resource created for {schema_name} (id: {resource.id})"
                    )

            else:
                logging.info("-- ❌ No consolidation dataset ID for this schema.")
                if should_succeed:
                    return False

        else:
            logging.info("-- ❌ No reference table for this schema.")
            if should_succeed:
                return False

    else:
        logging.info("-- ❌ No publication for this schema.")
        if should_succeed:
            return False
    return True


def append_stats_list(ref_tables_path, schema_name, stats_df_list):
    ref_table_path = os.path.join(
        ref_tables_path,
        build_ref_table_name(schema_name),
    )

    if os.path.isfile(ref_table_path):
        df_ref = pd.read_csv(ref_table_path)
        df_ref["schema_name"] = schema_name
        df_ref["is_schema_version_updated"] = (
            df_ref["is_schema_version_to_update"]
            & df_ref["resource_schema_update_success"]
        )
        df_ref["is_schema_added"] = (
            df_ref["is_valid_one_version"] & df_ref["resource_schema_update_success"]
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
                    "is_valid_one_version",
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
    ref_tables_path, schema_name: str, report_tables_path, should_succeed: bool = False
):
    ref_table_path = os.path.join(
        ref_tables_path,
        build_ref_table_name(schema_name),
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

        logging.info(f"- ✅ Report done for schema {schema_name}")

    else:
        logging.info(f"- ❌ No reference table for schema {schema_name}")
        if should_succeed:
            return False
    return True


def final_directory_clean_up(
    tmp_folder: str,
    output_data_folder: str,
):
    shutil.move(tmp_folder + "consolidated_data", output_data_folder)
    shutil.move(tmp_folder + "ref_tables", output_data_folder)
    shutil.move(tmp_folder + "report_tables", output_data_folder)


def upload_minio(
    TMP_FOLDER: str,
    MINIO_BUCKET_DATA_PIPELINE_OPEN: str,
    minio_output_filepath: str,
):
    minio_open = MinIOClient(bucket=MINIO_BUCKET_DATA_PIPELINE_OPEN)
    minio_open.send_files(
        list_files=[
            File(
                source_path=path,
                source_name=name,
                dest_path=(minio_output_filepath + path).replace(TMP_FOLDER, ""),
                dest_name=name,
            )
            for path, subdirs, files in os.walk(TMP_FOLDER + "/output/")
            for name in files
            if os.path.isfile(os.path.join(path, name))
        ],
        ignore_airflow_env=True,
    )
    return


def notification_synthese(
    MINIO_URL: str,
    MINIO_BUCKET_DATA_PIPELINE_OPEN: str,
    TMP_FOLDER: Path,
    MATTERMOST_DATAGOUV_SCHEMA_ACTIVITE: str,
    schema_name: str = "",
    list_schema_skip: list = [],
):
    """
    For single schema processing (e.g IRVE): specify schema_name as string
    For general case: specify list_schema_skip as a list of schemas to ignore
    """
    assert schema_name or len(list_schema_skip) > 0

    last_conso = datetime.today().strftime("%Y-%m-%d")
    r = requests.get("https://schema.data.gouv.fr/schemas/schemas.json")
    r.raise_for_status()
    schemas = r.json()["schemas"]
    minio_open = MinIOClient(bucket=MINIO_BUCKET_DATA_PIPELINE_OPEN)

    message = (
        ":mega: *Rapport sur la consolidation des données répondant à un schéma.*\n"
    )

    if schema_name:
        schemas = [s for s in schemas if s["name"] == schema_name]
    else:
        schemas = [s for s in schemas if s["name"] not in list_schema_skip]
    for s in schemas:
        if s["schema_type"] == "tableschema":
            try:
                filename = (
                    f"https://{MINIO_URL}/{MINIO_BUCKET_DATA_PIPELINE_OPEN}/schema/schemas_consolidation/"
                    f"{last_conso}/output/ref_tables/ref_table_{s['name'].replace('/', '_')}.csv"
                )
                df = pd.read_csv(filename)
                nb_declares = df[df["resource_found_by"] == "1 - schema request"].shape[
                    0
                ]
                nb_suspectes = df[
                    df["resource_found_by"] != "1 - schema request"
                ].shape[0]
                nb_valides = df[df["is_valid_one_version"]].shape[0]
                # if error_type is not empty, the error is inherent to the file or the context
                # not to its content, so we don't want it here
                df = df[(~(df["is_valid_one_version"])) & df["error_type"].isna()]
                df = df[
                    [
                        "dataset_id",
                        "resource_id",
                        "dataset_title",
                        "resource_title",
                        "organization_or_owner",
                        "resource_created_at",
                        "publish_source",
                        "dataset_page",
                        "resource_url",
                        "resource_found_by",
                    ]
                ]
                df["schema_name"] = s["title"]
                df["schema_id"] = s["name"]
                df["validata_report"] = (
                    "https://preprod-validata.dataeng.etalab.studio/table-schema?input=url&url="
                    f"{df['resource_url']}&schema_url={s['schema_url']}"
                )
                erreurs_file_name = f"liste_erreurs-{s['name'].replace('/', '_')}.csv"
                df.to_csv(f"{TMP_FOLDER}/{erreurs_file_name}", index=False)

                minio_open.send_files(
                    list_files=[
                        File(
                            source_path=f"{TMP_FOLDER}/",
                            source_name=erreurs_file_name,
                            dest_path="schema/schemas_consolidation/liste_erreurs/",
                            dest_name=erreurs_file_name,
                        )
                    ],
                    ignore_airflow_env=True,
                )

                message += f"\n- Schéma ***{s['title']}***\n - Ressources déclarées : {nb_declares}"

                if nb_suspectes != 0:
                    message += f"\n - Ressources suspectées : {nb_suspectes}"

                message += (
                    f"\n - Ressources valides : {nb_valides} \n - [Liste des ressources non valides]"
                    f"(https://{MINIO_URL}/{MINIO_BUCKET_DATA_PIPELINE_OPEN}/schema/"
                    f"schemas_consolidation/liste_erreurs/{erreurs_file_name})\n"
                )
            except Exception as e:
                logging.warning(f"{s['name']} erreur : {e}")
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
