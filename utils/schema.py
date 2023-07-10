from datagouvfr_data_pipelines.utils.datagouv import get_all_from_api_query
import pandas as pd
import requests
import json
from json import JSONDecodeError
import os
from datetime import datetime
import time
from unidecode import unidecode


# API parsing to get resources infos based on schema metadata, tags and search keywords
def parse_api_search(url: str, api_url: str, schema_name: str) -> pd.DataFrame:
    all_results = get_all_from_api_query(url)
    arr = []
    for dataset_result in all_results:
        r = requests.get(api_url + "datasets/" + dataset_result["id"])
        r.raise_for_status()
        dataset = r.json()
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


# API parsing to get resources infos based on schema metadata, tags and search keywords
def parse_api(url: str, schema_name: str) -> pd.DataFrame:
    all_datasets = get_all_from_api_query(url)
    arr = []
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