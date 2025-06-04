import yaml
from git import Repo, Git
import os
import shutil
import logging
from table_schema_to_markdown import convert_source, sources_to_markdown
import frictionless
import json
import jsonschema
import re
from unidecode import unidecode
import codecs
import requests
from urllib import parse
from datetime import datetime
from feedgen.feed import FeedGenerator
import xml.etree.ElementTree as ET
import pytz
import pandas as pd

from datagouvfr_data_pipelines.utils.schema import comparer_versions
from datagouvfr_data_pipelines.schema.utils.jsonschema import jsonschema_to_markdown
from datagouvfr_data_pipelines.utils.datagouv import demo_client, prod_client
from datagouvfr_data_pipelines.utils.filesystem import File

ERRORS_REPORT = []
SCHEMA_INFOS = {}
SCHEMA_CATALOG = {}


def initialization(ti, tmp_folder, branch):
    # DAG_NAME is defined in the DAG file, and all paths are made from it
    OUTPUT_DATA_FOLDER = f"{tmp_folder}output/"
    CACHE_FOLDER = tmp_folder + "cache"
    DATA_FOLDER1 = tmp_folder + "data"
    DATA_FOLDER2 = tmp_folder + "data2"
    folders = {
        "OUTPUT_DATA_FOLDER": OUTPUT_DATA_FOLDER,
        "CACHE_FOLDER": CACHE_FOLDER,
        "DATA_FOLDER1": DATA_FOLDER1,
        "DATA_FOLDER2": DATA_FOLDER2,
    }

    LIST_SCHEMAS_YAML = (
        "https://raw.githubusercontent.com/etalab/"
        f"schema.data.gouv.fr/{branch}/repertoires.yml"
    )

    # Loading yaml file containing all schemas that we want to display in schema.data.gouv.fr
    r = requests.get(LIST_SCHEMAS_YAML)

    with open(tmp_folder + "repertoires.yml", "wb") as f:
        f.write(r.content)

    with open(tmp_folder + "repertoires.yml", "r") as f:
        config = yaml.safe_load(f)

    ti.xcom_push(key="folders", value=folders)
    ti.xcom_push(key="config", value=config)


def clean_and_create_folder(folder: str) -> None:
    """Remove local folder if exist and (re)create it"""
    if os.path.exists(folder):
        shutil.rmtree(folder)
    os.mkdir(folder)


def get_consolidated_version(tag) -> tuple[str, bool]:
    """Analyze tag from a code source release, cast it to acceptable semver version X.X.X"""
    valid_version = True
    # Removing "v" or "V" from tag
    version_items = str(tag).replace("v", "").replace("V", "").split(".")
    # Add a patch number if only 2 items
    if len(version_items) == 2:
        version_items.append("0")
    # If more than 3, do not accept tag
    if len(version_items) > 3:
        valid_version = False
    # Verify if all items are digits
    for v in version_items:
        if not v.isdigit():
            valid_version = False
    # Return semver version and validity of it
    return ".".join(version_items), valid_version


def manage_errors(repertoire_slug: str, version: str, reason: str):
    """Create dictionnary that will populate ERRORS_REPORT object"""
    ERRORS_REPORT.append({
        "schema": repertoire_slug,
        "version": version,
        "type": reason,
    })


def check_schema(repertoire_slug: str, conf: dict, schema_type: str, folders: dict) -> dict:
    global SCHEMA_INFOS
    """Check validity of schema and all of its releases"""
    # schema_name in schema.data.gouv.fr is referenced by group and repo name in Git.
    # Ex : etalab / schema-irve-statique
    schema_name = "/".join(conf["url"].split(".git")[0].split("/")[-2:])
    # define source folder and create it
    # source folder will help us to checkout to every release and analyze source code for each one
    src_folder = folders["CACHE_FOLDER"] + "/" + schema_name + "/"
    os.makedirs(src_folder, exist_ok=True)
    # clone repo in source folder
    Repo.clone_from(conf["url"], src_folder)
    repo = Repo(src_folder)
    # get tags of repo
    tags = sorted(repo.tags, key=lambda t: t.commit.committed_datetime)

    list_schemas = {}

    # Defining SCHEMA_INFOS object for website use
    SCHEMA_INFOS[schema_name] = {
        "homepage": conf["url"],
        "external_doc": conf.get("external_doc"),
        "external_tool": conf.get("external_tool"),
        "type": conf["type"],
        "email": conf["email"],
        "labels": conf.get("labels"),
        "consolidation_dataset_id": conf.get("consolidation"),
        "versions": {},
    }

    # for every tag
    for t in tags:
        logging.info(f"version {t}")
        # get semver version and validity of it
        version, valid_version = get_consolidated_version(t)
        # if semver ok
        if valid_version:
            # define destination folder and create it
            # destination folder will store pertinents files for website for each version of each schema
            dest_folder = f"{folders['DATA_FOLDER1']}/{schema_name}/{version}/"
            os.makedirs(dest_folder, exist_ok=True)
            # checkout to current version
            g = Git(src_folder)
            g.checkout(str(t))

            conf_schema = None
            # Managing validation differently for each type of schema
            # tableschema will use frictionless package
            # jsonschema will use jsonschema package
            # other will only check if schema.yml file is present and contains correct information
            if schema_type == "tableschema":
                list_schemas: dict = manage_tableschema(
                    src_folder,
                    dest_folder,
                    list_schemas,
                    version,
                    schema_name,
                    repertoire_slug,
                )
            if schema_type == "jsonschema":
                list_schemas, conf_schema = manage_jsonschema(
                    src_folder,
                    dest_folder,
                    list_schemas,
                    version,
                    schema_name,
                    repertoire_slug,
                )
            if schema_type == "other":
                list_schemas, conf_schema = manage_other(
                    src_folder,
                    dest_folder,
                    list_schemas,
                    version,
                    schema_name,
                    repertoire_slug,
                )
    if not list_schemas:
        logging.warning("No valid version for this schema")
        return
    # Find latest valid version and create a specific folder "latest" copying files in it (for website use)
    latest_folder, sf = manage_latest_folder(schema_name, folders)
    # Indicate in schema_info object name of latest schema
    SCHEMA_INFOS[schema_name]["latest"] = sf
    schema_file = list_schemas[sf]
    # Complete catalog with all relevant information of schema in it
    logging.info(f"conf:  {conf}")
    logging.info(f"conf_schema: {conf_schema}")
    if conf_schema:
        conf.update(conf_schema)
    schema_to_add_to_catalog: dict = generate_catalog_object(
        latest_folder,
        list_schemas,
        schema_file,
        schema_type,
        schema_name,
        folders,
        conf,
    )
    return schema_to_add_to_catalog


def check_datapackage(conf, folders):
    global SCHEMA_INFOS
    """Check validity of schemas from a datapackage repo for all of its releases"""
    # define source folder and create it
    # source folder will help us to checkout to every release and analyze source code for each one
    dpkg_name = "/".join(conf["url"].split(".git")[0].split("/")[-2:])
    src_folder = (
        f"{folders['CACHE_FOLDER']}/{'/'.join(conf['url'].split('.git')[0].split('/')[-2:])}/"
    )
    os.makedirs(src_folder, exist_ok=True)
    # clone repo in source folder
    Repo.clone_from(conf["url"], src_folder)
    repo = Repo(src_folder)
    # get tags of repo
    tags = sorted(repo.tags, key=lambda t: t.commit.committed_datetime)

    list_schemas = {}
    schemas_to_add_to_catalog = []

    # Defining SCHEMA_INFOS object for website use
    SCHEMA_INFOS[dpkg_name] = {
        "homepage": conf["url"],
        "external_doc": conf.get("external_doc"),
        "external_tool": conf.get("external_tool"),
        "type": conf["type"],
        "email": conf.get("email"),
        "labels": conf.get("labels"),
        "consolidation_dataset_id": conf.get("consolidation"),
        "versions": {},
        "schemas": [],
    }
    one_valid = False
    frictionless_report = None
    for t in tags:
        logging.info(f"version {t}")
        # get semver version and validity of it
        version, valid_version = get_consolidated_version(t)
        # if semver ok
        if valid_version:
            g = Git(src_folder)
            g.checkout(str(t))

            SCHEMA_INFOS[dpkg_name]["versions"][version] = {"pages": []}

            dest_folder = f"{folders['DATA_FOLDER1']}/{dpkg_name}/{version}/"
            os.makedirs(dest_folder, exist_ok=True)
            for f in ["README.md", "SEE_ALSO.md", "CHANGELOG.md", "CONTEXT.md", "datapackage.json"]:
                if os.path.isfile(src_folder + f):
                    shutil.copyfile(src_folder + f, dest_folder + f)
                    if f != "datapackage.json":
                        SCHEMA_INFOS[dpkg_name]["versions"][version]["pages"].append(f)
                    else:
                        SCHEMA_INFOS[dpkg_name]["versions"][version]["schema_url"] = (
                            f"/{dpkg_name}/{version}/datapackage.json"
                        )

            # Verify that a file datapackage.json is present
            if os.path.isfile(src_folder + "datapackage.json"):
                # Validate it with frictionless package
                frictionless_report = frictionless.validate(src_folder + "datapackage.json")
                # If datapackage release is valid, then
                if frictionless_report.valid:
                    one_valid = True
                    with open(src_folder + "datapackage.json") as out:
                        dp = json.load(out)

                    schemas_dp = [
                        r["schema"] for r in dp["resources"]
                        if "schema" in r and isinstance(r["schema"], str)
                    ]

                    for schema in schemas_dp:

                        with open(src_folder + schema) as out:
                            schema_json = json.load(out)

                        schema_name = dpkg_name.split("/")[0] + "/" + schema_json["name"]

                        if schema_name not in list_schemas:
                            list_schemas[schema_name] = {}

                        list_schemas[schema_name][version] = schema.split("/")[-1]

                        if schema_name not in SCHEMA_INFOS:
                            # Defining SCHEMA_INFOS object for website use
                            SCHEMA_INFOS[schema_name] = {
                                "homepage": conf["url"],
                                "external_doc": conf.get("external_doc"),
                                "external_tool": conf.get("external_tool"),
                                "type": "tableschema",
                                "email": conf.get("email"),
                                "versions": {},
                                "datapackage": dp["title"],
                                "datapackage_id": dp["name"],
                                "labels": conf.get("labels"),
                                "consolidation_dataset_id": conf.get("consolidation"),
                            }

                        SCHEMA_INFOS[schema_name]["versions"][version] = {"pages": []}

                        # define destination folder and create it
                        # destination folder will store pertinents files for website
                        # for each version of each schema

                        schema_dest_folder = (
                            folders["DATA_FOLDER1"] + "/" + schema_name + "/" + version + "/"
                        )
                        if len(schema.split("/")) > 1:
                            os.makedirs(schema_dest_folder, exist_ok=True)
                        shutil.copyfile(src_folder + schema, schema_dest_folder + schema.split("/")[-1])
                        SCHEMA_INFOS[schema_name]["versions"][version]["schema_url"] = (
                            f"/{schema_name}/{version}/{schema.split('/')[-1]}"
                        )
                        for f in ["README.md", "SEE_ALSO.md", "CHANGELOG.md", "CONTEXT.md"]:
                            if os.path.isfile(src_folder + "/".join(schema.split("/")[:-1]) + "/" + f):
                                shutil.copyfile(
                                    src_folder + "/".join(schema.split("/")[:-1]) + "/" + f,
                                    schema_dest_folder + f
                                )
                                SCHEMA_INFOS[schema_name]["versions"][version]["pages"].append(f)

                        # Create documentation file and save it
                        with open(schema_dest_folder + "/" + "documentation.md", "w") as out:
                            # From schema.json, we use tableschema_to_markdown package to convert it in a
                            # readable mardown file that will be use for documentation
                            convert_source(schema_dest_folder + schema.split("/")[-1], out, "page")
                            SCHEMA_INFOS[schema_name]["versions"][version]["pages"].append(
                                "documentation.md"
                            )

                        # Create sources file
                        with open(schema_dest_folder + schema.split("/")[-1], "r") as f:
                            schema_content = json.load(f)
                        if schema_content.get("sources"):
                            sources_md = sources_to_markdown(schema_content)
                            with open(schema_dest_folder + "/" + "sources.md", "w") as out:
                                out.write(sources_md)
                            SCHEMA_INFOS[schema_name]["versions"][version]["pages"].append(
                                "sources.md"
                            )

                        latest_folder, sf = manage_latest_folder(schema_name, folders)
                else:
                    logging.warning("not valid")
            else:
                logging.warning("no datapackage.json file")
    if not one_valid:
        logging.warning("No valid version for this datapackage, see report:")
        logging.info(frictionless_report)
        return
    # Find latest valid version and create a specific folder "latest" copying files in it (for website use)
    latest_folder, sf = manage_latest_folder(dpkg_name, folders)

    for schema in schemas_dp:
        with open(src_folder + schema) as out:
            schema_json = json.load(out)
        # Complete catalog with all relevant information of schema in it
        statc = generate_catalog_object(
            folders["DATA_FOLDER1"] + "/" + dpkg_name.split("/")[0] + "/" + schema_json["name"] + "/latest/",
            list_schemas[dpkg_name.split("/")[0] + "/" + schema_json["name"]],
            schema.split("/")[-1],
            "tableschema",
            dpkg_name,
            folders,
            conf,
            dp
        )
        schemas_to_add_to_catalog.append(statc)
        SCHEMA_INFOS[dpkg_name.split("/")[0] + "/" + schema_json["name"]]["latest"] = sf
        SCHEMA_INFOS[dpkg_name]["schemas"].append(dpkg_name.split("/")[0] + "/" + schema_json["name"])

    SCHEMA_INFOS[dpkg_name]["latest"] = sf

    statc = generate_catalog_datapackage(
        latest_folder,
        dpkg_name,
        conf,
        list_schemas[dpkg_name.split("/")[0] + "/" + schema_json["name"]],
    )
    schemas_to_add_to_catalog.append(statc)

    # schemas_to_add_to_catalog.append(schema_to_add_to_catalog)
    return schemas_to_add_to_catalog


def manage_tableschema(
    src_folder: str,
    dest_folder: str,
    list_schemas: dict,
    version: str,
    schema_name: str,
    repertoire_slug: str,
    schema_file: str = "schema.json",
) -> dict:
    """Check validity of a schema release from tableschema type"""
    # Verify that a file schema.json is present
    if os.path.isfile(src_folder + schema_file):
        # Validate it with frictionless package
        frictionless_report = frictionless.validate(src_folder + schema_file)
        # If schema release is valid, then
        if frictionless_report.valid:
            list_schemas[version] = schema_file
            # We complete info of version
            SCHEMA_INFOS[schema_name]["versions"][version] = {"pages": []}
            subfolder = "/".join(schema_file.split("/")[:-1]) + "/"
            if subfolder == "/":
                subfolder = ""
            else:
                os.makedirs(dest_folder + subfolder, exist_ok=True)
            # We check for list of normalized files if it is present in source code
            # if so, we copy paste them into dest folder
            for f in [schema_file, "README.md", "SEE_ALSO.md", "CHANGELOG.md", "CONTEXT.md"]:
                if os.path.isfile(src_folder + subfolder + f):
                    logging.info(f"schema has {f}")
                    shutil.copyfile(src_folder + subfolder + f, dest_folder + f)
                    # if it is a markdown file, we will read it as page in website
                    if f[-3:] == ".md":
                        SCHEMA_INFOS[schema_name]["versions"][version]["pages"].append(f)
                    # if it is the schema, we indicate it as it in object
                    if f == schema_file:
                        SCHEMA_INFOS[schema_name]["versions"][version]["schema_url"] = (
                            f"/{schema_name}/{version}/{schema_file}"
                        )
            # Create documentation file and save it
            with open(dest_folder + "documentation.md", "w") as out:
                # From schema.json, we use tableschema_to_markdown package to convert it
                # into a readable mardown file that will be use as documentation
                convert_source(dest_folder + schema_file, out, "page")
                SCHEMA_INFOS[schema_name]["versions"][version]["pages"].append("documentation.md")

            # Create sources file
            with open(dest_folder + schema_file, "r") as f:
                schema_content = json.load(f)
            sources_md = sources_to_markdown(schema_content)
            if sources_md:
                with open(dest_folder + "/" + "sources.md", "w") as out:
                    out.write(sources_md)
                SCHEMA_INFOS[schema_name]["versions"][version]["pages"].append("sources.md")
        # If schema release is not valid, we remove it from DATA_FOLDER1
        else:
            logging.warning(
                f"> invalid version {frictionless_report.errors[0].message if frictionless_report.errors else ''}"
            )
            manage_errors(repertoire_slug, version, "tableschema validation")
            shutil.rmtree(dest_folder)
    # If there is no schema.json, schema release is not valid, we remove it from DATA_FOLDER1
    else:
        manage_errors(repertoire_slug, version, "missing " + schema_file)
        shutil.rmtree(dest_folder)

    return list_schemas


def manage_jsonschema(
    src_folder: str,
    dest_folder: str,
    list_schemas: dict,
    version: str,
    schema_name: str,
    repertoire_slug: str,
):
    """Check validity of a schema release from jsonschema type"""
    conf_schema = None
    # Verify that a file schemas.yml is present
    # This file will indicate title, description of jsonschema
    # (it is a prerequisite asked by schema.data.gouv.fr)
    # This file will also indicate which file store the jsonschema schema
    if os.path.isfile(src_folder + "schemas.yml"):
        try:
            with open(src_folder + "schemas.yml", "r") as f:
                conf_schema = yaml.safe_load(f)
                if "schemas" in conf_schema:
                    s = conf_schema["schemas"][0]
                    # Verify if jsonschema file indicate in schemas.yml is present, then load it
                    if os.path.isfile(src_folder + s["path"]):
                        with open(src_folder + s["path"], "r") as f:
                            schema_data = json.load(f)
                        # Validate schema with jsonschema package
                        jsonschema.validators.validator_for(schema_data).check_schema(schema_data)
                        list_schemas[version] = s["path"]
                        # We complete info of version
                        SCHEMA_INFOS[schema_name]["versions"][version] = {"pages": []}
                        # We check for list of normalized files if it is present in source code
                        # if so, we copy paste them into dest folder
                        for f in ["README.md", "SEE_ALSO.md", "CHANGELOG.md", "CONTEXT.md", s["path"]]:
                            if os.path.isfile(src_folder + f):
                                logging.info(f"schema has {f}")
                                os.makedirs(os.path.dirname(dest_folder + f), exist_ok=True)
                                shutil.copyfile(src_folder + f, dest_folder + f)
                                # if it is a markdown file, we will read them as page in website
                                if f[-3:] == ".md":
                                    SCHEMA_INFOS[schema_name]["versions"][version]["pages"].append(f)
                                # if it is the schema, we indicate it as it in object
                                if f == s["path"]:
                                    SCHEMA_INFOS[schema_name]["versions"][version]["schema_url"] = (
                                        "/" + schema_name + "/" + version + "/" + s["path"]
                                    )
                # Create documentation file and save it
                try:
                    with open(dest_folder + "documentation.md", "w", encoding="utf8") as out:
                        md = (
                            f"## {schema_name.split('/')[-1]}\n\n"
                            f"{conf_schema['title']}\n\n"
                            f"{conf_schema['description']}\n\n"
                            f"- Site web : {conf_schema['homepage']}\n"
                            f"- Version : {version}\n\n"
                            "### Arborescence des propriétés :\n\n"
                        )
                        md += jsonschema_to_markdown(schema_data)
                        out.write(md)
                        SCHEMA_INFOS[schema_name]["versions"][version]["pages"].append("documentation.md")
                except:
                    logging.warning("Could not build documentation from schema")
        # If schema release is not valid, we remove it from DATA_FOLDER1
        except:
            manage_errors(repertoire_slug, version, "jsonschema validation")
            shutil.rmtree(dest_folder)
    # If there is no schemas.yml, schema release is not valid, we remove it from DATA_FOLDER1
    else:
        manage_errors(repertoire_slug, version, "missing schemas.yml")
        shutil.rmtree(dest_folder)

    return list_schemas, conf_schema


def manage_other(
    src_folder,
    dest_folder,
    list_schemas,
    version,
    schema_name,
    repertoire_slug
):
    """Check validity of a schema release from other type"""
    conf_schema = None
    # Verify that a file schema.yml is present
    # This file will indicate title, description of schema
    # (it is a prerequisite asked by schema.data.gouv.fr)
    if os.path.isfile(src_folder + "schema.yml"):
        try:
            with open(src_folder + "schema.yml", "r") as f:
                conf_schema = yaml.safe_load(f)
            list_schemas[version] = "schema.yml"
            # We complete info of version
            SCHEMA_INFOS[schema_name]["versions"][version] = {"pages": []}
            # We check for list of normalized files if it is present in source code
            # if so, we copy paste them into dest folder
            for f in ["README.md", "SEE_ALSO.md", "CHANGELOG.md", "CONTEXT.md", "schema.yml"]:
                if os.path.isfile(src_folder + f):
                    shutil.copyfile(src_folder + f, dest_folder + f)
                    # if it is a markdown file, we will read them as page in website
                    if f[-3:] == ".md":
                        SCHEMA_INFOS[schema_name]["versions"][version]["pages"].append(f)
                    # if it is the schema, we indicate it as it in object
                    if f == "schema.yml":
                        SCHEMA_INFOS[schema_name]["versions"][version]["schema_url"] = (
                            "/" + schema_name + "/" + version + "/" + "schema.yml"
                        )
        # If schema release is not valid, we remove it from DATA_FOLDER1
        except:
            manage_errors(repertoire_slug, version, "validation of type other")
            shutil.rmtree(dest_folder)
    # If there is no schema.yml, schema release is not valid, we remove it from DATA_FOLDER1
    else:
        manage_errors(repertoire_slug, version, "missing schema.yml")
        shutil.rmtree(dest_folder)

    return list_schemas, conf_schema


def manage_latest_folder(schema_name: str, folders: dict) -> tuple[str, str]:
    """Create latest folder containing all files from latest valid version of a schema"""
    # Get all valid version from a schema by analyzing folders
    # then sort them to get latest valid version and related folder
    subfolders = [
        f.name for f in os.scandir(f"{folders['DATA_FOLDER1']}/{schema_name}/")
        if f.is_dir()
    ]
    subfolders = sorted(subfolders, key=comparer_versions)
    sf = subfolders[-1]
    if sf == "latest":
        sf = subfolders[-2]
    latest_version_folder = f"{folders['DATA_FOLDER1']}/{schema_name}/{sf}/"
    # Determine latest folder path then mkdir it
    latest_folder = f"{folders['DATA_FOLDER1']}/{schema_name}/latest/"
    os.makedirs(latest_folder, exist_ok=True)
    # For every file in latest valid version folder, copy them into
    shutil.copytree(latest_version_folder, latest_folder, dirs_exist_ok=True)
    # For website need, copy paste latest README into root of schema folder
    shutil.copyfile(
        latest_version_folder + "README.md",
        "/".join(latest_version_folder.split("/")[:-2]) + "/README.md"
    )

    return latest_folder, sf


def generate_catalog_datapackage(latest_folder, dpkg_name, conf, list_schemas):
    with open(latest_folder + "datapackage.json", "r") as f:
        dpkg = json.load(f)
    mydict = {}
    mydict["name"] = dpkg_name
    mydict["title"] = dpkg["title"]
    mydict["description"] = dpkg["description"]
    mydict["schema_url"] = (
        "https://schema.data.gouv.fr/schemas/" + dpkg_name + "/latest/" + "datapackage.json"
    )
    mydict["schema_type"] = "datapackage"
    mydict["contact"] = conf.get("email")
    mydict["examples"] = []
    mydict["labels"] = conf["labels"] if "labels" in conf else []
    mydict["consolidation_dataset_id"] = conf.get("consolidation")
    mydict["versions"] = []
    for sf in list_schemas:
        mydict2 = {}
        mydict2["version_name"] = sf
        mydict2["schema_url"] = (
            "https://schema.data.gouv.fr/schemas/" + dpkg_name + "/" + sf + "/" + "datapackage.json"
        )
        mydict["versions"].append(mydict2)
    # These four following property are not in catalog spec
    mydict["external_doc"] = conf.get("external_doc")
    mydict["external_tool"] = conf.get("external_tool")
    mydict["homepage"] = conf["url"]
    return mydict


def generate_catalog_object(
    latest_folder: str,
    list_schemas: list,
    schema_file: str,
    schema_type: str,
    schema_name: str,
    folders: dict,
    obj_info: dict | None = None,
    datapackage: str | None = None,
) -> dict:
    """Generate dictionnary containing all relevant information for catalog"""
    # If tableschema, relevant information are directly into schema.json,
    # if not, relevant info are in yaml files with are stored in obj_info variable
    if schema_type == "tableschema":
        with open(latest_folder + schema_file, "r") as f:
            schema = json.load(f)
    else:
        schema = obj_info
    mydict = {
        "name": (
            latest_folder.replace(folders["DATA_FOLDER1"] + "/", "").replace("/latest/", "")
            if datapackage
            else schema_name
        ),
        "title": schema["title"],
        "description": schema["description"],
        "schema_type": schema_type,
        "contact": obj_info.get("email"),
        "examples": schema.get("resources", []),
        "labels": obj_info.get("labels", []),
        "consolidation_dataset_id": obj_info.get("consolidation"),
        "versions": [],
        "external_doc": obj_info.get("external_doc"),
        "external_tool": obj_info.get("external_tool"),
        "homepage": obj_info.get("homepage", obj_info.get("url")),
    }
    mydict["schema_url"] = f"https://schema.data.gouv.fr/schemas/{mydict['name']}/latest/{schema_file}"
    for sf in list_schemas:
        mydict["versions"].append({
            "version_name": sf,
            "schema_url": f"https://schema.data.gouv.fr/schemas/{mydict['name']}/{sf}/{list_schemas[sf]}",
        })
    if datapackage:
        mydict["datapackage_title"] = datapackage["title"]
        mydict["datapackage_name"] = schema_name
        mydict["datapackage_description"] = datapackage.get("description")
    return mydict


def find_md_links(md):
    """Returns dict of links in markdown:
    "regular": [foo](some.url)
    "footnotes": [foo][3]
    [3]: some.url
    """
    # https://stackoverflow.com/a/30738268/2755116
    INLINE_LINK_RE = re.compile(r"\[([^\]]+)\]\(([^)]+)\)")
    FOOTNOTE_LINK_TEXT_RE = re.compile(r"\[([^\]]+)\]\[(\d+)\]")
    FOOTNOTE_LINK_URL_RE = re.compile(r"\[(\d+)\]:\s+(\S+)")

    links = list(INLINE_LINK_RE.findall(md))
    footnote_links = dict(FOOTNOTE_LINK_TEXT_RE.findall(md))
    footnote_urls = dict(FOOTNOTE_LINK_URL_RE.findall(md))

    footnotes_linking = []

    for key in footnote_links.keys():
        footnotes_linking.append((footnote_links[key], footnote_urls[footnote_links[key]]))

    return links


def cleanLinksDocumentation(dest_folder):
    """Custom cleaning for links in markdown"""
    # For every documentation.md file, do some custom cleaning for links
    file = codecs.open(dest_folder + "documentation.md", "r", "utf-8")
    data = file.read()
    file.close()
    # Find all links in file
    links = find_md_links(data)
    # For each one, lower string then manage space ; _ ; --- and replace them by -
    for (name, link) in links:
        if link.startswith("#"):
            newlink = link.lower()
            newlink = newlink.replace(" ", "-")
            newlink = newlink.replace("_", "-")
            newlink = unidecode(newlink, "utf-8")
            newlink = newlink.replace("---", "-")
            data = data.replace(link, newlink)
    # Save modifications
    with open(dest_folder + "documentation.md", "w", encoding="utf-8") as fin:
        fin.write(data)


def addFrontToMarkdown(dest_folder, f):
    """Custom add to every markdown files"""
    # for every markdown files
    file = codecs.open(dest_folder + f, "r", "utf-8")
    data = file.read()
    file.close()
    # Add specific tag for website interpretation
    data = "<MenuSchema />\n\n" + data
    # Exception scdl Budget not well interpreted by vuepress
    data = data.replace("<DocumentBudgetaire>", "DocumentBudgetaire")
    # Save modification
    with open(dest_folder + f, "w", encoding="utf-8") as fin:
        fin.write(data)


def getListOfFiles(dirName):
    """Get list off all files in a specific folder"""
    # create a list of file and sub directories
    # names in the given directory
    listOfFile = os.listdir(dirName)
    allFiles = list()
    # Iterate over all the entries
    for entry in listOfFile:
        # Create full path
        fullPath = os.path.join(dirName, entry)
        # If entry is a directory then get the list of files in this directory
        if os.path.isdir(fullPath):
            allFiles = allFiles + getListOfFiles(fullPath)
        else:
            allFiles.append(fullPath)
    return allFiles


def get_contributors(url):
    """Get list off all contributors of a specific git repo"""
    parse_url = parse.urlsplit(url)
    # if github, use github api
    if "github.com" in parse_url.netloc:
        api_url = (
            parse_url.scheme + "://api.github.com/repos/"
            + parse_url.path[1:].replace(".git", "") + "/contributors"
        )
    # else, use gitlab api
    else:
        api_url = (
            parse_url.scheme + "://" + parse_url.netloc
            + "/api/v4/projects/" + parse_url.path[1:].replace("/", "%2F").replace(".git", "")
            + "/repository/contributors"
        )
    try:
        r = requests.get(api_url)
        return len(r.json())
    except:
        return None


#######################################################################################################
# DAG functions

def check_and_save_schemas(ti, suffix):
    folders: dict = ti.xcom_pull(key="folders", task_ids="initialization" + suffix)
    config: dict[str, dict] = ti.xcom_pull(key="config", task_ids="initialization" + suffix)
    # Clean and (re)create CACHE AND DATA FOLDER
    clean_and_create_folder(folders["CACHE_FOLDER"])
    clean_and_create_folder(folders["DATA_FOLDER1"])

    # Initiate Catalog
    SCHEMA_CATALOG["$schema"] = "https://opendataschema.frama.io/catalog/schema-catalog.json"
    SCHEMA_CATALOG["version"] = 1
    SCHEMA_CATALOG["schemas"] = []

    # For every schema in repertoires.yml, check it
    for repertoire_slug, conf in config.items():
        logging.info("_______________________________")
        logging.info(f"Starting with {repertoire_slug}")
        logging.info(conf)
        if conf["type"] != "datapackage":
            logging.info(f"Recognized as {conf['type']}")
            schema_to_add_to_catalog: dict = check_schema(repertoire_slug, conf, conf["type"], folders)
            if schema_to_add_to_catalog:
                SCHEMA_CATALOG["schemas"].append(schema_to_add_to_catalog)
        else:
            logging.info("Recognized as datapackage")
            schemas_to_add_to_catalog = check_datapackage(conf, folders)
            if schemas_to_add_to_catalog:
                for schema in schemas_to_add_to_catalog:
                    SCHEMA_CATALOG["schemas"].append(schema)
        logging.info(f"--- {repertoire_slug} processed")
    schemas_scdl = SCHEMA_CATALOG.copy()
    schemas_transport = SCHEMA_CATALOG.copy()
    schemas_tableschema = SCHEMA_CATALOG.copy()
    # Save catalog to schemas.json file
    with open(folders["DATA_FOLDER1"] + "/schemas.json", "w") as fp:
        json.dump(SCHEMA_CATALOG, fp, indent=4)

    schemas_scdl["schemas"] = [
        x for x in schemas_scdl["schemas"] if "Socle Commun des Données Locales" in x["labels"]
    ]
    logging.info(f"Schémas SCDL : {schemas_scdl}")

    schemas_transport["schemas"] = [
        x for x in schemas_transport["schemas"] if "transport.data.gouv.fr" in x["labels"]
    ]
    logging.info(f"Schémas transport : {schemas_transport}")

    schemas_tableschema["schemas"] = [
        x for x in schemas_tableschema["schemas"] if x["schema_type"] == "tableschema"
    ]

    with open(folders["DATA_FOLDER1"] + "/schemas-scdl.json", "w") as fp:
        json.dump(schemas_scdl, fp, indent=4)

    with open(folders["DATA_FOLDER1"] + "/schemas-transport-data-gouv-fr.json", "w") as fp:
        json.dump(schemas_transport, fp, indent=4)

    with open(folders["DATA_FOLDER1"] + "/schemas-tableschema.json", "w") as fp:
        json.dump(schemas_tableschema, fp, indent=4)

    # Save schemas_infos to schema-infos.json file
    with open(folders["DATA_FOLDER1"] + "/schema-infos.json", "w") as fp:
        json.dump(SCHEMA_INFOS, fp, indent=4)

    # Save errors to errors.json file
    with open(folders["DATA_FOLDER1"] + "/errors.json", "w") as fp:
        json.dump(ERRORS_REPORT, fp, indent=4)

    ti.xcom_push(key="SCHEMA_CATALOG", value=SCHEMA_CATALOG)
    ti.xcom_push(key="SCHEMA_INFOS", value=SCHEMA_INFOS)
    ti.xcom_push(key="ERRORS_REPORT", value=ERRORS_REPORT)
    logging.info(f"End of process catalog: {SCHEMA_CATALOG}")
    logging.info(f"End of process infos: {SCHEMA_INFOS}")
    logging.info(f"End of process errors: {ERRORS_REPORT}")


def get_template_github_issues():
    def get_all_issues():
        url = "https://api.github.com/repos/datagouv/schema.data.gouv.fr/issues?state=all"
        issues = []
        page = 1
        while True:
            response = requests.get(
                url + f"&page={page}",
                # could need to specify token if rate limited
            )
            if response.status_code == 200:
                issues.extend(response.json())
                if len(response.json()) < 30:
                    break
                page += 1
            else:
                raise Exception(
                    "This shouldn't fail, maybe consider adding a token "
                    "in the headers, or wait a couple of minutes and retry"
                )
        return issues

    logging.info("Getting issues from repo")
    issues = get_all_issues()
    logging.info("Sorting relevant issues")
    dates = {}
    for issue in issues:
        d = issue["created_at"][:10]
        body = issue["body"]
        if body is None:
            continue
        processed = False
        # getting all issues that are correctly labelled
        for phase in ["investigation", "construction"]:
            if any(lab["name"] == f"Schéma en {phase}" for lab in issue["labels"]):
                processed = True
                d = issue["created_at"][:10]
                if d not in dates:
                    dates[d] = {}
                if phase not in dates[d]:
                    dates[d][phase] = []
                dates[d][phase].append({
                    "title": issue["title"],
                    "url": issue["html_url"]
                })
        # getting potentially unlabelled issues
        if not processed and "Stade d'avancement" in body:
            rows = body.split("\n")
            phases = [
                row for row in rows
                if row.startswith("- [")
                and any(w in row for w in ["investigation", "construction"])
            ]
            # reversed to get the latest advancement state
            for phase in reversed(phases):
                if phase.startswith("- [x]"):
                    if d not in dates:
                        dates[d] = {}
                    phase = "construction" if "construction" in phase else "investigation"
                    if phase not in dates[d]:
                        dates[d][phase] = []
                    dates[d][phase].append({
                        "title": issue["title"],
                        "url": issue["html_url"]
                    })
    return dates


def update_news_feed(ti, tmp_folder, suffix):
    new = ti.xcom_pull(key="SCHEMA_INFOS", task_ids="check_and_save_schemas" + suffix)
    today = datetime.now().strftime("%Y-%m-%d")
    changes = {today: {}}
    with open(
        tmp_folder + "schema.data.gouv.fr/site/.vuepress/public/schema-infos.json",
        "r",
        encoding="utf-8"
    ) as f:
        old = json.load(f)
        f.close()
    # getting existing schemas infos
    for schema in new:
        if schema not in old:
            if "new_schema" not in changes[today]:
                changes[today]["new_schema"] = []
            changes[today]["new_schema"].append({
                "schema_name": schema,
                "version": new[schema].get("latest"),
            })
        else:
            for v in new[schema]["versions"]:
                if v not in old[schema]["versions"]:
                    if "new_version" not in changes[today]:
                        changes[today]["new_version"] = []
                    changes[today]["new_version"].append({
                        "schema_name": schema,
                        "version": (
                            f"{old[schema].get('latest')} => "
                            f"{new[schema].get('latest')}"
                        ),
                    })
    for schema in old:
        if schema not in new:
            if "deleted_schema" not in changes[today]:
                changes[today]["deleted_schema"] = []
            changes[today]["deleted_schema"].append({
                "schema_name": schema,
                "version": old[schema].get("latest"),
            })

    # getting investigation/construction from github issues
    # only parsing issues made from the template
    schema_updates_file = tmp_folder + "schema.data.gouv.fr/site/.vuepress/public/schema-updates.json"
    with open(schema_updates_file, "r", encoding="utf-8") as f:
        updates = json.load(f)
        f.close()
    issues = get_template_github_issues()
    # to have updates when issues change status we check which ones have already been seen
    # in one state or another
    logging.info("Gathering issues in groups")
    already_there_issues = {"investigation": [], "construction": []}
    for date in updates:
        for k in already_there_issues:
            if k in updates[date]:
                for schema in updates[date][k]:
                    already_there_issues[k].append(schema["url"])
    logging.info("Updating changes with issues")
    for date in issues:
        for change_type in issues[date]:
            for issue in issues[date][change_type]:
                if issue["url"] not in already_there_issues[change_type]:
                    logging.info(f"  > {issue['title']} changed status for {change_type}")
                    if today not in changes:
                        changes[today] = {}
                    if change_type not in changes[today]:
                        changes[today][change_type] = []
                    changes[today][change_type].append(issue)
    # if you want to check changes in dev mode
    # changes[today] = {
    #     "new_schema": [
    #         {"schema_name": "test/schematest", "version": "0.1.0"},
    #     ],
    #     "new_version": [
    #         {"schema_name": "139bercy/format-commande-publique", "version": "3.1.1 => 3.1.2"},
    #     ],
    #     "investigation": [
    #         {
    #             "title": "Schéma table en bois",
    #             "url": "https://github.com/datagouv/schema.data.gouv.fr/issues/910"
    #         },
    #     ],
    # }
    if changes[today]:
        logging.info(f"Updating news feed with: {changes[today]}")
        # updating schema-updates.json
        if today not in updates:
            updates.update(changes)
        else:
            for change_type in changes[today]:
                if change_type not in updates[today]:
                    updates[today][change_type] = changes[today][change_type]
                else:
                    updates[today][change_type] += changes[today][change_type]
        updates = {k: updates[k] for k in sorted(updates.keys())}
        logging.info(updates)
        with open(schema_updates_file, "w", encoding="utf-8") as f:
            json.dump(updates, f, indent=4)

        # updating actualites.md
        mapping = {
            "new_schema": "Schéma{} ajouté{}",
            "new_version": "Montée{} de version{}",
            "deleted_schema": "Schéma{} supprimé{}",
            "investigation": "Schéma{} en investigation",
            "construction": "Schéma{} en construction",
        }
        md = ""
        for date in sorted(updates.keys())[::-1]:
            if md:
                md += "\n---\n\n"
            md += f"### {date}\n"
            for change_type in updates[date]:
                args = ["s" if len(updates[date][change_type]) > 1 else ""] * 2
                md += (
                    f"\n#### {mapping[change_type].format(*args)}:\n"
                )
                for schema in updates[date][change_type]:
                    if change_type in ["investigation", "construction"]:
                        md += (
                            f"&nbsp;&nbsp;&nbsp;&nbsp; - **[{schema['title']}]"
                            f"({schema['url']}/)**\n"
                        )
                    elif "=>" in schema["version"]:
                        old_v, new_v = schema["version"].split(" => ")
                        md += (
                            f"&nbsp;&nbsp;&nbsp;&nbsp; - **[{schema['schema_name']}]"
                            f"(/{schema['schema_name']}/)** : "
                            f'<span style="color:red;">{old_v}</span> => '
                            f'<span style="color:green;">{new_v}</span><br>\n'
                        )
                    else:
                        md += (
                            f"&nbsp;&nbsp;&nbsp;&nbsp; - **[{schema['schema_name']}]"
                            f"(/{schema['schema_name']}/)** : "
                            f'<span style="color:blue;">{schema["version"]}</span><br>\n'
                        )
        with open(tmp_folder + "schema.data.gouv.fr/site/actualites.md", "w", encoding="utf-8") as f:
            f.write(md)

        # updating RSS feed
        tz = pytz.timezone("CET")
        url = "https://schema.data.gouv.fr/"
        rss_folder = "schema.data.gouv.fr/site/.vuepress/public/rss/"
        # loading existing file
        tree = ET.parse(tmp_folder + rss_folder + "global.xml")
        root = tree.getroot()
        root.set("xmlns:atom", "http://www.w3.org/2005/Atom")
        root.set("xmlns:content", "http://purl.org/rss/1.0/modules/content/")
        # logging.info(ET.tostring(root, encoding="utf-8").decode("utf-8"))

        existing_feeds = [
            k.replace("_", "/").replace(".xml", "") for k in os.listdir(tmp_folder + rss_folder)
        ]

        for date, up in changes.items():
            for update_type, versions in up.items():
                for version in versions:
                    # updating global feed
                    new_item = ET.Element("item")
                    title = ET.SubElement(new_item, "title")
                    link = ET.SubElement(new_item, "link")
                    description = ET.SubElement(new_item, "description")
                    if version.get("schema_name"):
                        # existing schemas
                        logging.info(f"   - {version['schema_name']}")
                        v = version["version"].replace("=>", "to")
                        title.text = (
                            f"{update_type.capitalize().replace('_', ' ')} - {version['schema_name']} ({v})"
                        )
                        link.text = f"{url}{version['schema_name']}"
                        description.text = f"Schema update on {date}: {v} for {version['schema_name']}"
                    else:
                        # issues from repo
                        logging.info(f"   - {version['title']}")
                        title.text = (
                            f"{update_type.capitalize()} - {version['title']}"
                        )
                        link.text = version["url"]
                        description.text = (
                            f"Schema update on {date}: {version['title']} is in {update_type}"
                        )
                    pub_date = ET.SubElement(new_item, "pubDate")
                    pub_date.text = (
                        tz.localize(datetime.strptime(date, "%Y-%m-%d")).strftime("%a, %d %b %Y %H:%M:%S %z")
                    )
                    ET.indent(new_item, level=2)
                    root.find("./channel").text = "\n  "
                    root.find("./channel").append(new_item)
                    root.find("./channel").text = "\n  "

                    # updating specific feed
                    if version.get("title"):
                        # no feed for schemas that are not yet published
                        continue
                    title_content = f"{date} - {update_type} - {v}"
                    description_content = (
                        f"Update on {date}: {update_type.capitalize().replace('_', ' ')} "
                        f"({version['schema_name']})"
                    )
                    date_content = (
                        tz.localize(datetime.strptime(date, "%Y-%m-%d")).strftime("%a, %d %b %Y %H:%M:%S %z")
                    )
                    if version["schema_name"] in existing_feeds:
                        feed_path = (
                            tmp_folder + rss_folder + version["schema_name"].replace("/", "_") + ".xml"
                        )
                        specific_tree = ET.parse(feed_path)
                        specific_root = specific_tree.getroot()
                        specific_root.set("xmlns:atom", "http://www.w3.org/2005/Atom")
                        specific_root.set("xmlns:content", "http://purl.org/rss/1.0/modules/content/")
                        # logging.info(ET.tostring(specific_root, encoding="utf-8").decode("utf-8"))
                        new_item = ET.Element("item")
                        title = ET.SubElement(new_item, "title")
                        title.text = title_content
                        description = ET.SubElement(new_item, "description")
                        description.text = description_content
                        pub_date = ET.SubElement(new_item, "pubDate")
                        pub_date.text = date_content
                        ET.indent(new_item, level=2)
                        specific_root.find("./channel").text = "\n  "
                        specific_root.find("./channel").append(new_item)
                        specific_root.find("./channel").text = "\n  "
                        # logging.info(ET.tostring(specific_root, encoding="utf-8").decode("utf-8"))
                        specific_tree.write(feed_path)
                    else:
                        specific_fg = FeedGenerator()
                        specific_fg.title(f"{version['schema_name']} Versioning RSS Feed")
                        specific_fg.link(href=url + version["schema_name"], rel="alternate")
                        specific_fg.description(f"Updates on {version['schema_name']} versioning")
                        fe = specific_fg.add_entry()
                        fe.title(title_content)
                        fe.description(description_content)
                        fe.published(date_content)
                        # logging.info(specific_fg.rss_str(pretty=True))
                        specific_fg.rss_file(
                            tmp_folder + rss_folder + version["schema_name"].replace("/", "_") + ".xml",
                            pretty=True
                        )
        # logging.info(ET.tostring(root, encoding="utf-8").decode("utf-8"))
        tree.write(tmp_folder + rss_folder + "global.xml")
    else:
        logging.info("No update today")


def sort_folders(ti, suffix):
    SCHEMA_CATALOG = ti.xcom_pull(key="SCHEMA_CATALOG", task_ids="check_and_save_schemas" + suffix)
    folders = ti.xcom_pull(key="folders", task_ids="initialization" + suffix)
    # Get list of all files in DATA_FOLDER
    files = getListOfFiles(folders["DATA_FOLDER1"])
    # Create list of file that we do not want to copy paste
    avoid_files = [
        folders["DATA_FOLDER1"] + "/" + s["name"] + "/README.md" for s in SCHEMA_CATALOG["schemas"]
    ]
    jsonschemas = [s["name"] for s in SCHEMA_CATALOG["schemas"] if s["schema_type"] == "jsonschema"]
    # for every file
    for f in files:
        # if it is a markdown, add custom front to content
        if f[-3:] == ".md":
            addFrontToMarkdown("/".join(f.split("/")[:-1]) + "/", f.split("/")[-1])
        # if it is the documentation file (except for jsonschemas), clean links on it
        if f.split("/")[-1] == "documentation.md" and not any(s in f for s in jsonschemas):
            cleanLinksDocumentation("/".join(f.split("/")[:-1]) + "/")
        # if it is a README file (except if on avoid_list)
        # then copy paste it to root folder of schema (for website use)
        # That will create README file with name X.X.X.md (X.X.X corresponding to a specific version)
        if f.split("/")[-1] == "README.md":
            if f not in avoid_files:
                shutil.copyfile(f, f.replace("/README.md", ".md"))

    # Clean and (re)create DATA_FOLDER2, then copy paste all DATA_FOLDER1 into DATA_FOLDER2
    # DATA_FOLDER1 will be use to contain all markdown files
    # DATA_FOLDER2 will be use to contain all yaml and json files
    # This is needed for vuepress that need to store page in one place and "resources" in another
    if os.path.exists(folders["DATA_FOLDER2"]):
        shutil.rmtree(folders["DATA_FOLDER2"])
    shutil.copytree(folders["DATA_FOLDER1"], folders["DATA_FOLDER2"])


def get_issues_and_labels(ti, suffix):
    SCHEMA_CATALOG = ti.xcom_pull(key="SCHEMA_CATALOG", task_ids="check_and_save_schemas" + suffix)
    folders = ti.xcom_pull(key="folders", task_ids="initialization" + suffix)
    # For every issue, request them by label schema status (en investigation or en construction)
    mydict = {}
    labels = ["construction", "investigation"]
    # For each label, get relevant info via github api of schema.data.gouv.fr repo
    logging.info("Getting issues for each label")
    for lab in labels:
        logging.info(f"   > {lab}")
        try:
            r = requests.get(
                (
                    "https://api.github.com/repos/etalab/schema.data.gouv.fr/"
                    "issues?q=is%3Aopen+is%3Aissue&labels=Sch%C3%A9ma%20en%20"
                )
                + lab
            )
            mydict[lab] = []
            for issue in r.json():
                mydict2 = {}
                mydict2["created_at"] = issue["created_at"]
                mydict2["labels"] = [lab]
                mydict2["nb_comments"] = issue["comments"]
                mydict2["title"] = issue["title"]
                mydict2["url"] = issue["html_url"]
                mydict[lab].append(mydict2)
        except:
            logging.warning("Error with github API")

    # Find number of current issue in schema.data.gouv.fr repo
    try:
        r = requests.get(
            "https://api.github.com/repos/etalab/schema.data.gouv.fr/issues?q=is%3Aopen+is%3Aissue"
        )
        mydict["nb_issues"] = len(r.json())
    except:
        logging.warning("Error with github API while trying to get issues from schema.data.gouv.fr repo")

    # for every schema, find relevant info in data.gouv.fr API
    mydict["references"] = {}
    logging.info("Getting nb resources on data.gouv")
    for s in SCHEMA_CATALOG["schemas"]:
        r = requests.get(
            "https://www.data.gouv.fr/api/1/datasets/?schema=" + s["name"],
            headers={"X-fields": "total"}
        )
        mydict["references"][s["name"]] = {
            "dgv_resources": r.json()["total"],
            "title": s["title"],
            "contributors": get_contributors(s["homepage"]),
        }
        logging.info(f"   > {mydict['references'][s['name']]}")

    # Save stats infos to stats.json file
    with open(folders["DATA_FOLDER2"] + "/stats.json", "w") as fp:
        json.dump(mydict, fp, indent=4)


def publish_schema_dataset(ti, tmp_folder, AIRFLOW_ENV, branch, suffix):
    schemas = ti.xcom_pull(key="SCHEMA_INFOS", task_ids="check_and_save_schemas" + suffix)
    folders = ti.xcom_pull(key="folders", task_ids="initialization" + suffix)
    with open(folders["DATA_FOLDER2"] + "/stats.json", "r") as f:
        references = json.load(f)["references"]
    df = pd.DataFrame(
        {"name": n, **v} for n, v in schemas.items()
    ).drop("email", axis=1)
    df["versions"] = df["versions"].apply(lambda d: list(d.keys()))
    df["nb_versions"] = df["versions"].apply(lambda versions: len(versions))
    stats = pd.DataFrame([
        {"name": r, **references[r]} for r in references
    ])[["name", "dgv_resources"]]
    stats.rename({"dgv_resources": "datagouv_resources"}, axis=1, inplace=True)
    merged = pd.merge(
        df,
        stats,
        on="name",
        how="outer",
    )
    file = File(
        source_path=tmp_folder,
        source_name="schemas_catalog_table.csv",
        remote_source=True,  # not remote but not created yet
    )
    merged.to_csv(file.full_source_path, index=False)
    target_demo = (branch != "main") or (AIRFLOW_ENV == "dev")
    client = demo_client if target_demo else prod_client
    client.resource(
        # ids are the same now, but keeping syntax in case they unsync
        id=(
            "31ed3bb3-cab4-48c2-b9b1-cb7095e8a548" if not target_demo
            else "31ed3bb3-cab4-48c2-b9b1-cb7095e8a548"
        ),
        dataset_id=(
            "668282444f9d3f48f2702fcd" if not target_demo
            else "668282444f9d3f48f2702fcd"
        ),
        fetch=False,
        _from_response={"filetype": "file"},  # to be able to update the file without fetching
    ).update(
        file_to_upload=file.full_source_path,
        payload={
            "title": f"Catalogue des schémas de données ({datetime.now().strftime('%Y-%m-%d')})"
        },
    )


def remove_all_files_extension(folder, extension):
    """Remove all file of a specific extension in a folder"""
    files = []
    files = getListOfFiles(folder)
    for f in files:
        if f[-1 * len(extension):] == extension:
            os.remove(f)


def final_clean_up(ti, suffix):
    folders = ti.xcom_pull(key="folders", task_ids="initialization" + suffix)
    # Remove all markdown from DATA_FOLDER1 and all json, yaml and yml file of DATA_FOLDER2
    remove_all_files_extension(folders["DATA_FOLDER2"], ".md")
    remove_all_files_extension(folders["DATA_FOLDER1"], ".json")
    remove_all_files_extension(folders["DATA_FOLDER1"], ".yml")
    remove_all_files_extension(folders["DATA_FOLDER1"], ".yaml")
