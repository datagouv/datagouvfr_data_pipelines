import json
import logging
import mimetypes


from airflow.decorators import task

# from datagouv import Client
from datagouvfr_data_pipelines.config import (
    AIRFLOW_ENV,
    AIRFLOW_DAG_HOME,
)
from datagouvfr_data_pipelines.utils.datagouv import (
    local_client,
)
from datagouvfr_data_pipelines.utils.tchap import send_message
import requests

mimetypes.init()

# Variables directly below need an update each year
LATEST_YEAR = "2026"
END_DATE = "2026-04-01"

commons = ["communes", "epci", "departements", "regions", "arrondissements"]
commons_name = [
    "communes dont communes associées/déléguées et arrondissements municipaux",
    "EPCI",
    "départements",
    "régions",
    "arrondissements",
]
commons_dict = dict(zip(commons, commons_name))
commons_dict["ept"] = "EPT"


versions_prefix = {
    "2019": "0.7",
    "2020": "0.8",
    "2021": "1.",
    "2022": "2.",
    "2023": "3.",
    "2024": "4.",
    "2025": "5.",
    "2026": "6.",
}

base_unpkg_decoupage_administratif = "https://unpkg.com/@etalab/decoupage-administratif"


DAG_NAME = "decoupage-administratif"
DAG_FOLDER = "datagouvfr_data_pipelines/data_processing/"
with open(f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}decoupage_administratif/config.json") as fp:
    config = json.load(fp)

DATASET_ID = config[DAG_NAME][AIRFLOW_ENV]["dataset_id"]

dataset = local_client.dataset(DATASET_ID)


def npm_package_metadata(package_name):
    url_npm_registry = f"https://registry.npmjs.org/{package_name}/"
    r_url_npm_registry = requests.get(url_npm_registry)
    return r_url_npm_registry.json()


def npm_package_latest_version_for_years():
    versions = {}
    metadata = npm_package_metadata("@etalab/decoupage-administratif")
    npm_package_versions_without_alpha_beta = [
        i for i in metadata.get("versions").keys() if "-" not in i
    ]
    for year, version_prefix in versions_prefix.items():
        versions[year] = sorted(
            [
                j
                for j in npm_package_versions_without_alpha_beta
                if j.startswith(version_prefix)
            ]
        )[-1]
    return versions


NPM_PACKAGE_VERSIONS = npm_package_latest_version_for_years()


def generate_files_infos_decoupage_administratif():
    output = []
    priority_by_type = {i:float((idx + 1) * 10) for idx, i in enumerate(reversed(commons_dict.keys()))}
    for year, version in NPM_PACKAGE_VERSIONS.items():
        type_file = "main" if LATEST_YEAR in year else "other"
        mime_file = "application/json"
        format_file = "json"
        if version == "0.8.0":
            priority = priority_by_type['epci'] + 1 / float(2020)
            mytitle = "Données EPCI 2020"
            url = f"{base_unpkg_decoupage_administratif}@{version}/data/epci.json"
            r = requests.get(url, stream=True, headers={"Accept-Encoding": None})
            mysize = r.headers["Content-Length"]
            output.append(
                {
                    "title": mytitle,
                    "url": url,
                    "filesize": mysize,
                    "type": type_file,
                    "mime": mime_file,
                    "format": format_file,
                    "year": year,
                    "priority": priority,
                }
            )
        elif int(year) >= 2023:
            for name in reversed(commons + ["ept"]):
                priority = priority_by_type[name] + 1 / float(year)
                url = f"{base_unpkg_decoupage_administratif}@{version}/data/{name}.json"
                mytitle = f"Données {commons_dict[name]} {year}"
                r = requests.get(url, stream=True, headers={"Accept-Encoding": None})
                mysize = r.headers["Content-Length"]
                output.append(
                    {
                        "title": mytitle,
                        "url": url,
                        "filesize": mysize,
                        "type": type_file,
                        "mime": mime_file,
                        "format": format_file,
                        "year": year,
                        "priority": priority,
                    }
                )
        else:
            for name in reversed(commons):
                priority = priority_by_type[name] + 1 / float(year)
                url = f"{base_unpkg_decoupage_administratif}@{version}/data/{name}.json"
                mytitle = f"Données {commons_dict[name]} {year}"
                r = requests.get(url, stream=True, headers={"Accept-Encoding": None})
                mysize = r.headers["Content-Length"]
                output.append(
                    {
                        "title": mytitle,
                        "url": url,
                        "filesize": mysize,
                        "type": type_file,
                        "mime": mime_file,
                        "format": format_file,
                        "year": year,
                        "priority": priority,
                    }
                )
    return output


@task
def update_create_resources():
    ressources_unpkg = generate_files_infos_decoupage_administratif()
    urls_res_dataset = {res.title: res for res in dataset.resources}
    for res in ressources_unpkg:
        if res.get("title") in urls_res_dataset:
            logging.info("Existing ressource. Only update")
            payload = {
                "url": res.get("url"),
                "title": res.get("title"),
                "filesize": res.get("filesize"),
                "format": res.get("format"),
                "mime": res.get("mime"),
                "type": res.get("type"),
            }
            logging.info(payload)
            urls_res_dataset[res.get("title")].update(payload)
        else:
            logging.info("Create new ressource", res.get("url"))
            payload = {
                "url": res.get("url"),
                "title": res.get("title"),
                "filesize": res.get("filesize"),
                "format": res.get("format"),
                "mime": res.get("mime"),
                "type": res.get("type"),
            }
            dataset.create_remote(payload=payload)
            logging.info(payload)


@task
def specific_sort_ressources():
    dataset_reloaded = local_client.dataset(DATASET_ID)
    ressources_unpkg = generate_files_infos_decoupage_administratif()    
    urls_res_dataset_reloaded = {
        res.url: res for res in dataset_reloaded.resources
    }
    # sorted_ids_res_datagouv = [urls_res_dataset_reloaded[j[0]].id for j in sorted([[i.get('url'), i.get('priority')] for i in ressources_unpkg], key=lambda x: x[1]) if j[0] in urls_res_dataset_reloaded]
    sorted_ids_res_datagouv = [urls_res_dataset_reloaded[j[0]].id for j in sorted([[i.get('url'), i.get('priority')] for i in ressources_unpkg], key=lambda x: x[1]) if j[0]]
    local_client.session.put(
        dataset_reloaded.uri + "resources/",
        json=sorted_ids_res_datagouv,
    )


@task()
def update_temporal_coverage():
    dataset.update({"temporal_coverage": {"end": END_DATE, "start": "2019-01-01"}})


@task()
def notification():
    send_message(
        text=(
            f"📣 Mise à jour de la liste des ressources sur data.gouv.fr pour les données {DAG_NAME}.\n\n"
            f"- Données stockées sur npmjs - https://www.npmjs.com/package/@etalab/decoupage-administratif\n"
            f"- Données publiées [sur data.gouv.fr]({local_client.base_url}/datasets/{DATASET_ID}/)"
        )
    )


NPM_PACKAGE_VERSIONS = npm_package_latest_version_for_years()
