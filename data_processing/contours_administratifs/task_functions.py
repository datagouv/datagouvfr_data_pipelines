import json
import logging
import mimetypes
import re


from airflow.decorators import task

# from datagouv import Client
from datagouvfr_data_pipelines.config import (
    AIRFLOW_ENV,
    AIRFLOW_DAG_HOME,
    S3_URL_RBX,
)
from datagouvfr_data_pipelines.utils.datagouv import (
    local_client,
)
from datagouvfr_data_pipelines.utils.s3 import S3Client
from datagouvfr_data_pipelines.utils.tchap import send_message
import requests

mimetypes.init()

# Variables directly below need an update each year
LATEST_YEAR = "2026"
END_DATE = "2026-04-01"

DAG_BUCKET_NAME = "contours-administratifs"
DAG_FOLDER = "datagouvfr_data_pipelines/data_processing/"
with open(f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}contours_administratifs/config.json") as fp:
    config = json.load(fp)

REGEX = r"[0-9]+m"
DATASET_ID = config[DAG_BUCKET_NAME][AIRFLOW_ENV]["dataset_id"]

dataset = local_client.dataset(DATASET_ID)


def get_files_info_from_s3():
    content = S3Client(s3_url=S3_URL_RBX, bucket=DAG_BUCKET_NAME).get_files_from_prefix(
        prefix="", ignore_airflow_env=True
    )
    s3_paths_to_reference = [
        s3_path
        for s3_path in content
        if len([j for j in ["simplified", "fgb", "shp", "latest"] if j in s3_path]) == 0
    ]
    return s3_paths_to_reference


def generate_files_infos_contours_administratifs(items):
    output = []
    for i in items:
        paths = i.split("/")
        year = paths[0]
        name = paths[2]
        if "communes-associees-deleguees" in name:
            mytitle = "Communes associées/déléguées"
            priority = 80
        elif "communes" in name:
            mytitle = "Communes"
            priority = 100
        elif "departements" in name:
            mytitle = "Départements"
            priority = 60
        elif "regions" in name:
            mytitle = "Régions"
            priority = 50
        elif "epci" in name:
            mytitle = "EPCI"
            priority = 70
        elif "mairies" in name:
            mytitle = "Mairies"
            priority = 90
        elif "arrondissements-municipaux" in name:
            mytitle = "Arrondissements municipaux"
            priority = 90
        else:
            pass
        matches = [j for j in re.finditer(REGEX, name, re.MULTILINE)]
        text_simplify = " " + matches[0].group() if len(matches) == 1 else ""
        priority = (
            float(priority)
            if text_simplify == ""
            else float(priority) + 1 / float(text_simplify.replace("m", ""))
        )
        if "2025-01-08" in name:
            priority = 99 + float(year)
            year = year + " en date du 2025-01-08"
        else:
            priority = priority + float(year)
        mytitle = (
            mytitle
            + " "
            + year
            + text_simplify
            + " (format GeoJSON"
            + (" compressé GZ" if ".gz" in name else "")
            + ")"
        )
        remote_url = "https://contours-administratifs.s3.rbx.io.cloud.ovh.net/" + i
        ext = ".".join(remote_url.split("/")[-1].split(".")[1:])
        r = requests.head(remote_url)
        filesize = r.headers["Content-Length"]
        mime = mimetypes.types_map.get("." + ext)
        output.append(
            {
                "title": mytitle,
                "url": remote_url,
                "s3_path": i,
                "filesize": filesize,
                "mime": mime,
                "format": ext,
                "type": ("main" if LATEST_YEAR in year else "other"),
                "priority": priority,
            }
        )
    output = sorted(output, key=lambda k: k["priority"])
    return output


@task
def update_create_resources():
    ressources_s3 = generate_files_infos_contours_administratifs(
        get_files_info_from_s3()
    )
    urls_res_dataset = {
        res.url[res.url.index("/20") + 1 :]: res for res in dataset.resources
    }
    for res in ressources_s3:
        if res.get("s3_path") in urls_res_dataset:
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
            urls_res_dataset[res.get("s3_path")].update(payload)
        else:
            logging.info("Create new ressource", res.get("s3_path"))
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
    urls_res_dataset_reloaded = {
        res.url[res.url.index("/20") + 1 :]: res for res in dataset_reloaded.resources
    }
    sorted_ids_res_datagouv = [
        urls_res_dataset_reloaded[i].id for i in urls_res_dataset_reloaded
    ]
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
            f"📣 Mise à jour de la liste des ressources sur data.gouv.fr pour les données {DAG_BUCKET_NAME}.\n\n"
            f"- Données stockées sur S3 - Bucket {DAG_BUCKET_NAME}\n"
            f"- Données publiées [sur data.gouv.fr]({local_client.base_url}/datasets/{DATASET_ID}/)"
        )
    )
