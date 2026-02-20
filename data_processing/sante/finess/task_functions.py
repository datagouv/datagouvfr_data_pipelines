import json
import logging
import os
from datetime import datetime
from io import StringIO

import pandas as pd
import requests
from airflow.decorators import task
from datagouv import Resource
from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_HOME,
    AIRFLOW_DAG_TMP,
    AIRFLOW_ENV,
    S3_BUCKET_DATA_PIPELINE_OPEN,
)
from datagouvfr_data_pipelines.utils.datagouv import local_client, prod_client
from datagouvfr_data_pipelines.utils.filesystem import File
from datagouvfr_data_pipelines.utils.mattermost import send_message
from datagouvfr_data_pipelines.utils.s3 import S3Client

DAG_FOLDER = "datagouvfr_data_pipelines/data_processing/"
TMP_FOLDER = f"{AIRFLOW_DAG_TMP}finess/"
s3_open = S3Client(bucket=S3_BUCKET_DATA_PIPELINE_OPEN)

with open(f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}sante/finess/config/dgv.json") as fp:
    config = json.load(fp)


def check_if_modif(scope: str):
    return prod_client.resource(
        id=config[scope]["prod"]["resource_id"]
    ).check_if_more_recent_update(
        dataset_id=Resource(config[scope]["source_resource"]).dataset_id
    )


def load_df_sections(scope: str) -> list[pd.DataFrame]:
    logging.info(f"Getting standard Finess {scope}")
    rows = (
        requests.get(
            f"https://www.data.gouv.fr/api/1/datasets/r/{config[scope]['source_resource']}"
        )
        .content.decode("utf8")
        .split("\n")
    )
    columns = config[scope]["columns"]
    sections: list[list] = [[] for _ in range(len(columns))]
    logging.info(f"Looking for {len(columns)} section(s)")
    prefix, current_section = None, None
    for idx, row in enumerate(rows):
        if idx == 0:
            if not row.startswith("finess"):
                raise ValueError(f"Unexpected first row: {row}")
            continue
        if prefix is None:
            prefix = row.split(";")[0]
            current_section = 0
        if row.startswith(prefix):
            sections[current_section].append(row)
        else:
            current_section += 1
            if current_section > len(sections):
                raise ValueError(
                    f"An unexpected section n°{current_section} was detected: {row}"
                )
            prefix = row.split(";")[0]
            sections[current_section].append(row)
    return [
        pd.read_csv(
            StringIO("\n".join(sections[k])),
            names=["index"] + columns[k],
            dtype=str,
            sep=";",
        ).drop("index", axis=1)
        for k in range(len(columns))
    ]


@task()
def build_finess_table_etablissements():
    scope = "etablissements"
    # this one is the "normal" Finess file
    logging.info(f"Getting standard Finess {scope}")
    df_finess = load_df_sections(scope)[0]
    # we also retrieve the geolocalised version, because some row can be missing
    logging.info("Getting geolocalised file")
    df_finess_geoloc, df_geoloc = load_df_sections("geoloc")
    # retrieving missing rows
    missing = df_finess_geoloc.loc[
        ~(df_finess_geoloc["nofinesset"].isin(set(df_finess["nofinesset"].to_list())))
        | ~(df_finess_geoloc["nofinessej"].isin(set(df_finess["nofinessej"].to_list())))
    ]
    logging.info(f"Adding {len(missing)} rows from geoloc")
    final_finess = pd.concat([df_finess, missing], ignore_index=True)
    # merging the two parts
    logging.info("> Merging")
    merged = pd.merge(
        final_finess,
        df_geoloc,
        on="nofinesset",
        how="outer",
    )
    merged.to_csv(
        TMP_FOLDER + f"finess_{scope}.csv",
        index=False,
        sep=";",  # because "," is in the sourcecoordet column
    )


@task()
def build_and_save(scope: str):
    dfs = load_df_sections(scope)
    if len(dfs) == 1:
        dfs[0].to_csv(
            TMP_FOLDER + f"finess_{scope}.csv",
            index=False,
            sep=";",
        )
    elif len(dfs) == 2:
        matching, df_data = dfs
        key = None
        if (
            "nofinesset" in config[scope]["columns"][1]
            and "nofinessej" in config[scope]["columns"][1]
        ):
            # idk why there are two sections in this case but anyway
            logging.warning("Both merge keys in the data, keep the file as it is")
            merged = df_data
        else:
            if "nofinesset" in config[scope]["columns"][1]:
                key = "nofinesset"
            elif "nofinessej" in config[scope]["columns"][1]:
                key = "nofinessej"
            else:
                raise ValueError(
                    f"None of the known keys is in the data: {list(df_data.columns)}"
                )
            logging.info(f"Detected merge key: {key}")
            merged = pd.merge(
                df_data,
                matching,
                on=key,
                how="outer",
            )
            merged = merged[
                config[scope]["columns"][0]
                + [
                    col
                    for col in merged.columns
                    if col not in config[scope]["columns"][0]
                ]
            ]
        merged.to_csv(
            TMP_FOLDER + f"finess_{scope}.csv",
            index=False,
            sep=";",
        )
    else:
        # this shouldn't be possible but we never know
        raise ValueError(f"Too many sections to handle: {len(dfs)}")


@task()
def send_to_s3(scope: str):
    s3_open.send_file(
        File(
            source_path=TMP_FOLDER,
            source_name=f"finess_{scope}.csv",
            dest_path="finess/",
            dest_name=f"finess_{scope}.csv",
            content_type="text/csv",
        ),
        ignore_airflow_env=True,
    )


@task()
def publish_on_datagouv(scope: str):
    date = datetime.today().strftime("%d-%m-%Y")
    source_dataset = Resource(config[scope]["source_resource"]).dataset_id
    local_client.resource(
        dataset_id=config[scope][AIRFLOW_ENV]["dataset_id"],
        id=config[scope][AIRFLOW_ENV]["resource_id"],
        fetch=False,
    ).update(
        payload={
            "url": (
                f"https://object.files.data.gouv.fr/{S3_BUCKET_DATA_PIPELINE_OPEN}"
                f"/finess/finess_{scope}.csv"
            ),
            "filesize": os.path.getsize(TMP_FOLDER + f"finess_{scope}.csv"),
            "title": (f"{config[scope]['title']} au {date}"),
            "format": "csv",
            "description": (
                f"{config[scope]['title']}"
                " (créé à partir des [fichiers du Ministère des Solidarités et de la santé]"
                f"({prod_client.base_url}/datasets/{source_dataset}/))"
                f" (dernière mise à jour le {date})"
            ),
        },
    )


@task()
def send_notification_mattermost():
    dataset_id = config["etablissements"][AIRFLOW_ENV]["dataset_id"]
    send_message(
        text=(
            ":mega: Données Finess mises à jour.\n"
            f"- Données stockées sur S3 - Bucket {S3_BUCKET_DATA_PIPELINE_OPEN}\n"
            f"- Données publiées [sur data.gouv.fr]({local_client.base_url}/datasets/{dataset_id}/)"
        )
    )
