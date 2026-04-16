import json
import logging
import os
import re
from zipfile import ZipFile

from airflow.decorators import task
from datagouv import Dataset, Resource
import pandas as pd
import requests

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_HOME,
    AIRFLOW_DAG_TMP,
    AIRFLOW_ENV,
)
from datagouvfr_data_pipelines.utils.datagouv import local_client
from datagouvfr_data_pipelines.utils.tchap import send_message

DAG_FOLDER = AIRFLOW_DAG_HOME + "datagouvfr_data_pipelines/data_processing/"
TMP_FOLDER = f"{AIRFLOW_DAG_TMP}dvf/"
SOURCE_DATASET_ID = "5c4ae55a634f4117716d5656"
GEOLOC_DATASET_ID = "5cc1b94a634f4165e96436c1"


def check_if_modif():
    # triggering the pipeline if any of the source dataset's resource has been
    # updated more recently than the agregated file
    with open(DAG_FOLDER + f"dvf/explore/config.json", "r") as f:
        config = json.load(f)
    return Resource(
        id=config["concat"]["prod"]["resource_id"],
    ).check_if_more_recent_update(dataset_id=SOURCE_DATASET_ID)


@task()
def download_source_data(**context):
    dvf_dataset = Dataset(SOURCE_DATASET_ID)
    data = [res for res in dvf_dataset.resources if res.type == "main"]
    if len(data) not in {5, 6}:
        # 5 full years, or half first and last years and 4 full years
        raise ValueError(f"Unexpected number of resources: {len(data)}")
    files = []
    max_year = 2000
    for res in data:
        logging.info(res.title)
        file_name = res.url.split("/")[-1]
        max_year = max(max_year, int(file_name.split(".")[0].split("-")[1]))
        if not re.match(r"^valeursfoncieres-\d{4}\.txt\.zip$", file_name):
            raise ValueError(f"Unexpected file name: {file_name}")
        dest_path = TMP_FOLDER + file_name
        res.download(dest_path)
        with ZipFile(dest_path, mode="r") as z:
            zipped = z.namelist()
            if len(zipped) != 1:
                raise ValueError("Unexpected number of files in zip")
            z.extractall(TMP_FOLDER)
            files.append(zipped[0])
        os.remove(dest_path)

    logging.info("Retrieving reference data...")
    r = requests.get("https://unpkg.com/@etalab/decoupage-administratif/data/communes.json").json()
    arrondissements_muni = [
        {"nom": k["nom"], "code": k["code"], "type": "COM"}
        for k in r
        if k["type"] == "arrondissement-municipal"
    ]
    context["ti"].xcom_push(key="arrondissements_muni", value=arrondissements_muni)
    return files


def build_code_commune(row: pd.Series) -> str:
    code_dep = row["Code departement"]
    code_com = row["Code commune"]
    if code_dep.startswith("97"):
        code_com = code_com.rjust(2, "0")
    else:
        code_com = code_com.rjust(3, "0")
        code_dep = code_dep.rjust(2, "0")
    return code_dep + code_com


def build_parcelle_id(row: pd.Series) -> str:
    return (
        build_code_commune(row)
        + (
            prefix.rjust(3, "0")
            if pd.notna(prefix := row["Prefixe de section"])
            else "000"
        )
        + (
            section.rjust(2, "0")
            if pd.notna(section := row["Section"])
            else "00"
        )
        + (
            num_plan.rjust(4, "0")
            if pd.notna(num_plan := row["No plan"])
            else "0000"
        )
    )


def enrich_year(
    file: str,
    arrond: dict,
    map_cultures: dict[str, dict],
):
    logging.info(f"Processing {file}")
    year = file.split(".")[0].split("-")[1]
    source = pd.read_csv(TMP_FOLDER + file, dtype=str, sep="|")
    logging.info("Building output...")
    output = pd.DataFrame()
    output["id_mutation"] = ""
    output["date_mutation"] = (
        source["Date mutation"].str.slice(6,)
        + "-"
        + source["Date mutation"].str.slice(3, 5)
        + "-"
        + source["Date mutation"].str.slice(0, 2)
    )
    output["numero_disposition"] = source["No disposition"]
    output["nature_mutation"] = source["Nature mutation"]
    output["valeur_fonciere"] = source["Valeur fonciere"].str.replace(",", ".").astype(float)
    output["adresse_numero"] = source["No voie"]
    output["adresse_suffixe"] = source["B/T/Q"]
    output["adresse_nom_voie"] = source.apply(
        lambda row: row["Type de voie"] + " " + row["Voie"]
        if pd.notna(row["Type de voie"]) and pd.notna(row["Voie"])
        else pd.NA,
        axis=1,
    )
    output["adresse_code_voie"] = source["Code voie"].str.rjust(4, "0")
    output["code_postal"] = source["Code postal"].str.rjust(5, "0")
    # output["code_commune"] = 
    # output["nom_commune"] = 
    # output["code_departement"] = getCodeDepartement(communeActuelle.code)
    output["ancien_code_commune"] = ""
    output["ancien_nom_commune"] = ""
    output["id_parcelle"] = source.apply(build_parcelle_id, axis=1)
    output["ancien_id_parcelle"] = ""
    output["numero_volume"] = source["No Volume"]
    for no in ["1er", "2eme", "3eme", "4eme", "5eme"]:
        output[f"lot{no[0]}_numero"] = source[f"{no} lot"]
        output[f"lot{no[0]}_surface_carrez"] = source[f"Surface Carrez du {no} lot"].str.replace(",", ".").astype(float)
    output["nombre_lots"] = source["Nombre de lots"]
    output["code_type_local"] = source["Code type local"]
    output["type_local"] = source["Type local"]
    output["surface_reelle_bati"] = source["Surface reelle bati"].str.replace(",", ".").astype(float)
    output["nombre_pieces_principales"] = source["Nombre pieces principales"]
    output["code_nature_culture"] = source["Nature culture"]
    output["nature_culture"] = source["Nature culture"].map(map_cultures["cultures"])
    output["code_nature_culture_speciale"] = source["Nature culture speciale"]
    output["nature_culture_speciale"] = source["Nature culture speciale"].map(map_cultures["cultures-speciales"])
    output["surface_terrain"] = source["Surface terrain"].str.replace(",", ".").astype(float)
    output["longitude"] = ""
    output["latitude"] = ""
    del source

    logging.info("Creating mutation ids...")
    # new mutation id when either date or price changes
    mask = (
        (output["date_mutation"] != output["date_mutation"].shift())
        | (output["valeur_fonciere"] != output["valeur_fonciere"].shift())
    )
    output["id_mutation"] = f"{year}-" + mask.cumsum().astype(str)
    print(output)

    logging.info("Saving file...")
    output.to_csv(
        TMP_FOLDER + f"full-{year}.csv.gz",
        index=False,
        compression="gzip",
    )
    del output


@task()
def enrich_years(files, **context):
    # we can't parallelize for RAM containment
    arrondissements_muni = context["ti"].xcom_pull(
        key="arrondissements_muni", task_ids="download_source_data"
    )
    map_cultures = {
        scope: {
            (row := list(it.items()))[0][1]: row[1][1]
            for it in pd.read_csv(
                DAG_FOLDER + f"dvf/geoloc/data/table-{scope}.csv"
            ).to_dict(orient="records")
        }
        for scope in ["cultures", "cultures-speciales"]
    }
    for file in files:
        enrich_year(
            file,
            arrond=arrondissements_muni,
            map_cultures=map_cultures,
        )
    for file in files:
        os.remove(TMP_FOLDER + file)


@task()
def publish_datagouv():
    dataset = local_client.dataset(GEOLOC_DATASET_ID)
    yearly_resources = sorted(
        [
            res for res in dataset.resources
            if res.type == "main"
            and "full" in res.url
        ],
        key=lambda r: r.title,
    )
    files = sorted(os.listdir(TMP_FOLDER))

    # we want to replace resources when we can, so that we keep the history
    if len(yearly_resources) == 5:
        # october delivery: one more file (oldest year last semester and latest year first semester)
        assert len(files) == 6
        for idx, file in enumerate(files):
            kwargs = {
                "payload": {
                    "title": f"DVF {file.split('.')[0].split(('-'))[1]}",
                },
                "file_to_upload": TMP_FOLDER + file,
            }
            if idx < 5:
                yearly_resources[idx].update(**kwargs)
            else:
                dataset.create_static(**kwargs)
    elif len(yearly_resources) == 6:
        # april delivery: one file fewer (five full years)
        assert len(files) == 5
        for idx, res in enumerate(yearly_resources):
            if idx < 5:
                file = files[idx]
                res.update(
                    payload={"title": f"DVF {file.split('.')[0].split(('-'))[1]}"},
                    file_to_upload=TMP_FOLDER + file,
                )
            else:
                res.delete()
    else:
        raise ValueError("Unexpected number of resources in geoloc dataset")


@task()
def notification() -> None:
    send_message(
        f"DVF géolocalisé mis à jour :\n"
        f"\n- publié [sur {'demo.' if AIRFLOW_ENV == 'dev' else ''}data.gouv.fr]"
        f"({local_client.base_url}/datasets/{GEOLOC_DATASET_ID})"
    )
