import json
import logging
import os
import re
from time import sleep
from zipfile import ZipFile

from airflow.decorators import task
from datagouv import Dataset, Resource
import geopandas as gpd
import pandas as pd
import requests
from shapely import Polygon

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
    with open(DAG_FOLDER + "dvf/explore/config.json", "r") as f:
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
    # one major version per year since 2020
    version = max_year - 2020
    r = requests.get(
        f"https://unpkg.com/@etalab/decoupage-administratif@{version}/data/communes.json"
    ).json()
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
        + (section.rjust(2, "0") if pd.notna(section := row["Section"]) else "00")
        + (num_plan.rjust(4, "0") if pd.notna(num_plan := row["No plan"]) else "0000")
    )


def get_parcelle_geometry(pid: str) -> Polygon | None:
    # could also be with https://data.geopf.fr/geocodage/search?index=parcel
    # but some parcels seem to be missing? e.g. 01033458AB0174
    url = (
        "https://apicarto.ign.fr/api/cadastre/parcelle?"
        f"code_insee={pid[:5]}&section={pid[8:10]}&numero={pid[10:]}"
    )
    r = requests.get(url)
    if r.status_code == 429:
        logging.warning("Reached rate-limit, sleeping then retrying...")
        sleep(r.headers.get("retry-after", 5))
        return get_parcelle_geometry(pid)
    try:
        return Polygon(r.json()["features"][0]["geometry"]["coordinates"][0][0])
    except Exception:
        return None


def add_geoloc(output: pd.DataFrame) -> None:
    # a potential improvement: reuse parcelles from one enrichment to another
    # so that we don't retrieve the same twice, but that means more RAM consumption
    # and assumes that many mutations touch the same parcel across the years
    parcelles = pd.DataFrame({"id_parcelle": output["id_parcelle"].unique()})
    logging.info(f"Retrieving geometry for {len(parcelles)} parcelles...")
    parcelles["geometry"] = parcelles["id_parcelle"].apply(get_parcelle_geometry)
    parcelles = gpd.GeoDataFrame(parcelles)
    parcelles["longitude"] = parcelles.centroid.x
    parcelles["latitude"] = parcelles.centroid.y
    del parcelles["geometry"]
    logging.info("Merging coordinates...")
    # trying to be memory efficient so map instead of merge
    for col in ["latitude", "longitude"]:
        output[col] = output["id_parcelle"].map(parcelles.set_index("id_parcelle")[col])
    del parcelles


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
        source["Date mutation"].str.slice(
            6,
        )
        + "-"
        + source["Date mutation"].str.slice(3, 5)
        + "-"
        + source["Date mutation"].str.slice(0, 2)
    )
    output["numero_disposition"] = source["No disposition"]
    output["nature_mutation"] = source["Nature mutation"]
    output["valeur_fonciere"] = (
        source["Valeur fonciere"].str.replace(",", ".").astype(float)
    )
    output["adresse_numero"] = source["No voie"]
    output["adresse_suffixe"] = source["B/T/Q"]
    output["adresse_nom_voie"] = source.apply(
        lambda row: (
            row["Type de voie"] + " " + row["Voie"]
            if pd.notna(row["Type de voie"]) and pd.notna(row["Voie"])
            else pd.NA
        ),
        axis=1,
    )
    output["adresse_code_voie"] = source["Code voie"].str.rjust(4, "0")
    output["code_postal"] = source["Code postal"].str.rjust(5, "0")
    # not as sophisticated as the original code
    output["code_commune"] = source.apply(build_code_commune, axis=1)
    patterns = {
        f"{sep}{sw}{sep}": f"{sep}{sw.lower()}{sep}"
        for sw in {"Le", "La", "Les", "En", "Sur", "De"}
        for sep in {"-", " "}
    }
    output["nom_commune"] = source["Commune"].str.title()
    # this can be changed when upgrading to pandas 3 (pat can be a dict)
    for pat, repl in patterns.items():
        output["nom_commune"] = output["nom_commune"].str.replace(pat, repl)
    output["code_departement"] = output["code_commune"].str.extract(
        r"^(97.|..)", expand=False
    )
    # TODO: fill in the "ancien..." columns
    output["ancien_code_commune"] = ""
    output["ancien_nom_commune"] = ""
    output["id_parcelle"] = source.apply(build_parcelle_id, axis=1)
    output["ancien_id_parcelle"] = ""
    output["numero_volume"] = source["No Volume"]
    for no in ["1er", "2eme", "3eme", "4eme", "5eme"]:
        output[f"lot{no[0]}_numero"] = source[f"{no} lot"]
        output[f"lot{no[0]}_surface_carrez"] = (
            source[f"Surface Carrez du {no} lot"].str.replace(",", ".").astype(float)
        )
    output["nombre_lots"] = source["Nombre de lots"]
    output["code_type_local"] = source["Code type local"]
    output["type_local"] = source["Type local"]
    output["surface_reelle_bati"] = (
        source["Surface reelle bati"].str.replace(",", ".").astype(float)
    )
    output["nombre_pieces_principales"] = source["Nombre pieces principales"]
    output["code_nature_culture"] = source["Nature culture"]
    output["nature_culture"] = source["Nature culture"].map(map_cultures["cultures"])
    output["code_nature_culture_speciale"] = source["Nature culture speciale"]
    output["nature_culture_speciale"] = source["Nature culture speciale"].map(
        map_cultures["cultures-speciales"]
    )
    output["surface_terrain"] = (
        source["Surface terrain"].str.replace(",", ".").astype(float)
    )
    del source

    logging.info("Creating mutation ids...")
    # new mutation id when either date or price changes
    mask = (output["date_mutation"] != output["date_mutation"].shift()) | (
        output["valeur_fonciere"] != output["valeur_fonciere"].shift()
    )
    output["id_mutation"] = f"{year}-" + mask.cumsum().astype(str)

    add_geoloc(output)
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
    map_cultures = {}
    for scope in ["cultures", "cultures-speciales"]:
        with open(DAG_FOLDER + f"dvf/geoloc/data/{scope}.json", "r") as f:
            map_cultures[scope] = json.load(f)
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
        [res for res in dataset.resources if res.type == "main" and "full" in res.url],
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
