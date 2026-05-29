import json
import logging
import os
import re
from zipfile import ZipFile

from airflow.sdk import task
from datagouv import Dataset
import pandas as pd
import requests

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_HOME,
    AIRFLOW_DAG_TMP,
    AIRFLOW_ENV,
    S3_URL_RBX,
    SECRET_S3_PASSWORD,
    SECRET_S3_USER,
)
from datagouvfr_data_pipelines.utils.datagouv import local_client
from datagouvfr_data_pipelines.utils.s3 import S3Client
from datagouvfr_data_pipelines.utils.tchap import send_message

DAG_FOLDER = AIRFLOW_DAG_HOME + "datagouvfr_data_pipelines/data_processing/"
TMP_FOLDER = f"{AIRFLOW_DAG_TMP}dvf/"
SOURCE_DATASET_ID = "5c4ae55a634f4117716d5656"
GEOLOC_DATASET_ID = "5cc1b94a634f4165e96436c1"
bucket = "dataeng-open"


def check_if_modif():
    # triggering the pipeline if any of the source dataset's resource has been
    # updated more recently than the agregated file
    # with open(DAG_FOLDER + "dvf/explore/config.json", "r") as f:
    #     config = json.load(f)
    # return Resource(
    #     id=config["concat"]["prod"]["resource_id"],
    # ).check_if_more_recent_update(dataset_id=SOURCE_DATASET_ID)
    return True


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


def merge_parcelles(restr_output: pd.DataFrame, parcelle_file: str) -> pd.DataFrame:
    logging.info("Merging in batches with " + parcelle_file)
    parcelles_prefixes = sorted(
        restr_output["id_parcelle"]
        .str[:3]
        .where(
            restr_output["id_parcelle"].str.startswith("97"),
            restr_output["id_parcelle"].str[:2],
        )
        .unique()
    )
    merged = []
    storage_options = {
        "client_kwargs": {"endpoint_url": "https://" + S3_URL_RBX},
        "key": SECRET_S3_USER,
        "secret": SECRET_S3_PASSWORD,
    }
    for idx, prefix in enumerate(parcelles_prefixes):
        if idx == len(parcelles_prefixes) - 1:
            high = "99999"
        else:
            high = parcelles_prefixes[idx + 1]
        logging.info(f"> parcelles between {prefix} and {high}")
        sample_dvf = restr_output.loc[
            restr_output["id_parcelle"].str.startswith(prefix)
        ]
        # for RAM optimization
        restr_output.drop(sample_dvf.index, inplace=True)
        sample_geo_parcelles = pd.read_parquet(
            f"s3://{bucket}/{parcelle_file}",
            storage_options=storage_options,
            columns=["id", "latitude", "longitude"],
            filters=[("id", ">=", prefix), ("id", "<", high)],
        ).rename({"id": "id_parcelle"}, axis=1)
        merged.append(
            pd.merge(
                sample_dvf,
                sample_geo_parcelles,
                on="id_parcelle",
                how="left",
            )
        )
        del sample_dvf
        del sample_geo_parcelles
        logging.info(
            f"> {round(len(merged[-1].loc[merged[-1]['latitude'].isna()]) / len(merged[-1]) * 100, 2)}% missing"
        )
    return pd.concat(merged, ignore_index=True)


def enrich_year(
    file: str,
    # arrond: dict,
    map_cultures: dict[str, dict],
    available_dates: dict[str, str],
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
        for sw in {
            "Le",
            "La",
            "Les",
            "En",
            "Sur",
            "Sous",
            "De",
            "Des",
            "Du",
            "Au",
            "Aux",
        }
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
    # sorting to group mutations in the dataframe
    output.sort_values(by=["date_mutation", "valeur_fonciere"], inplace=True)
    # new mutation id when either date or price changes
    mask = (output["date_mutation"] != output["date_mutation"].shift()) | (
        output["valeur_fonciere"] != output["valeur_fonciere"].shift()
    )
    output["id_mutation"] = f"{year}-" + mask.cumsum().astype(str)
    output.reset_index(drop=True, inplace=True)
    expected_len = len(output)

    # adding geo columns
    restr_available_dates = [
        max(k for k in available_dates.keys() if k.startswith(f"{int(year) - 1}"))
    ] + sorted([k for k in available_dates.keys() if k.startswith(f"{year}")])
    if restr_available_dates[-1] < f"{year}-12-31":
        restr_available_dates.append(f"{year}-12-31")
    logging.info(restr_available_dates)
    geoloced = []
    remainders = None
    for k in range(len(restr_available_dates) - 1):
        dmin, dmax = restr_available_dates[k], restr_available_dates[k + 1]
        restr_ouput = output.loc[
            output["date_mutation"].between(
                dmin, dmax, inclusive="both" if dmax == f"{year}-12-31" else "left"
            )
        ]
        output.drop(restr_ouput.index, inplace=True)
        logging.info(f"{len(restr_ouput)} rows between {dmin} and {dmax}")
        if remainders is not None and not remainders.empty:
            logging.info(f"- adding {len(remainders)} remainders")
            restr_ouput = pd.concat([restr_ouput, remainders], ignore_index=True)
        if len(restr_ouput) == 0:
            logging.info("> skipping")
            continue
        enriched = merge_parcelles(restr_ouput, available_dates[dmin])
        remainders = enriched.loc[enriched["longitude"].isna()][
            [c for c in enriched.columns if c not in ["latitude", "longitude"]]
        ]
        geoloced.append(enriched.dropna(subset="longitude"))
    geoloced = pd.concat(geoloced + [remainders], ignore_index=True).sort_values(
        by="id_mutation", key=lambda col: col.str.split("-").str[1].astype(int)
    )
    assert len(geoloced) == expected_len
    del output
    logging.warning(
        f"No coords: {round(sum(geoloced['longitude'].isna()) / len(geoloced) * 100, 2)}%"
    )

    logging.info("Saving file...")
    geoloced.to_csv(
        TMP_FOLDER + f"full-{year}.csv.gz",
        index=False,
        compression="gzip",
    )
    del geoloced


@task()
def enrich_years(files, **context):
    # we can't parallelize for RAM containment

    # arrondissements_muni = context["ti"].xcom_pull(
    #     key="arrondissements_muni", task_ids="download_source_data"
    # )
    map_cultures = {}
    for scope in ["cultures", "cultures-speciales"]:
        with open(DAG_FOLDER + f"dvf/geoloc/data/{scope}.json", "r") as f:
            map_cultures[scope] = json.load(f)
    s3_client = S3Client(
        bucket=bucket,
        user=SECRET_S3_USER,
        pwd=SECRET_S3_PASSWORD,
        s3_url=S3_URL_RBX,
    )
    available_dates = {
        o.split(".")[0].split("-", maxsplit=3)[-1]: o
        for o in s3_client.get_files_from_prefix(
            "parcelles/",
            ignore_airflow_env=True,
        )
    }
    logging.info(available_dates)
    for file in files:
        enrich_year(
            file,
            # arrond=arrondissements_muni,  # not used (yet?)
            map_cultures=map_cultures,
            available_dates=available_dates,
        )
    # deleting in the end so that if the loop above fails, we can rerun safely
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
