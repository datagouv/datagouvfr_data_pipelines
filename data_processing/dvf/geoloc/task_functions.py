import gc
import json
import logging
import os
import re
from time import sleep
from zipfile import ZipFile

import pandas as pd
import requests
from airflow.sdk import task
from datagouv import Dataset
from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_HOME,
    AIRFLOW_DAG_TMP,
    AIRFLOW_ENV,
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

    # bypassing for now, the DAG has not completed yet
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
        # checking that the year is where we expect it in the resource's title
        if not re.match(r"^valeursfoncieres-\d{4}\.txt\.zip$", file_name):
            raise ValueError(f"Unexpected file name: {file_name}")
        max_year = max(max_year, int(file_name.split(".")[0].split("-")[1]))
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
    # downloading cadastre data for downstream merges
    # one major version per year since 2020, which was version 0.x.y (then incremented each year, ask Thomas for more infos)
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


def merge_parcelles(
    restr_output: pd.DataFrame, parcelle_file: str, s3_client: S3Client
) -> pd.DataFrame:
    # getting and merging parcelle's geographical columns from parquet files made by Thomas
    logging.info("Merging in batches with " + parcelle_file)
    parcelles_prefixes = sorted(restr_output["id_parcelle"].str[:3].unique())
    merged = []
    storage_options = {
        "client_kwargs": {"endpoint_url": "https://" + s3_client.url},
        "key": s3_client.login,
        "secret": s3_client.password,
    }
    # it would be too RAM heavy to merge everything at once, so batch-merging using the parcelle's prefixes
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
        # expecting around 1-2% missing coords per batch
        logging.info(
            f"> {round(len(merged[-1].loc[merged[-1]['latitude'].isna()]) / len(merged[-1]) * 100, 2)}% missing"
        )
    del restr_output
    return pd.concat(merged, ignore_index=True)


def enrich_year(
    file: str,
    # arrond: dict,
    map_cultures: dict[str, dict],
    available_dates: dict[str, str],
    s3_client: S3Client,
):
    # the whole process is RAM heavy, so trying to be efficient to fit within Airflow's capabilities
    year = file.split(".")[0].split("-")[1]  #  # "valeursfoncieres-2023.txt" => "2023"
    if f"full-{year}.csv.gz" in os.listdir(TMP_FOLDER):
        logging.info(f"Skipping {file} - already processed")  # In case of retry
        return
    
    logging.info(f"Processing {file}")
    source = pd.read_csv(TMP_FOLDER + file, dtype=str, sep="|")
    logging.info("Building output...")
    output = pd.DataFrame()
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
        source["Valeur fonciere"].str.replace(",", ".").astype("float32")
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
            source[f"Surface Carrez du {no} lot"]
            .str.replace(",", ".")
            .astype("float32")
        )
    output["nombre_lots"] = source["Nombre de lots"]
    output["code_type_local"] = source["Code type local"]
    output["type_local"] = source["Type local"]
    output["surface_reelle_bati"] = (
        source["Surface reelle bati"].str.replace(",", ".").astype("float32")
    )
    output["nombre_pieces_principales"] = source["Nombre pieces principales"]
    output["code_nature_culture"] = source["Nature culture"]
    output["nature_culture"] = source["Nature culture"].map(map_cultures["cultures"])
    output["code_nature_culture_speciale"] = source["Nature culture speciale"]
    output["nature_culture_speciale"] = source["Nature culture speciale"].map(
        map_cultures["cultures-speciales"]
    )
    output["surface_terrain"] = (
        source["Surface terrain"].str.replace(",", ".").astype("float32")
    )
    del source
    gc.collect()

    logging.info("Creating mutation ids...")
    # sorting to group mutations in the dataframe
    output["date_mutation"] = pd.to_datetime(output["date_mutation"])
    output.sort_values(by=["date_mutation", "valeur_fonciere"], inplace=True)
    output["date_mutation"] = output["date_mutation"].dt.strftime("%Y-%m-%d")
    # new mutation id when either date or price changes
    mask = (output["date_mutation"] != output["date_mutation"].shift()) | (
        output["valeur_fonciere"] != output["valeur_fonciere"].shift()
    )
    output.insert(0, "id_mutation", f"{year}-" + mask.cumsum().astype(str)) # First col
    del mask
    output.reset_index(drop=True, inplace=True)
    expected_len = len(output)

    # adding geo columns
    restr_available_dates = [
        max(k for k in available_dates.keys() if k.startswith(f"{int(year) - 1}"))
    ] + sorted([k for k in available_dates.keys() if k.startswith(year)])
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
        # dropping from original df to keep RAM low
        output.drop(restr_ouput.index, inplace=True)
        logging.info(f"{len(restr_ouput)} rows between {dmin} and {dmax}")
        if remainders is not None and not remainders.empty:
            # for parcelles that didn't get geolocalized in the expected batch, we'll try again at each upcoming batch
            logging.info(f"- adding {len(remainders)} remainders")
            restr_ouput = pd.concat([restr_ouput, remainders], ignore_index=True)
        if len(restr_ouput) == 0:
            logging.info("> skipping")
            continue
        enriched = merge_parcelles(restr_ouput, available_dates[dmin], s3_client)
        remainders = enriched.loc[enriched["longitude"].isna()][
            [c for c in enriched.columns if c not in ["latitude", "longitude"]]
        ]
        geoloced.append(enriched.dropna(subset="longitude"))
        del enriched
        gc.collect()
    logging.info("Done with geoloc, concatenating results...")
    geoloced.append(remainders)
    del remainders
    # using pd.concat on geoloced directly is too RAM heavy, so workaround
    final = pd.DataFrame()
    while geoloced:
        logging.info(f"> {len(geoloced)} dfs still to concatenate")
        final = pd.concat([final, geoloced[0]], ignore_index=True)
        del geoloced[0]
        gc.collect()
    del geoloced
    logging.info("Sorting by mutation id...")
    final["_sort_key"] = final["id_mutation"].str[len(year) + 1 :].astype("int32")
    final.sort_values("_sort_key", inplace=True)
    final.drop(columns="_sort_key", inplace=True)
    assert len(final) == expected_len
    del output
    logging.warning(
        f"No coords: {round(sum(final['longitude'].isna()) / len(final) * 100, 2)}%"
    )

    logging.info("Saving file...")
    final.to_csv(
        TMP_FOLDER + f"full-{year}.csv.gz",
        index=False,
        compression="gzip",
    )
    del final
    # end-of-loop garbage management, giving time to reclaim memory
    gc.collect()
    sleep(5)


@task()
def enrich_years(files, **context):
    # we can't parallelize for RAM containment

    # arrondissements_muni = context["ti"].xcom_pull(
    #     key="arrondissements_muni", task_ids="download_source_data"
    # )
    map_cultures = {}
    for scope in ["cultures", "cultures-speciales"]:
        with open(DAG_FOLDER + f"dvf/geoloc/data/{scope}.json", "r") as f:
            map_cultures[scope] = json.load(
                f
            )  # {"cultures": {"AB": "terrains a bâtir", ...}, "cultures-speciales": {"ABREU": "Abreuvoirs",...}}
    s3_client = S3Client(
        bucket=bucket,
        conn_name="S3_OVH_RBX",
    )
    # Maps each cadastre snapshot date to its S3 file path
    available_dates = {
        o.split(".")[0].split("-", maxsplit=3)[-1]: o
        for o in s3_client.get_files_from_prefix(
            "parcelles/",
            ignore_airflow_env=True,
        )
    }  # {"2020-01-01": "parcelles/cadastre-point-wgs84-2020-01-01.parquet", ...}
    logging.info(f"Available cadastre snapshots : {available_dates}")
    for file in files:
        enrich_year(
            file,
            # arrond=arrondissements_muni,  # not used (yet?)
            map_cultures=map_cultures,
            available_dates=available_dates,
            s3_client=s3_client,
        )
    # deleting in the end so that if the loop above fails, we can rerun safely
    for file in files:
        os.remove(TMP_FOLDER + file)


@task()
def publish_datagouv():
    # april delivery: five full years, october delivery: one more file
    # (oldest year last semester and latest year first semester)
    files = sorted(f for f in os.listdir(TMP_FOLDER) if f.startswith("full-"))
    if len(files) not in {5, 6}:
        raise ValueError(f"Unexpected number of files to publish: {len(files)}")

    # matching resources to files by year (and not by index), so that a given year always
    # keeps the same resource: we want to replace resources when we can, so that we keep
    # the history. Legacy resources that don't hold a yearly file (the single file and the CSV
    # tree) are not managed here.
    dataset = local_client.dataset(GEOLOC_DATASET_ID)
    existing = {
        match.group(1): res
        for res in dataset.resources
        if res.type == "main"
        and (
            match := re.search(r"full-(\d{4})\.", res.url)
        )  # url match avoids any wrong match with a resource renamed on UI
    }  # {"2021": Resource(title="DVF 2021"...), ..., "2025": Resource(...)}

    to_publish = set()
    for file in files:
        year = file.split(".")[0].split("-")[1]  # "full-2022.csv.gz" => "2022"
        to_publish.add(year)
        kwargs = {
            "payload": {"title": f"DVF {year}"},
            "file_to_upload": TMP_FOLDER + file,
        }
        if year in existing:
            logging.info(f"Updating resource for {year}")
            existing[year].update(**kwargs)
        else:
            logging.info(f"Creating resource for {year}")
            dataset.create_static(**kwargs)

    # oldest year last semester that fell out of the rolling window
    for year, res in existing.items():
        if year not in to_publish:
            logging.info(f"Deleting resource for {year}, out of the window")
            res.delete()


@task()
def notification() -> None:
    send_message(
        f"DVF géolocalisé mis à jour :\n"
        f"\n- publié [sur {'demo.' if AIRFLOW_ENV == 'dev' else ''}data.gouv.fr]"
        f"({local_client.base_url}/datasets/{GEOLOC_DATASET_ID})"
    )
