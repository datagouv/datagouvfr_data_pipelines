import logging
import gc
import os
import re

from collections.abc import Hashable
from time import sleep
import numpy as np
import pandas as pd

from datagouvfr_data_pipelines.utils.s3 import S3Client
from dags.datagouvfr_data_pipelines.data_processing.dvf.geoloc.utils.latlong import (
    enrich_parcelles_with_coord,
)

LOTS = ["1er", "2eme", "3eme", "4eme", "5eme"]

# output columns copied from the source, renamed only
PASSTHROUGH_COLUMNS = {
    "No disposition": "numero_disposition",
    "Nature mutation": "nature_mutation",
    "No voie": "adresse_numero",
    "B/T/Q": "adresse_suffixe",
    "No Volume": "numero_volume",
    "Nombre de lots": "nombre_lots",
    "Code type local": "code_type_local",
    "Type local": "type_local",
    "Nombre pieces principales": "nombre_pieces_principales",
    "Nature culture": "code_nature_culture",
    "Nature culture speciale": "code_nature_culture_speciale",
    **{f"{no} lot": f"lot{no[0]}_numero" for no in LOTS},
}
# copied too, but parsed as floats by read_csv itself (french decimal comma)
FLOAT_COLUMNS = {
    "Valeur fonciere": "valeur_fonciere",
    "Surface reelle bati": "surface_reelle_bati",
    "Surface terrain": "surface_terrain",
    **{f"Surface Carrez du {no} lot": f"lot{no[0]}_surface_carrez" for no in LOTS},
}
# source columns read only to compute other columns
COMPUTED_SOURCE_COLUMNS = [
    "Date mutation",
    "Type de voie",
    "Voie",
    "Code voie",
    "Code postal",
    "Code departement",
    "Code commune",
    "Commune",
    "Prefixe de section",
    "Section",
    "No plan",
]
# TODO: fill in the "ancien..." columns
EMPTY_COLUMNS = ("ancien_code_commune", "ancien_nom_commune", "ancien_id_parcelle")

# what read_csv reads: everything as a string, except the numeric columns parsed as floats.
SOURCE_DTYPES: dict[Hashable, str] = {
    **{c: "str" for c in list(PASSTHROUGH_COLUMNS) + COMPUTED_SOURCE_COLUMNS},
    **{c: "float32" for c in FLOAT_COLUMNS},
}


def build_code_commune(source: pd.DataFrame) -> pd.Series:
    """Returns the 5 caracters commune code:
    department + commune, 2+3 caracters in mainland France, 3+2 overseas (97x)"""
    code_dep = source["Code departement"]
    code_com = source["Code commune"]
    return pd.Series(
        np.where(
            code_dep.str.startswith("97"),
            code_dep + code_com.str.rjust(2, "0"),
            code_dep.str.rjust(2, "0") + code_com.str.rjust(3, "0"),
        ),
        index=source.index,
    )


def build_parcelle_id(source: pd.DataFrame, code_commune: pd.Series) -> pd.Series:
    """Build 14 caracters parcelle ID
    shared both by DVF df and parcelles coordinates parquet file."""
    return (
        code_commune
        + source["Prefixe de section"].str.rjust(3, "0").fillna("000")
        + source["Section"].str.rjust(2, "0").fillna("00")
        + source["No plan"].str.rjust(4, "0").fillna("0000")
    )


def build_output_columns(
    source: pd.DataFrame, map_cultures: dict[str, dict]
) -> dict[str, pd.Series]:
    """Build all DVF cols that are based off the source only.
    Output one dict entry per col."""
    cols = {}

    # Cols copied from the source, renamed only (floats already parsed by read_csv)
    cols |= {new: source[old] for old, new in PASSTHROUGH_COLUMNS.items()}
    cols |= {new: source[old] for old, new in FLOAT_COLUMNS.items()}
    cols |= {name: "" for name in EMPTY_COLUMNS}

    # Computed cols
    date = source["Date mutation"]
    cols["date_mutation"] = (
        date.str[6:] + "-" + date.str[3:5] + "-" + date.str[:2]
    )  # make str date ISO : DD/MM/YYYY => YYYY-MM-DD
    cols["adresse_nom_voie"] = source["Type de voie"].str.cat(
        source["Voie"], sep=" "
    )  # NaN if either part is missing
    cols["adresse_code_voie"] = source["Code voie"].str.rjust(4, "0")  # "143" => "0143"
    cols["code_postal"] = source["Code postal"].str.rjust(5, "0")
    code_commune = build_code_commune(source)
    cols["code_commune"] = code_commune
    STOP_WORDS = re.compile(
        r"(?<=[- ])(Le|La|Les|En|Sur|Sous|De|Des|Du|Au|Aux)(?=[- ])"
    )  # stop words are lowered only between two separators: "LE PUY EN VELAY" => "Le Puy en Velay"
    cols["nom_commune"] = (
        source["Commune"]
        .str.title()
        .str.replace(STOP_WORDS, lambda m: m.group(0).lower(), regex=True)
    )
    cols["code_departement"] = code_commune.str.extract(
        r"^(97.|..)", expand=False
    )  # Note: 97. match the only overseas departements that are part of DVF : Guadeloupe, Martinique, Guyane, La Réunion
    cols["nature_culture"] = source["Nature culture"].map(map_cultures["cultures"])
    cols["nature_culture_speciale"] = source["Nature culture speciale"].map(
        map_cultures["cultures-speciales"]
    )
    cols["id_parcelle"] = build_parcelle_id(source, code_commune)
    return cols


def enrich_year(
    file: str,
    tmp_folder: str,
    map_cultures: dict[str, dict],
    output_schema: dict[str, str],
    available_dates: dict[str, str],
    s3_client: S3Client,
    bucket: str,
):
    # Note : the whole process is RAM heavy, so trying to be efficient to fit within Airflow's capabilities
    year = file.split(".")[0].split("-")[1]  #  # "valeursfoncieres-2023.txt" => "2023"
    if f"full-{year}.csv.gz" in os.listdir(tmp_folder):
        logging.info(f"Skipping {file} - already processed")  # In case of retry
        return

    # READ SOURCE
    logging.info(f"Processing {file}")
    # only the columns we actually use, and the numeric ones parsed straight to float
    # by the C parser (decimal="," handles the french decimal comma)
    source = pd.read_csv(
        tmp_folder + file,
        sep="|",
        usecols=list(SOURCE_DTYPES),
        dtype=SOURCE_DTYPES,
        decimal=",",
    )

    # COMPUTE MOST COLS
    logging.info("Building output...")
    cols = build_output_columns(source, map_cultures)
    del source
    # dict insertion order IS column order, so the schema order costs no extra copy here.
    # "id_mutation" is inserted first below, latitude and longitude are added by the geoloc step
    base_columns = [
        c for c in output_schema if c not in ("id_mutation", "longitude", "latitude")
    ]
    output = pd.DataFrame({name: cols[name] for name in base_columns})
    del cols
    gc.collect()

    # ADD MUTATIONS ID
    logging.info("Creating mutation ids...")
    output["date_mutation"] = pd.to_datetime(output["date_mutation"])
    output.sort_values(by=["date_mutation", "valeur_fonciere"], inplace=True)
    output["date_mutation"] = output["date_mutation"].dt.strftime("%Y-%m-%d")
    # new mutation id when either date or price changes
    mask = (output["date_mutation"] != output["date_mutation"].shift()) | (
        output["valeur_fonciere"] != output["valeur_fonciere"].shift()
    )
    output.insert(0, "id_mutation", f"{year}-" + mask.cumsum().astype(str))  # First col
    del mask
    output.reset_index(drop=True, inplace=True)

    # ADD LAT, LONG COORDINATES
    expected_len = len(output)
    final = enrich_parcelles_with_coord(
        output, year, available_dates, s3_client, bucket
    )  # Add lat, long coordinates tied to parcelle ID
    del output
    assert len(final) == expected_len

    # SORT
    logging.info("Sorting by mutation id...")
    final["_sort_key"] = final["id_mutation"].str[len(year) + 1 :].astype("int32")
    final.sort_values("_sort_key", inplace=True)
    final.drop(columns="_sort_key", inplace=True)

    # SCHEMA CHECKS & WRITE
    assert list(final.columns) == list(output_schema), list(final.columns)
    assert final.dtypes.astype(str).to_dict() == output_schema
    logging.warning(
        f"No coords: {round(sum(final['longitude'].isna()) / len(final) * 100, 2)}%"
    )

    logging.info("Saving file...")
    final.to_csv(
        tmp_folder + f"full-{year}.csv.gz",
        index=False,
        compression="gzip",
    )
    del final
    # end-of-loop garbage management, giving time to reclaim memory
    gc.collect()
    sleep(5)
