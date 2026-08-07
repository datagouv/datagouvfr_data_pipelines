import logging
import gc
import os
import re

from collections.abc import Hashable
from time import sleep
import numpy as np
import pandas as pd
import pyarrow.fs as pafs
import pyarrow.parquet as pq

from datagouvfr_data_pipelines.utils.s3 import S3Client

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


def match_year_to_snapshots(year, available_dates) -> list[str]:
    """Match year to available snapshots dates.
    Returns a list of ISO str dates ["YYYY-MM-DD", ...]"""
    restr_available_dates = [
        max(
            k for k in available_dates.keys() if k.startswith(f"{int(year) - 1}")
        )  # take last snapshot of the prior year, e.g. "2022-10-01"
    ] + sorted(
        [k for k in available_dates.keys() if k.startswith(year)]
    )  # all snapshots of the current year
    if restr_available_dates[-1] < f"{year}-12-31":
        restr_available_dates.append(
            f"{year}-12-31"
        )  # closing bound of the current year, not a snapshot
    return restr_available_dates


def enrich_parcelles_with_snapshot_coord(
    dvf_batch_df: pd.DataFrame,
    snapshot_file: str,
    s3_client: S3Client,
    bucket: str,
) -> pd.DataFrame:
    """Match parcelle's lat, long coordinates for a snapshot file to the DVF batch,
    relying on parcelle ID.
    Note: snapshot parquet files are produced by Thomas G.
    """
    logging.info("Streaming " + snapshot_file)
    parcelle_ids = pd.Index(dvf_batch_df["id_parcelle"].unique())

    # Open just once the file to accelerate time
    fs = pafs.S3FileSystem(  # type: ignore - lazily init. by pafs GH-38364
        access_key=s3_client.login,
        secret_key=s3_client.password,
        endpoint_override=s3_client.url,
        scheme="https",
    )
    coord_pf = pq.ParquetFile(
        f"{bucket}/{snapshot_file}", filesystem=fs, pre_buffer=True
    )
    logging.info(
        f"> {coord_pf.metadata.num_rows:,} parcelles in {coord_pf.metadata.num_row_groups} row groups"
    )
    coord_to_keep_chunks = []
    for i, coord_batch in enumerate(
        coord_pf.iter_batches(
            batch_size=100_000, columns=["id", "latitude", "longitude"]
        )
    ):
        coord_batch_df = coord_batch.to_pandas()
        coord_to_keep = coord_batch_df.loc[coord_batch_df["id"].isin(parcelle_ids)]
        if not coord_to_keep.empty:
            coord_to_keep_chunks.append(coord_to_keep)
        del coord_batch_df, coord_batch, coord_to_keep
        if i % 20 == 0:
            logging.info(
                f"> batch {i}, {sum(map(len, coord_to_keep_chunks)):,} parcelles matched so far"
            )
    del parcelle_ids
    if not coord_to_keep_chunks:
        raise ValueError(
            f"No matching parcelles ID between DVF batch and coordinates snapshot {snapshot_file}"
        )
    coord_to_keep_df = pd.concat(coord_to_keep_chunks, ignore_index=True).rename(
        {"id": "id_parcelle"}, axis=1
    )
    assert (
        coord_to_keep_df["id_parcelle"].is_unique
    )  # if id is NOT unique in the parquet file, duplicates would appear in the join
    del coord_to_keep_chunks
    gc.collect()

    enriched_batch_df = pd.merge(
        dvf_batch_df, coord_to_keep_df, on="id_parcelle", how="left"
    )
    del coord_to_keep_df
    logging.info(
        f"> {round(len(enriched_batch_df.loc[enriched_batch_df['latitude'].isna()]) / len(enriched_batch_df) * 100, 2)}% missing"
    )  # expecting around 1-2% missing coords
    return enriched_batch_df


def enrich_parcelles_with_coord(
    dvf_df: pd.DataFrame,
    year: str,
    available_dates: dict[str, str],
    s3_client: S3Client,
    bucket: str,
):
    """Enrich DVF dataframe with parcelle's lat, long coordinates.
    Those parcelle coordinates can be defined at a slightly different timeframe than the mutation,
    so looping through each available snapshot of parcelle coordinates.
    """
    restr_available_dates = match_year_to_snapshots(year, available_dates)
    logging.info(restr_available_dates)
    geoloced = []
    remainders = None
    for k in range(len(restr_available_dates) - 1):
        dmin, dmax = (
            restr_available_dates[k],
            restr_available_dates[k + 1],
        )  # lower and upper date bounds for current snapshot
        matching_mutations_df = dvf_df.loc[
            dvf_df["date_mutation"].between(
                dmin, dmax, inclusive="both" if dmax == f"{year}-12-31" else "left"
            )
        ]  # the rows between the snapshot date bounds to process
        dvf_df.drop(
            matching_mutations_df.index, inplace=True
        )  # dropping them from original df to keep RAM low
        logging.info(f"{len(matching_mutations_df)} rows between {dmin} and {dmax}")
        if remainders is not None and not remainders.empty:
            # for parcelles that didn't get geolocalized in the expected batch, we'll try again at each upcoming batch
            logging.info(f"- adding {len(remainders)} remainders")
            matching_mutations_df = pd.concat(
                [matching_mutations_df, remainders], ignore_index=True
            )
        if len(matching_mutations_df) == 0:
            logging.info("> skipping")
            continue
        # TODO: sorting below preserve row order from prior implem. Need to verify if necessary otherwise can be removed
        matching_mutations_df = matching_mutations_df.sort_values(
            by="id_parcelle", key=lambda s: s.str[:3], kind="stable", ignore_index=True
        )  # sort by 2 digits department + 1st digit of the commune code like "380"
        enriched = enrich_parcelles_with_snapshot_coord(
            matching_mutations_df, available_dates[dmin], s3_client, bucket
        )
        remainders = enriched.loc[enriched["longitude"].isna()][
            [c for c in enriched.columns if c not in ["latitude", "longitude"]]
        ]
        geoloced.append(enriched.dropna(subset="longitude"))
        del enriched
        gc.collect()
    logging.info("Done with geoloc, concatenating results...")
    if remainders is not None:
        geoloced.append(remainders)
    del remainders

    # using pd.concat on geoloced directly is too RAM heavy, so workaround
    enriched_df = pd.DataFrame()
    while geoloced:
        logging.info(f"> {len(geoloced)} dfs still to concatenate")
        enriched_df = pd.concat([enriched_df, geoloced[0]], ignore_index=True)
        del geoloced[0]
        gc.collect()
    del geoloced
    return enriched_df


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
