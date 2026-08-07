import logging
import gc
import os

from time import sleep
import pandas as pd
import pyarrow.fs as pafs
import pyarrow.parquet as pq

from datagouvfr_data_pipelines.utils.s3 import S3Client


def build_code_commune(row: pd.Series) -> str:
    """Returns the 5 caracters commune code"""
    code_dep = row["Code departement"]
    code_com = row["Code commune"]
    if code_dep.startswith("97"):
        code_com = code_com.rjust(2, "0")
    else:
        code_com = code_com.rjust(3, "0")
        code_dep = code_dep.rjust(2, "0")
    return code_dep + code_com


def build_parcelle_id(row: pd.Series) -> str:
    """Build 14 caracters parcelle ID
    shared both by DVF df and parcelles coordinates parquet file"""
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
    # getting and merging parcelle's geographical columns from parquet files made by Thomas
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
    available_dates: dict[str, str],
    s3_client: S3Client,
    bucket: str,
):
    # the whole process is RAM heavy, so trying to be efficient to fit within Airflow's capabilities
    year = file.split(".")[0].split("-")[1]  #  # "valeursfoncieres-2023.txt" => "2023"
    if f"full-{year}.csv.gz" in os.listdir(tmp_folder):
        logging.info(f"Skipping {file} - already processed")  # In case of retry
        return

    logging.info(f"Processing {file}")
    source = pd.read_csv(tmp_folder + file, dtype=str, sep="|")
    logging.info("Building output...")
    # TODO: group cols operations type : copy of cols, simple cols vectorized ops, more complex ops
    # TODO: reorder cols at the end to enforce correct order + verify col type
    # TODO: verify if variable names change in and outside function is a memory issue
    output = pd.DataFrame()
    # Columns are created in their order left-to-right :
    # ("id_mutation" col will be added first later)
    output["date_mutation"] = (
        source["Date mutation"].str.slice(
            6,
        )
        + "-"
        + source["Date mutation"].str.slice(3, 5)
        + "-"
        + source["Date mutation"].str.slice(0, 2)
    )  # Make str date ISO : DD/MM/YYYY => YYYY-MM-DD
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
    )  # todo : vectorized ? "RUE" + "DE LA REPUBLIQUE" => "RUE DE LA REPUBLIQUE"
    output["adresse_code_voie"] = source["Code voie"].str.rjust(
        4, "0"
    )  # "143" => "0143"
    output["code_postal"] = source["Code postal"].str.rjust(5, "0")
    output["code_commune"] = source.apply(
        build_code_commune, axis=1
    )  # not as sophisticated as the original code
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
    )  # Note:  97. match the only overseas departements that are part of DVF : Guadeloupe, Martinique, Guyane, La Réunion
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
    output.insert(0, "id_mutation", f"{year}-" + mask.cumsum().astype(str))  # First col
    del mask
    output.reset_index(drop=True, inplace=True)
    expected_len = len(output)

    final = enrich_parcelles_with_coord(
        output, year, available_dates, s3_client, bucket
    )  # Add lat, long coordinates tied to parcelle ID
    del output
    logging.info("Sorting by mutation id...")
    final["_sort_key"] = final["id_mutation"].str[len(year) + 1 :].astype("int32")
    final.sort_values("_sort_key", inplace=True)
    final.drop(columns="_sort_key", inplace=True)
    assert len(final) == expected_len
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
