import json
import logging
import os

import geojson
import pandas as pd
import requests
from shapely.geometry import Point, shape

from datagouvfr_data_pipelines.config import AIRFLOW_DAG_HOME
from datagouvfr_data_pipelines.utils.datagouv import local_client
from datagouvfr_data_pipelines.utils.retry import _simple_connection_retry

with open(
    f"{AIRFLOW_DAG_HOME}/datagouvfr_data_pipelines/data_processing/irve/geo_utils/france_bbox.geojson"
) as f:
    FRANCE_BBOXES = geojson.load(f)

# Create a Polygon
geoms = [region["geometry"] for region in FRANCE_BBOXES.get("features")]
polys = [shape(geom) for geom in geoms]
geo_api = "https://geo.api.gouv.fr/communes"
conn_retry = _simple_connection_retry(reraise=False)


def is_point_in_france(
    coordonnees_xy: list[float],
) -> bool:
    p = Point(*coordonnees_xy)
    return any(p.within(poly) for poly in polys)


def fix_coordinates_order(
    df: pd.DataFrame,
    coordinates_column: str = "coordonneesXY",
) -> pd.DataFrame:
    """
    Cette fonction modifie une dataframe pour placer la longitude avant la latitude
    dans la colonne qui contient les deux au format "[lon, lat]".
    """

    def fix_coordinates(row: pd.Series) -> pd.Series:
        try:
            coordonnees_xy: list[float] = json.loads(row[coordinates_column])
        except Exception as e:
            raise ValueError(f"Error with row: {row.to_list()}") from e
        reversed_coordonnees = coordonnees_xy[::-1]
        row["consolidated_coordinates_reordered"] = False
        if is_point_in_france(reversed_coordonnees):
            # Coordinates are inverted with lat before lon
            row[coordinates_column] = json.dumps(reversed_coordonnees)
            row["consolidated_coordinates_reordered"] = True
        return row

    df = df.apply(fix_coordinates, axis=1)
    return df


def create_lon_lat_cols(
    df: pd.DataFrame,
    coordinates_column: str = "coordonneesXY",
) -> pd.DataFrame:
    """Add longitude and latitude columns to dataframe using coordinates_column"""
    coordinates = df[coordinates_column].apply(json.loads)
    df["consolidated_longitude"] = coordinates.str[0]
    df["consolidated_latitude"] = coordinates.str[1]
    return df


def export_to_geojson(
    df: pd.DataFrame,
    target_filepath: str,
    coordinates_column: str = "coordonneesXY",
) -> None:
    """Export dataframe into Geojson format"""
    json_result_string = df.to_json(
        orient="records", double_precision=12, date_format="iso"
    )
    json_result = json.loads(json_result_string)

    features = []
    for record in json_result:
        coordinates = json.loads(record[coordinates_column])
        longitude, latitude = coordinates
        features.append(
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [longitude, latitude],
                },
                "properties": record,
            }
        )
    geojson = {"type": "FeatureCollection", "features": features}
    with open(target_filepath, "w") as f:
        f.write(json.dumps(geojson, indent=2))


def fix_code_insee(
    df: pd.DataFrame,
    latest_resource_id: str,
    code_insee_col: str = "code_insee_commune",
    address_col: str = "adresse_station",
    lon_col: str = "consolidated_longitude",
    lat_col: str = "consolidated_latitude",
    coordinates_column: str = "coordonneesXY",
) -> pd.DataFrame:
    """Check code INSEE in CSV file and enrich with postcode and city
    Requires address and coordinates columns
    """

    @conn_retry
    def _get_retry(session: requests.Session, url: str) -> requests.models.Response:
        r = session.get(url)
        r.raise_for_status()
        return r

    def enrich_row_address(row: pd.Series, session: requests.Session) -> pd.Series:
        if (
            # having this means the row has been enriched from yesterday's file, we just keep it
            row.get("consolidated_is_code_insee_verified") is True
            and isinstance(row[coordinates_column], str)
            and row[coordinates_column]
        ):
            return row
        row["consolidated_is_lon_lat_correct"] = False
        row["consolidated_is_code_insee_verified"] = False
        row["consolidated_is_code_insee_modified"] = False
        # Try getting commune with code INSEE from latitude and longitude alone
        response = _get_retry(
            session,
            url=(
                f"{geo_api}?lat={row[lat_col]}&lon={row[lon_col]}&fields=code,nom,codesPostaux"
            ),
        )
        commune_results = json.loads(response.content)
        if response.ok and len(commune_results) > 0:
            commune = commune_results[0]
            if row[code_insee_col] == commune["code"]:
                # everything is perfectly filled in
                if len(commune["codesPostaux"]) == 1:
                    row["consolidated_code_postal"] = commune["codesPostaux"][0]
                row["consolidated_commune"] = commune["nom"]
                row["consolidated_is_lon_lat_correct"] = True
                row["consolidated_is_code_insee_verified"] = True
                enrich_row_address.already_good += 1
                return row
            elif row[code_insee_col] in commune["codesPostaux"]:
                # the INSEE code is actually a postcode
                row["consolidated_code_postal"] = row[code_insee_col]
                row["consolidated_is_code_insee_modified"] = True
                row[code_insee_col] = commune["code"]
                row["consolidated_commune"] = commune["nom"]
                row["consolidated_is_lon_lat_correct"] = True
                row["consolidated_is_code_insee_verified"] = True
                enrich_row_address.code_fixed += 1
                return row
            else:
                # Lat lon match a commune which does not match the INSEE code
                enrich_row_address.code_coords_mismatch += 1
        else:
            # Lat lon do not match any commune
            enrich_row_address.no_match_coords += 1

        if (
            pd.notna(row[code_insee_col])
            and row[code_insee_col] != ""
            and str(row[code_insee_col]) in row[address_col]
        ):
            # INSEE code field actually contains a postcode (which is also in the address column)
            response = _get_retry(
                session,
                url=f"{geo_api}?codePostal={row[code_insee_col]}&fields=code,nom",
            )
            commune_results = json.loads(response.content)
            if response.ok and len(commune_results) > 0:
                commune = commune_results[0]
                row["consolidated_code_postal"] = row[code_insee_col]
                row["consolidated_commune"] = commune["nom"]
                row[code_insee_col] = commune["code"]
                row["consolidated_is_code_insee_modified"] = True
                row["consolidated_is_code_insee_verified"] = True
                enrich_row_address.code_insee_is_postcode_in_address += 1
                return row

        if isinstance(row[code_insee_col], str) and row[code_insee_col]:
            # Check if postcode is in address
            response = _get_retry(
                session,
                url=f"{geo_api}?code={row[code_insee_col]}&fields=codesPostaux,nom"
            )
            commune_results = json.loads(response.content)
            if response.ok and len(commune_results) > 0:
                for commune in commune_results:
                    for postcode in commune["codesPostaux"]:
                        if postcode in row[address_col]:
                            row["consolidated_code_postal"] = postcode
                            row["consolidated_commune"] = commune["nom"]
                            row["consolidated_is_code_insee_verified"] = True
                            enrich_row_address.code_insee_has_postcode_in_address += 1
                            return row

        # None of the above checks succeeded. Code INSEE validity cannot be checked.
        # Geo data is not enriched using code INSEE due to risk of introducing fake data
        row["consolidated_code_postal"] = ""
        row["consolidated_commune"] = ""
        enrich_row_address.nothing_matches += 1
        # logging.warning(
        #     f"Could not retrieve infos for this combination: "
        #     f"coords: {row[coordinates_column]}, code INSEE: {row[code_insee_col]}"
        # )
        return row

    cols = list(df.columns)
    total_rows = len(df)
    session = requests.Session()
    enrich_row_address.already_good = 0
    enrich_row_address.code_fixed = 0
    enrich_row_address.code_coords_mismatch = 0
    enrich_row_address.no_match_coords = 0
    enrich_row_address.code_insee_is_postcode_in_address = 0
    enrich_row_address.code_insee_has_postcode_in_address = 0
    enrich_row_address.nothing_matches = 0

    logging.info("Getting data from yesterday's file")
    # to prevent calling the address API for every row everyday, we get yesterday's data
    # and fill in the rows that have a known address
    process_infos_cols = [
        "consolidated_is_lon_lat_correct",
        "consolidated_is_code_insee_verified",
        "consolidated_is_code_insee_modified",
        "consolidated_code_postal",
        "consolidated_commune",
    ]
    # quick check that yesterday's file contains the right columns (in case of addition)
    sample = pd.read_csv(
        f"{local_client.base_url}/api/1/datasets/r/{latest_resource_id}",
        dtype=str,
        nrows=5,
    )
    if all(c in sample.columns for c in process_infos_cols):
        # we read the whole file
        yesterdays_data = pd.read_csv(
            f"{local_client.base_url}/api/1/datasets/r/{latest_resource_id}",
            dtype=(
                {c: bool if "_is_" in c else str for c in process_infos_cols}
                | {coordinates_column: str}
            ),
            # only loading the columns with the needed info to merge with today
            usecols=[coordinates_column] + process_infos_cols,
        )
        # keeping rows where the coords are present and INSEE code is verified
        # and only one occurrence of each coords pair
        coords = yesterdays_data.loc[
            (yesterdays_data[coordinates_column].notna())
            & (yesterdays_data["consolidated_is_code_insee_verified"]),
            [coordinates_column] + process_infos_cols,
        ].drop_duplicates(subset=coordinates_column)
        logging.info("Merging existing data")
        df = pd.merge(
            coords,
            df[
                [c for c in df.columns if c not in coords.columns] + [coordinates_column]
            ],
            on=coordinates_column,
            how="right",
        )
        logging.info(
            f"{len(df.loc[df['consolidated_is_code_insee_verified'].notna()])}/"
            f"{len(df)} lines filled from yesterday's coords column"
        )
    else:
        logging.warning(
            f"Columns are missing in yesterday's file: {[c for c in process_infos_cols if c not in sample.columns]}"
        )
    del sample
    assert not [c for c in cols if c not in df.columns]
    assert len(df) == total_rows
    df = df.progress_apply(
        lambda row: enrich_row_address(row, session),
        axis=1,
    )

    logging.info(
        f"Coords OK. INSEE codes already correct, simply enriched: {enrich_row_address.already_good}/{total_rows}"
    )
    logging.info(
        "Coords OK. INSEE code field contained postcode. Fixed and enriched: "
        f"{enrich_row_address.code_fixed}/{total_rows}"
    )
    logging.info(
        "Coords not matching code INSEE field as code INSEE or postcode: "
        f"{enrich_row_address.code_coords_mismatch}/{total_rows}"
    )
    logging.info(
        f"Coords not matching any commune: {enrich_row_address.no_match_coords}/{total_rows}"
    )
    logging.info(
        "Code INSEE is postcode in address. Fixed and enriched: "
        f"{enrich_row_address.code_insee_is_postcode_in_address}/{total_rows}"
    )
    logging.info(
        "Code INSEE has postcode in address. "
        f"Enriched: {enrich_row_address.code_insee_has_postcode_in_address}/{total_rows}"
    )
    logging.info(
        "No indication of postcode/code INSEE in address or coordinates matching code INSEE field. "
        f"No enriching performed: {enrich_row_address.nothing_matches}/{total_rows}"
    )
    return df


def improve_geo_data_quality(
    file_cols_mapping: dict[str, dict[str, str]],
    latest_resource_id: str,
) -> None:
    for filepath, cols_dict in file_cols_mapping.items():
        df = pd.read_csv(filepath, dtype=str, na_filter=False, keep_default_na=False)
        # we load and dump with the same name, so in case of crash we need to remove the newly created columns
        df = df[[c for c in df.columns if not c.startswith("consolidated_")]]
        schema_cols = list(df.columns)

        df = fix_coordinates_order(df, coordinates_column=cols_dict["xy_coords"])
        logging.info(
            f"Done fixing coordinates: ({df['consolidated_coordinates_reordered'].sum()}/{len(df)})"
        )

        df = create_lon_lat_cols(df, coordinates_column=cols_dict["xy_coords"])
        logging.info("Done creating long lat")
        df = fix_code_insee(
            df,
            latest_resource_id=latest_resource_id,
            code_insee_col=cols_dict["code_insee"],
            address_col=cols_dict["adress"],
            lon_col=cols_dict["longitude"],
            lat_col=cols_dict["latitude"],
        )
        logging.info("Done fixing code INSEE")
        new_cols = [
            "consolidated_longitude",
            "consolidated_latitude",
            "consolidated_code_postal",
            "consolidated_commune",
            "consolidated_is_lon_lat_correct",
            "consolidated_is_code_insee_verified",
            "consolidated_is_code_insee_modified",
        ]
        df = df[schema_cols + new_cols]
        df.to_csv(filepath, index=False)
        export_to_geojson(
            df,
            os.path.splitext(filepath)[0] + ".geojson",
            coordinates_column=cols_dict["xy_coords"],
        )
