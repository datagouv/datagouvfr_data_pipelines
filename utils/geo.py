from typing import Dict, List
import geojson
import json
import os
import pandas as pd
import requests
from unidecode import unidecode
from shapely.geometry import Point, shape
from shapely.geometry.polygon import Polygon
from datagouvfr_data_pipelines.config import AIRFLOW_DAG_HOME

with open(
    f"{AIRFLOW_DAG_HOME}/datagouvfr_data_pipelines/schema/utils/france_bbox.geojson"
) as f:
    FRANCE_BBOXES = geojson.load(f)


def is_point_in_polygon(x: float, y: float, polygon: List[List[float]]) -> bool:
    point = Point(x, y)
    polygon_shape = Polygon(polygon)
    return polygon_shape.contains(point)


def is_point_in_france(coordonnees_xy: List[float]) -> bool:
    p = Point(*coordonnees_xy)

    # Create a Polygon
    geoms = [region["geometry"] for region in FRANCE_BBOXES.get("features")]
    polys = [shape(geom) for geom in geoms]
    return any([p.within(poly) for poly in polys])


def fix_coordinates_order(
    df: pd.DataFrame, coordinates_column: str = "coordonneesXY"
) -> pd.DataFrame:
    """
    Cette fonction modifie une dataframe pour placer la longitude avant la latitude
    dans la colonne qui contient les deux au format "[lon, lat]".
    """

    def fix_coordinates(row: pd.Series) -> pd.Series:
        coordonnees_xy = json.loads(row[coordinates_column])
        reversed_coordonnees = list(reversed(coordonnees_xy))
        row["consolidated_coordinates_reordered"] = False
        if is_point_in_france(reversed_coordonnees):
            # Coordinates are inverted with lat before lon
            row[coordinates_column] = json.dumps(reversed_coordonnees)
            row["consolidated_coordinates_reordered"] = True
            fix_coordinates.rows_modified = fix_coordinates.rows_modified + 1
        return row

    fix_coordinates.rows_modified = 0
    df = df.apply(fix_coordinates, axis=1)
    print(f"Coordinates reordered: {fix_coordinates.rows_modified}/{len(df)}")
    return df


def create_lon_lat_cols(
    df: pd.DataFrame, coordinates_column: str = "coordonneesXY"
) -> pd.DataFrame:
    """Add longitude and latitude columns to dataframe using coordinates_column"""
    coordinates = df[coordinates_column].apply(json.loads)
    df["consolidated_longitude"] = coordinates.str[0]
    df["consolidated_latitude"] = coordinates.str[1]
    return df


def export_to_geojson(
    df: pd.DataFrame, target_filepath: str, coordinates_column: str = "coordonneesXY"
) -> None:
    """Export dataframe into Geojson format"""
    json_result_string = df.to_json(
        orient="records", double_precision=12, date_format="iso"
    )
    json_result = json.loads(json_result_string)

    geojson = {"type": "FeatureCollection", "features": []}
    for record in json_result:
        coordinates = json.loads(record[coordinates_column])
        longitude, latitude = coordinates
        geojson["features"].append(
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [longitude, latitude],
                },
                "properties": record,
            }
        )
    with open(target_filepath, "w") as f:
        f.write(json.dumps(geojson, indent=2))


def fix_code_insee( # noqa
    df: pd.DataFrame,
    code_insee_col: str = "code_insee_commune",
    address_col: str = "adresse_station",
    lon_col: str = "consolidated_longitude",
    lat_col: str = "consolidated_latitude",
) -> pd.DataFrame:
    """Check code INSEE in CSV file and enrich with postcode and city
    Requires address and coordinates columns
    """

    def enrich_row_address(row: pd.Series) -> pd.Series:
        row["consolidated_is_lon_lat_correct"] = False
        row["consolidated_is_code_insee_verified"] = False
        row["consolidated_code_insee_modified"] = False
        # Try getting commune with code INSEE from latitude and longitude alone
        response = requests.get(
            url=(
                f"https://geo.api.gouv.fr/communes?lat={row[lat_col]}"
                f"&lon={row[lon_col]}&fields=code,nom,codesPostaux"
            )
        )
        commune_results = json.loads(response.content)
        if (response.status_code == requests.codes.ok) and (len(commune_results) > 0):
            commune = commune_results[0]
            if row[code_insee_col] == commune["code"]:
                if len(commune["codesPostaux"]) == 1:
                    row["consolidated_code_postal"] = commune["codesPostaux"][0]
                row["consolidated_commune"] = commune["nom"]
                row["consolidated_is_lon_lat_correct"] = True
                row["consolidated_is_code_insee_verified"] = True
                enrich_row_address.already_good += 1
                return row
            elif row[code_insee_col] in commune["codesPostaux"]:
                row["consolidated_code_postal"] = row[code_insee_col]
                row["consolidated_code_insee_modified"] = True
                row[code_insee_col] = commune["code"]
                row["consolidated_commune"] = commune["nom"]
                row["consolidated_is_lon_lat_correct"] = True
                row["consolidated_is_code_insee_verified"] = True
                enrich_row_address.code_fixed += 1
                return row
            else:
                # Lat lon match a commune which does not match code INSEE
                enrich_row_address.code_coords_mismatch += 1
        else:
            # Lat lon do not match any commune
            enrich_row_address.no_match_coords += 1

        if str(row[code_insee_col]) in row[address_col]:
            # Code INSEE field actually contains a postcode
            response = requests.get(
                url=f"https://geo.api.gouv.fr/communes?codePostal={row[code_insee_col]}&fields=code,nom"
            )
            commune_results = json.loads(response.content)
            if (response.status_code == requests.codes.ok) and (
                len(commune_results) > 0
            ):
                commune = commune_results[0]
                row["consolidated_code_postal"] = row[code_insee_col]
                row["consolidated_commune"] = commune["nom"]
                row[code_insee_col] = commune["code"]
                row["consolidated_code_insee_modified"] = True
                row["consolidated_is_code_insee_verified"] = True
                enrich_row_address.code_insee_is_postcode_in_address += 1
                return row

        # Check if postcode is in address
        response = requests.get(
            url=f"https://geo.api.gouv.fr/communes?code={row[code_insee_col]}&fields=codesPostaux,nom"
        )
        commune_results = json.loads(response.content)
        if (response.status_code == requests.codes.ok) and (len(commune_results) > 0):
            commune = commune_results[0]
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
        return row

    enrich_row_address.already_good = 0
    enrich_row_address.code_fixed = 0
    enrich_row_address.code_coords_mismatch = 0
    enrich_row_address.no_match_coords = 0
    enrich_row_address.code_insee_is_postcode_in_address = 0
    enrich_row_address.code_insee_has_postcode_in_address = 0
    enrich_row_address.nothing_matches = 0

    df = df.apply(enrich_row_address, axis=1)

    total_rows = len(df)
    print(
        "Coords OK. INSEE codes already correct, simply enriched: "
        f"{enrich_row_address.already_good}/{total_rows}"
    )
    print(
        "Coords OK. INSEE code field contained postcode. Fixed and enriched: "
        f"{enrich_row_address.code_fixed}/{total_rows}"
    )
    print(
        "Coords not matching code INSEE field as code INSEE or postcode: "
        f"{enrich_row_address.code_coords_mismatch}/{total_rows}"
    )
    print(
        f"Coords not matching any commune: {enrich_row_address.no_match_coords}/{total_rows}"
    )
    print(
        "Code INSEE is postcode in address. Fixed and enriched: "
        f"{enrich_row_address.code_insee_is_postcode_in_address}/{total_rows}"
    )
    print(
        "Code INSEE has postcode in address. "
        f"Enriched: {enrich_row_address.code_insee_has_postcode_in_address}/{total_rows}"
    )
    print(
        "No indication of postcode/code INSEE in address or coordinates matching code INSEE field. "
        f"No enriching performed: {enrich_row_address.nothing_matches}/{total_rows}"
    )
    return df


def improve_geo_data_quality(file_cols_mapping: Dict[str, Dict[str, str]]) -> None:
    for filepath, cols_dict in file_cols_mapping.items():
        df = pd.read_csv(filepath, dtype="str", na_filter=False, keep_default_na=False)
        schema_cols = list(df.columns)
        df = fix_coordinates_order(df, coordinates_column=cols_dict["xy_coords"])
        print("Done fixing coordinates")
        df = create_lon_lat_cols(df, coordinates_column=cols_dict["xy_coords"])
        print("Done creating long lat")
        df = fix_code_insee(
            df,
            code_insee_col=cols_dict["code_insee"],
            address_col=cols_dict["adress"],
            lon_col=cols_dict["longitude"],
            lat_col=cols_dict["latitude"],
        )
        print("Done fixing code INSEE")
        new_cols = [
            "consolidated_longitude",
            "consolidated_latitude",
            "consolidated_code_postal",
            "consolidated_commune",
            "consolidated_is_lon_lat_correct",
            "consolidated_is_code_insee_verified",
        ]
        df = df[schema_cols + new_cols]
        df.to_csv(filepath, index=False)
        export_to_geojson(
            df,
            os.path.splitext(filepath)[0] + ".json",
            coordinates_column=cols_dict["xy_coords"],
        )
