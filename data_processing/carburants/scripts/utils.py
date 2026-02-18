import json

import pandas as pd

LIST_FUELS = ["SP95", "E10", "SP98", "Gazole", "GPLc", "E85"]


def parseCP(val):
    if val[:2] == "97":
        return val[:3]
    if val[:3] == "200" or val[:3] == "201":
        return "2A"
    if val[:3] == "202" or val[:3] == "206":
        return "2B"
    if val[:2] == "20" and val[:3] != "200" and val[:3] != "202":
        print(val)
    else:
        return val[:2]


def create_todays_df(path):
    with open(f"{path}quotidien.geojson") as fp:
        data = json.load(fp)

    obj = {
        "type": "FeatureCollection",
        "features": [],
    }
    for d in data["features"]:
        feature = {
            "type": "Feature",
            "properties": {
                "id": d["properties"]["id"],
                "adr": (
                    d["properties"]["adresse"]
                    .encode("Latin-1", "ignore")
                    .decode("utf-8", "ignore")
                    .lower()
                    if isinstance(d["properties"]["adresse"], str)
                    else None
                ),
                "cpl_adr": (
                    d["properties"]["cp"]
                    .encode("Latin-1", "ignore")
                    .decode("utf-8", "ignore")
                    .lower()
                    + " "
                    + d["properties"]["ville"]
                    .encode("Latin-1", "ignore")
                    .decode("utf-8", "ignore")
                    .lower()
                ),
                "dep": parseCP(d["properties"]["cp"]),
            },
        }
        for r in d["properties"]["ruptures"]:
            if r["debut"] > "2022-09-15":
                feature["properties"][r["nom"]] = "R"
            else:
                feature["properties"][r["nom"]] = "N"

            feature["properties"][r["nom"] + "_s"] = r["debut"]
            feature["properties"][r["nom"] + "_m"] = None

        for p in d["properties"]["prix"]:
            feature["properties"][p["nom"]] = p["valeur"]
            feature["properties"][p["nom"] + "_s"] = None
            feature["properties"][p["nom"] + "_m"] = p["maj"]

        for fuel in LIST_FUELS:
            if fuel not in feature["properties"]:
                feature["properties"][fuel] = "N"
                feature["properties"][fuel + "_s"] = None
                feature["properties"][fuel + "_m"] = None

        feature["geometry"] = d["geometry"]
        obj["features"].append(feature)

    # improvement: save as csv and convert to a task, so that we don't do it twice
    return pd.DataFrame([d["properties"] for d in obj["features"]])
