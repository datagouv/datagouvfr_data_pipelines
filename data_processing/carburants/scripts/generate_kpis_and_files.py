import json
import pandas as pd
import numpy as np
from datetime import datetime

from datagouvfr_data_pipelines.data_processing.carburants.scripts.utils import (
    LIST_FUELS,
    create_todays_df,
)


def generate_kpis(path):

    df = create_todays_df(path)
    df = df.where(pd.notnull(df), None)

    with open(f"{path}latest.geojson") as fp:
        data = json.load(fp)

    final = {
        "type": "FeatureCollection",
        "features": [],
    }
    for d in data["features"]:
        for p in d["properties"]["prix"]:
            df.loc[df["id"] == d["properties"]["id"], p["nom"] + "_m"] = p["maj"]
            df.loc[df["id"] == d["properties"]["id"], p["nom"]] = p["valeur"]
        feature = {"type": "Feature"}
        dfinter = df[df["id"] == d["properties"]["id"]]
        if dfinter.shape[0] > 0:
            feature["properties"] = dfinter.to_dict(orient="records")[0]
        else:
            feature["properties"] = {}
        feature["geometry"] = d["geometry"]
        final["features"].append(feature)

    tab = {
        k: [] if k.endswith("v") else 0
        for k in [
            fuel + suffix for fuel in LIST_FUELS for suffix in ["", "r", "v"]
        ]
    }

    for f in final["features"]:
        for fuel in LIST_FUELS:
            if fuel in f["properties"]:
                if f["properties"][fuel] not in ["R", "N"]:
                    tab[fuel] = tab[fuel] + 1
                    tab[fuel + "v"].append(float(f["properties"][fuel]))
                elif f["properties"][fuel] == "R":
                    tab[fuel + "r"] = tab[fuel + "r"] + 1

    final["properties"] = {}
    for fuel in LIST_FUELS:
        final["properties"][fuel] = [
            np.min(tab[fuel + "v"]),
            round(np.quantile(tab[fuel + "v"], .333333), 2),
            round(np.quantile(tab[fuel + "v"], .66666), 2),
            np.max(tab[fuel + "v"])
        ]
        final["properties"][fuel + "_mean"] = np.mean(tab[fuel + "v"])
        final["properties"][fuel + "_median"] = np.median(tab[fuel + "v"])
        final["properties"][fuel + "_rupture"] = round(
            (tab[fuel + "r"] / (tab[fuel + "r"] + tab[fuel]) * 100),
            2
        )

    def getColor(val, fuel):
        if fuel in LIST_FUELS:
            arr = final["properties"][fuel]
        if val < arr[1]:
            return "1"
        if val < arr[2]:
            return "2"
        if val >= arr[2]:
            return "3"

    for d in final["features"]:
        for fuel in LIST_FUELS:
            if fuel in d["properties"]:
                if d["properties"][fuel] == "R":
                    d["properties"][fuel + "_color"] = "0"
                elif d["properties"][fuel] == "N":
                    d["properties"][fuel + "_color"] = "-1"
                else:
                    d["properties"][fuel + "_color"] = getColor(float(d["properties"][fuel]), fuel)

    final["properties"]["maj"] = datetime.now().isoformat(' ')

    with open(f"{path}latest_france.geojson", "w") as fp:
        json.dump(final, fp)

    with open(f"{path}daily_prices.json", 'r') as fp:
        data = json.load(fp)

    for d in data:
        if d["date"] == final["properties"]["maj"][:10]:
            del d

    mydict = {"date": final["properties"]["maj"][:10]}
    for fuel in LIST_FUELS:
        for method in ["mean", "median"]:
            mydict[f"{fuel}_{method}"] = final["properties"][f"{fuel}_{method}"]
    data.append(mydict)

    with open(f"{path}daily_prices.json", 'w') as fp:
        json.dump(data, fp)
