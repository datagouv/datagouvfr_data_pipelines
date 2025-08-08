import pandas as pd

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_HOME,
)
from datagouvfr_data_pipelines.data_processing.carburants.scripts.utils import (
    LIST_FUELS,
    create_todays_df,
)


AUGMENTED_LIST_FUELS = LIST_FUELS + [
    "essence",
    "au_moins_un_produit",
    "deux_produits",
]


def rupture_essence(row):
    if row["SP95"] == "R" and row["E10"] != "S" and row["SP98"] != "S":
        return "R"
    if row["SP95"] != "S" and row["E10"] == "R" and row["SP98"] != "S":
        return "R"
    if row["SP95"] != "S" and row["E10"] != "S" and row["SP98"] == "R":
        return "R"
    if row["SP95"] != "ND":
        return "S"
    if row["E10"] != "ND":
        return "S"
    if row["SP98"] != "ND":
        return "S"
    return "ND"


def rupture_au_moins_un_produit(row):
    if row["essence"] == "R" or row["Gazole"] == "R":
        return "R"
    if row["essence"] == "ND" and row["Gazole"] == "ND":
        return "ND"
    return "S"


def rupture_deux_produits(row):
    if row["essence"] == "R" and row["Gazole"] == "R":
        return "R"
    if row["essence"] == "ND" and row["Gazole"] == "ND":
        return "ND"
    return "S"


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


def get_stats_df(df):
    arr = []
    for fuel in AUGMENTED_LIST_FUELS:
        for d in df["dep"].unique():
            mydict = {
                "dep": d,
                "carburant": fuel,
            }
            try:
                mydict["pourcentage_rupture"] = (
                    df.loc[
                        (df["dep"] == d)
                        & (df["carburant"] == fuel)
                        & (df["etat"] == "R"),
                        "nb",
                    ].iloc[0]
                    / (
                        df.loc[
                            (df["dep"] == d)
                            & (df["carburant"] == fuel)
                            & (df["etat"] == "R"),
                            "nb",
                        ].iloc[0]
                        + df.loc[
                            (df["dep"] == d)
                            & (df["carburant"] == fuel)
                            & (df["etat"] == "S"),
                            "nb",
                        ].iloc[0]
                    )
                    * 100
                )
            except Exception:
                mydict["pourcentage_rupture"] = 0
            mydict["nombre_rupture"] = df.loc[
                (df["dep"] == d) & (df["carburant"] == fuel) & (df["etat"] == "R"), "nb"
            ].iloc[0]
            mydict["nombre_stations"] = (
                df.loc[
                    (df["dep"] == d) & (df["carburant"] == fuel) & (df["etat"] == "R"),
                    "nb",
                ].iloc[0]
                + df.loc[
                    (df["dep"] == d) & (df["carburant"] == fuel) & (df["etat"] == "S"),
                    "nb",
                ].iloc[0]
            )
            arr.append(mydict)
    return pd.DataFrame(arr)


def generate_kpis_rupture(path):
    print("Building today's table")
    mef = create_todays_df(path)

    print("Detecting shortages")
    mef["essence"] = mef.apply(lambda row: rupture_essence(row), axis=1)
    mef["au_moins_un_produit"] = mef.apply(
        lambda row: rupture_au_moins_un_produit(row), axis=1
    )
    mef["deux_produits"] = mef.apply(lambda row: rupture_deux_produits(row), axis=1)

    print("Aggreggation")
    mef_dep = []
    for fuel in AUGMENTED_LIST_FUELS:
        inter = (
            mef[["dep", fuel, "id"]]
            .groupby(["dep", fuel], as_index=False)
            .count()
            .rename(columns={"id": "nb", fuel: "etat"})
        )
        inter["carburant"] = fuel
        mef_dep.append(inter)
    mef_dep = pd.concat(mef_dep, ignore_index=True)

    print("Adding rows")
    new_rows = []
    for fuel in AUGMENTED_LIST_FUELS:
        for d in mef_dep["dep"].unique():
            if (
                mef_dep[
                    (mef_dep["dep"] == d)
                    & (mef_dep["carburant"] == fuel)
                    & (mef_dep["etat"] == "R")
                ].shape[0]
                == 0
            ):
                new_rows.append({"dep": d, "etat": "R", "nb": 0, "carburant": fuel})
            if (
                mef_dep[
                    (mef_dep["dep"] == d)
                    & (mef_dep["carburant"] == fuel)
                    & (mef_dep["etat"] == "S")
                ].shape[0]
                == 0
            ):
                new_rows.append({"dep": d, "etat": "S", "nb": 0, "carburant": fuel})
    mef_dep = pd.concat([mef_dep, pd.DataFrame(new_rows)], ignore_index=True)

    print("Getting stats")
    stats_mef_dep = get_stats_df(mef_dep)
    deps = pd.read_csv(
        f"{AIRFLOW_DAG_HOME}datagouvfr_data_pipelines/data_processing/carburants/utils/deps_regs.csv",
        dtype=str,
    )
    stats_mef_reg = pd.merge(stats_mef_dep, deps, on="dep", how="left")
    stats_mef_reg = stats_mef_reg.groupby(
        ["reg", "libelle_region", "carburant"], as_index=False
    ).sum()
    stats_mef_reg["pourcentage_rupture"] = round(
        stats_mef_reg["nombre_rupture"] / stats_mef_reg["nombre_stations"] * 100, 2
    )
    stats_mef_nat = stats_mef_reg.groupby(["carburant"], as_index=False).sum()
    stats_mef_nat["pourcentage_rupture"] = round(
        stats_mef_nat["nombre_rupture"] / stats_mef_nat["nombre_stations"] * 100, 2
    )
    print("Exporting as excel")
    writer = pd.ExcelWriter(f"{path}synthese_ruptures_latest.xlsx", engine="xlsxwriter")
    stats_mef_dep.to_excel(writer, sheet_name="départements", index=False)
    stats_mef_reg.to_excel(writer, sheet_name="régions", index=False)
    stats_mef_nat.to_excel(writer, sheet_name="national", index=False)
    writer.close()

    print("Final process")
    df = stats_mef_dep
    df.groupby(["dep", "carburant"], as_index=False).count()

    arr = []
    for d in df.dep.unique():
        mydict = {"dep": d}
        for fuel in AUGMENTED_LIST_FUELS:
            print(fuel, d)
            mydict[fuel] = round(
                df[(df["dep"] == d) & (df["carburant"] == fuel)][
                    "pourcentage_rupture"
                ].iloc[0],
                2,
            )
            mydict[fuel + "_nbrup"] = df[(df["dep"] == d) & (df["carburant"] == fuel)][
                "nombre_rupture"
            ].iloc[0]
            mydict[fuel + "_nbstation"] = df[
                (df["dep"] == d) & (df["carburant"] == fuel)
            ]["nombre_stations"].iloc[0]
        arr.append(mydict)

    res = pd.DataFrame(arr)

    res.to_json(f"{path}latest_france_ruptures.json", orient="records")
