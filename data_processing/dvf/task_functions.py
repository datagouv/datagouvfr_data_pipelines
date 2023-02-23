from airflow.hooks.base import BaseHook
from dag_datagouv_data_pipelines.config import (
    AIRFLOW_DAG_HOME,
    AIRFLOW_DAG_TMP,
    DATAGOUV_SECRET_API_KEY,
    AIRFLOW_ENV,
)
from dag_datagouv_data_pipelines.utils.postgres import execute_sql_file, copy_file
from dag_datagouv_data_pipelines.utils.datagouv import post_resource
from dag_datagouv_data_pipelines.utils.mattermost import send_message
import gc
import glob
from unidecode import unidecode
import numpy as np
import os
import pandas as pd
import requests
import json

DAG_FOLDER = "dag_datagouv_data_pipelines/data_processing/"
DATADIR = f"{AIRFLOW_DAG_TMP}dvf/data"

conn = BaseHook.get_connection("postgres_localhost")


def create_dvf_table():
    execute_sql_file(
        conn.host,
        conn.port,
        conn.schema,
        conn.login,
        conn.password,
        [
            {
                "source_path": f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}dvf/sql/",
                "source_name": "create_dvf_table.sql",
            }
        ],
    )


def create_stats_dvf_table():
    execute_sql_file(
        conn.host,
        conn.port,
        conn.schema,
        conn.login,
        conn.password,
        [
            {
                "source_path": f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}dvf/sql/",
                "source_name": "create_stats_dvf_table.sql",
            }
        ],
    )


def populate_utils(files, table):
    format_files = []
    for file in files:
        format_files.append(
            {"source_path": f"{DATADIR}/", "source_name": file.split("/")[-1]}
        )
    copy_file(
        PG_HOST=conn.host,
        PG_PORT=conn.port,
        PG_DB=conn.schema,
        PG_TABLE=table,
        PG_USER=conn.login,
        PG_PASSWORD=conn.password,
        list_files=format_files,
    )


def populate_dvf_table():
    files = glob.glob(f"{DATADIR}/full_*.csv")
    populate_utils(files, "public.dvf")


def populate_stats_dvf_table():
    populate_utils([f"{DATADIR}/stats_dvf.csv"], "public.stats_dvf")


def get_epci():
    page = requests.get(
        "https://unpkg.com/@etalab/decoupage-administratif/data/epci.json"
    )
    epci = page.json()
    data = [
        {
            "code_epci": e["code"],
            "libelle_geo": unidecode(e["nom"]),
            "liste_membres": [m["code"] for m in e["membres"]],
        }
        for e in epci
    ]
    epci_list = [
        [commune, d["code_epci"], d["libelle_geo"]]
        for d in data
        for commune in d["liste_membres"]
    ]
    pd.DataFrame(
        epci_list, columns=["code_commune", "code_epci", "libelle_geo"]
    ).to_csv(DATADIR + "/epci.csv", sep=",", encoding="utf8", index=False)


def process_dvf_stats(ti):
    years = sorted(
        [
            int(f.replace("full_", "").replace(".csv", ""))
            for f in os.listdir(DATADIR)
            if "full_" in f
        ]
    )
    export = pd.DataFrame(None)
    epci = pd.read_csv(
        DATADIR + "/epci.csv",
        sep=",",
        encoding="utf8",
        dtype=str
    )
    to_keep = [
        "id_mutation",
        "date_mutation",
        "code_departement",
        "code_commune",
        "id_parcelle",
        "nature_mutation",
        "code_type_local",
        "type_local",
        "valeur_fonciere",
        "surface_reelle_bati",
    ]
    for year in years:
        df_ = pd.read_csv(
            DATADIR + f"/full_{year}.csv",
            sep=",",
            encoding="utf8",
            dtype={
                "code_commune": str,
                "code_departement": str,
            },
            usecols=to_keep,
        )
        # les fichiers d'entrée contiennent entre 4 et 8% de doublons purs
        df = df_.drop_duplicates()
        # certaines communes ne sont pas dans des EPCI
        df = pd.merge(df, epci, on="code_commune", how="left")
        df["code_section"] = df["id_parcelle"].str[:10]
        df = df.drop("id_parcelle", axis=1)

        natures_of_interest = [
            "Vente",
            "Vente en l'état futur d'achèvement",
            "Adjudication",
        ]
        types_bien = {
            k: v
            for k, v in df_[["code_type_local", "type_local"]]
            .value_counts()
            .to_dict()
            .keys()
        }
        del df_

        # types_bien = {
        #     1: "Maison",
        #     2: "Appartement",
        #     3: "Dépendance",
        #     4: "Local industriel. commercial ou assimilé",
        #     NaN: terres (cf nature_culture)
        # }

        # filtres sur les ventes et les types de biens à considérer
        # choix : les terres et/ou dépendances ne rendent pas une mutation
        # multitype
        ventes = df.loc[
            (df["nature_mutation"].isin(natures_of_interest)) & (df["code_type_local"].isin([1, 2, 4]))
        ]
        del df
        ventes["month"] = (
            ventes["date_mutation"]
            .apply(lambda x: int(x.split("-")[1]))
        )
        print(len(ventes))

        # drop mutations multitypes pour les prix au m², impossible de
        # classer une mutation qui contient X maisons et Y appartements
        # par exemple
        # à voir pour la suite : quid des mutations avec dépendances, dont
        # le le prix est un prix de lot ? prix_m2 = prix_lot/surface_bien ?
        multitypes = ventes[["id_mutation", "code_type_local"]].value_counts()
        multitypes_ = multitypes.unstack()
        mutations_drop = multitypes_.loc[
            sum([multitypes_[c].isna() for c in multitypes_.columns]) < len(multitypes_.columns) - 1
        ].index
        ventes_nodup = ventes.loc[
            ~(ventes["id_mutation"].isin(mutations_drop)) & ~(ventes["code_type_local"].isna())
        ]
        print(len(ventes_nodup))

        # group par mutation, on va chercher la surface totale de la mutation
        # pour le prix au m²
        surfaces = (
            ventes_nodup.groupby(["id_mutation"])["surface_reelle_bati"]
            .sum()
            .reset_index()
        )
        surfaces.columns = ["id_mutation", "surface_totale_mutation"]
        # avec le inner merge sur surfaces on se garantit aucune ambiguïté sur
        # les type_local
        ventes_nodup = ventes.drop_duplicates(subset="id_mutation")
        ventes_nodup = pd.merge(
            ventes_nodup,
            surfaces,
            on="id_mutation",
            how="inner"
        )
        print(len(ventes_nodup))

        # pour une mutation donnée la valeur foncière est globale,
        # on la divise par la surface totale, sachant qu'on n'a gardé
        # que les mutations monotypes
        ventes_nodup["prix_m2"] = (
            ventes_nodup["valeur_fonciere"] / ventes_nodup["surface_totale_mutation"]
        )
        ventes_nodup["prix_m2"] = ventes_nodup["prix_m2"].replace(
            [np.inf, -np.inf], np.nan
        )

        # pas de prix ou pas de surface
        ventes_nodup = ventes_nodup.dropna(subset=["prix_m2"])

        types_of_interest = [1, 2, 4]
        echelles_of_interest = ["departement", "epci", "commune", "section"]

        for m in range(1, 13):
            dfs_dict = {}
            for echelle in echelles_of_interest:
                # ici on utilise bien le df ventes, qui contient l'eneemble
                # des ventes sans filtres autres que les types de mutations
                # d'intérêt
                nb = (
                    ventes.loc[ventes["code_type_local"].isin(
                        types_of_interest
                    )]
                    .groupby([f"code_{echelle}", "month", "type_local"])[
                        "valeur_fonciere"
                    ]
                    .count()
                )
                nb_ = (
                    nb.loc[nb.index.get_level_values(1) == m]
                    .unstack()
                    .reset_index()
                    .drop("month", axis=1)
                )
                nb_.columns = [
                    "nb_ventes_" + unidecode(c.split(" ")[0].lower())
                    if c != f"code_{echelle}"
                    else c
                    for c in nb_.columns
                ]

                # pour mean et median on utlise le df nodup, dans lequel on a
                # drop_dup sur les mutations
                grouped = ventes_nodup.loc[
                    ventes_nodup["code_type_local"].isin(types_of_interest)
                ].groupby(
                    [f"code_{echelle}", "month", "type_local"]
                )["prix_m2"]
                mean = grouped.mean()
                mean_ = (
                    mean.loc[mean.index.get_level_values(1) == m]
                    .unstack()
                    .reset_index()
                    .drop("month", axis=1)
                )
                mean_.columns = [
                    "moy_prix_m2_" + unidecode(c.split(" ")[0].lower())
                    if c != f"code_{echelle}"
                    else c
                    for c in mean_.columns
                ]

                median = grouped.median()
                median_ = (
                    median.loc[median.index.get_level_values(1) == m]
                    .unstack()
                    .reset_index()
                    .drop("month", axis=1)
                )
                median_.columns = [
                    "med_prix_m2_" + unidecode(c.split(" ")[0].lower())
                    if c != f"code_{echelle}"
                    else c
                    for c in median_.columns
                ]

                merged = pd.merge(nb_, mean_, on=[f"code_{echelle}"])
                merged = pd.merge(merged, median_, on=[f"code_{echelle}"])
                for c in merged.columns:
                    if any([k in c for k in ["moy_", "med_"]]):
                        merged[c] = merged[c].round()

                merged.rename(
                    columns={f"code_{echelle}": "code_geo"},
                    inplace=True
                )
                merged["echelle_geo"] = echelle
                dfs_dict[echelle] = merged

            general = {"code_geo": "all", "echelle_geo": "nation"}
            for t in types_of_interest:
                general[
                    "nb_ventes_" + unidecode(
                        types_bien[t].split(" ")[0].lower()
                    )
                ] = len(
                    ventes.loc[
                        (ventes["code_type_local"] == t) & (ventes["month"] == m)
                    ]
                )
                general[
                    "moy_prix_m2_" + unidecode(types_bien[t].split(" ")[0].lower())
                ] = np.round(
                    ventes_nodup.loc[
                        (ventes_nodup["code_type_local"] == t) & (ventes_nodup["month"] == m)
                    ]["prix_m2"].mean()
                )
                general[
                    "med_prix_m2_" + unidecode(
                        types_bien[t].split(" ")[0].lower()
                    )
                ] = np.round(
                    ventes_nodup.loc[
                        (ventes_nodup["code_type_local"] == t) & (ventes_nodup["month"] == m)
                    ]["prix_m2"].median()
                )

            all_month = pd.concat(
                list(dfs_dict.values()) + [pd.DataFrame([general])]
            )
            all_month["annee_mois"] = f'{year}-{"0"+str(m) if m<10 else m}'
            export = pd.concat([export, all_month])
        del ventes
        del ventes_nodup
        gc.collect()
        print("Done with", year)

    # on ajoute les colonnes libelle_geo et code_parent
    departements = pd.read_csv(
        DATADIR + "/departements.csv", dtype=str, usecols=["DEP", "LIBELLE"]
    )
    departements = departements.rename(
        {"DEP": "code_geo", "LIBELLE": "libelle_geo"}, axis=1
    )
    departements["code_parent"] = "all"
    epci_communes = epci[["code_commune", "code_epci"]]
    epci["code_parent"] = epci["code_commune"].apply(
        lambda code: code[:2] if code[:2] != "97" else code[:3]
    )
    epci = epci.drop("code_commune", axis=1)
    epci = epci.drop_duplicates(subset=["code_epci", "code_parent"]).rename(
        {"code_epci": "code_geo"}, axis=1
    )

    communes = pd.read_csv(
        DATADIR + "/communes.csv",
        dtype=str,
        usecols=["TYPECOM", "COM", "LIBELLE"]
    )
    communes = (
        communes.loc[communes["TYPECOM"].isin(["COM", "ARM"])]
        .rename({"COM": "code_geo", "LIBELLE": "libelle_geo"}, axis=1)
        .drop("TYPECOM", axis=1)
    )
    epci_communes = epci_communes.rename(
        {"code_commune": "code_geo", "code_epci": "code_parent"}, axis=1
    )
    communes = pd.merge(communes, epci_communes, on="code_geo", how="outer")
    libelles_parents = pd.concat([departements, epci, communes])
    libelles_parents["libelle_geo"] = libelles_parents["libelle_geo"] \
        .apply(unidecode)

    export = pd.merge(export, libelles_parents, on="code_geo", how="left")
    export.loc[export["echelle_geo"] == "section", "code_parent"] = export.loc[
        export["echelle_geo"] == "section"
    ]["code_geo"].str.slice(0, 5)
    export.loc[export["echelle_geo"] == "nation", "code_parent"] = "-"
    export.loc[
        (export["echelle_geo"] == "commune") & (export["code_geo"].str.startswith("75")),
        "code_parent",
    ] = "200054781"
    export.loc[
        (export["echelle_geo"] == "commune") & (export["code_geo"].str.startswith("13")),
        "code_parent",
    ] = "200054807"
    export.loc[
        (export["echelle_geo"] == "commune") & (export["code_geo"].str.startswith("69")),
        "code_parent",
    ] = "200046977"
    export["code_parent"] = export["code_parent"].fillna("NA")

    export.to_csv(
        DATADIR + "/stats_dvf.csv",
        sep=",",
        encoding="utf8",
        index=False,
        float_format="%.0f",
    )


def publish_stats_dvf(ti):
    with open(f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}dvf/config/dgv.json") as fp:
        data = json.load(fp)
    post_resource(
        api_key=DATAGOUV_SECRET_API_KEY,
        file_to_upload={
            "dest_path": f"{DATADIR}/",
            "dest_name": data["file"]
        },
        dataset_id=data[AIRFLOW_ENV]["dataset_id"],
        resource_id=data[AIRFLOW_ENV]["resource_id"],
    )
    ti.xcom_push(key="dataset_id", value=data[AIRFLOW_ENV]["dataset_id"])


def notification_mattermost(ti):
    dataset_id = ti.xcom_pull(key="dataset_id", task_ids="publish_stats_dvf")
    if AIRFLOW_ENV == "dev":
        url = "https://demo.data.gouv.fr/fr/datasets/"
    if AIRFLOW_ENV == "prod":
        url = "https://www.data.gouv.fr/fr/datasets/"
    send_message(
        f"Stats DVF générées :"
        f"\n- intégré en base de données"
        f"\n- publié [sur data.gouv.fr]({url}{dataset_id})",
    )
