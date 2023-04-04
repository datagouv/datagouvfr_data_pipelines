from airflow.hooks.base import BaseHook
from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_HOME,
    AIRFLOW_DAG_TMP,
    DATAGOUV_SECRET_API_KEY,
    AIRFLOW_ENV,
    MINIO_URL,
    MINIO_BUCKET_DATA_PIPELINE_OPEN,
    SECRET_MINIO_DATA_PIPELINE_USER,
    SECRET_MINIO_DATA_PIPELINE_PASSWORD,
)
from dag_datagouv_data_pipelines.utils.postgres import (
    execute_sql_file,
    copy_file
)
<<<<<<< HEAD
from datagouvfr_data_pipelines.utils.postgres import execute_sql_file, copy_file
from datagouvfr_data_pipelines.utils.datagouv import post_resource
from datagouvfr_data_pipelines.utils.mattermost import send_message
=======
from dag_datagouv_data_pipelines.utils.datagouv import post_resource
from dag_datagouv_data_pipelines.utils.mattermost import send_message
from dag_datagouv_data_pipelines.utils.minio import send_files
>>>>>>> 5b979ba (feat: return all occurrences of geo and time, upload to minio, reshape DAG)
import gc
import glob
from unidecode import unidecode
import numpy as np
import os
import pandas as pd
import requests
import json

DAG_FOLDER = "datagouvfr_data_pipelines/data_processing/"
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


def create_distribution_table():
    execute_sql_file(
        conn.host,
        conn.port,
        conn.schema,
        conn.login,
        conn.password,
        [
            {
                "source_path": f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}dvf/sql/",
                "source_name": "create_distribution_table.sql",
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


def populate_distribution_table():
    populate_utils([f"{DATADIR}/distribution_prix.csv"], "public.distribution_prix")


def populate_dvf_table():
    files = glob.glob(f"{DATADIR}/full_*.csv")
    populate_utils(files, "public.dvf")


def populate_stats_dvf_table():
    populate_utils([f"{DATADIR}/stats_dvf_api.csv"], "public.stats_dvf")


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
    sections_from_dvf = set()
    communes_from_dvf = set()
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
        df = pd.merge(
            df,
            epci[['code_commune', 'code_epci']],
            on="code_commune",
            how="left"
        )
        df["code_section"] = df["id_parcelle"].str[:10]
        sections_from_dvf = sections_from_dvf | set(df['code_section'].unique())
        communes_from_dvf = communes_from_dvf | set(df['code_commune'].unique())
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
            (df["nature_mutation"].isin(natures_of_interest)) &
            (df["code_type_local"].isin([1, 2, 4]))
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
            sum(
                [multitypes_[c].isna() for c in multitypes_.columns]
            ) < len(multitypes_.columns) - 1
        ].index
        ventes_nodup = ventes.loc[
            ~(ventes["id_mutation"].isin(mutations_drop)) &
            ~(ventes["code_type_local"].isna())
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
            ventes_nodup["valeur_fonciere"] /
            ventes_nodup["surface_totale_mutation"]
        )
        ventes_nodup["prix_m2"] = ventes_nodup["prix_m2"].replace(
            [np.inf, -np.inf], np.nan
        )

        # pas de prix ou pas de surface
        ventes_nodup = ventes_nodup.dropna(subset=["prix_m2"])

        # garde fou pour les valeurs aberrantes
        ventes_nodup = ventes_nodup.loc[ventes_nodup['prix_m2'] < 100000]

        types_of_interest = [1, 2, 4]
        echelles_of_interest = ["departement", "epci", "commune", "section"]

        for m in range(1, 13):
            dfs_dict = {}
            for echelle in echelles_of_interest:
                # ici on utilise bien le df ventes, qui contient l'ensemble
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
                dfs_dict[echelle] = merged

            general = {"code_geo": "nation"}
            for t in types_of_interest:
                general[
                    "nb_ventes_" + unidecode(
                        types_bien[t].split(" ")[0].lower()
                    )
                ] = len(
                    ventes.loc[
                        (ventes["code_type_local"] == t) &
                        (ventes["month"] == m)
                    ]
                )
                general[
                    "moy_prix_m2_" + unidecode(
                        types_bien[t].split(" ")[0].lower()
                    )
                ] = np.round(
                    ventes_nodup.loc[
                        (ventes_nodup["code_type_local"] == t) &
                        (ventes_nodup["month"] == m)
                    ]["prix_m2"].mean()
                )
                general[
                    "med_prix_m2_" + unidecode(
                        types_bien[t].split(" ")[0].lower()
                    )
                ] = np.round(
                    ventes_nodup.loc[
                        (ventes_nodup["code_type_local"] == t) &
                        (ventes_nodup["month"] == m)
                    ]["prix_m2"].median()
                )

            all_month = pd.concat(
                list(dfs_dict.values()) + [pd.DataFrame([general])]
            )
            all_month["annee_mois"] = f'{year}-{"0"+str(m) if m<10 else m}'
            export = pd.concat([export, all_month])
            del all_month
            del dfs_dict
            del general
        del ventes
        del ventes_nodup
        gc.collect()
        print("Done with", year)

    # on ajoute les colonnes libelle_geo et code_parent
    with open(DATADIR + "/sections.txt", 'r') as f:
        sections = [s.replace('\n', '') for s in f.readlines()]
    sections = pd.DataFrame(
        set(sections) | sections_from_dvf,
        columns=['code_geo']
    )
    sections['code_geo'] = sections['code_geo'].str.slice(0, 10)
    sections = sections.drop_duplicates()
    sections['code_parent'] = sections['code_geo'].str.slice(0, 5)
    sections['libelle_geo'] = sections['code_geo']
    sections['echelle_geo'] = 'section'
    departements = pd.read_csv(
        DATADIR + "/departements.csv", dtype=str, usecols=["DEP", "LIBELLE"]
    )
    departements = departements.rename(
        {"DEP": "code_geo", "LIBELLE": "libelle_geo"}, axis=1
    )
    departements['code_parent'] = 'nation'
    departements['echelle_geo'] = 'departement'
    epci_communes = epci[["code_commune", "code_epci"]]
    epci["code_parent"] = epci["code_commune"].apply(
        lambda code: code[:2] if code[:2] != "97" else code[:3]
    )
    epci = epci.drop("code_commune", axis=1)
    epci = epci.drop_duplicates(subset=["code_epci", "code_parent"]).rename(
        {"code_epci": "code_geo"}, axis=1
    )
    epci['echelle_geo'] = 'epci'

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
    communes = pd.merge(
        communes,
        pd.DataFrame(communes_from_dvf, columns=['code_geo']),
        on='code_geo',
        how='outer'
    )
    epci_communes = epci_communes.rename(
        {"code_commune": "code_geo", "code_epci": "code_parent"}, axis=1
    )
    communes = pd.merge(communes, epci_communes, on="code_geo", how="outer")
    communes.loc[
        communes['code_geo'].str.startswith('75'),
        'code_parent'
    ] = '200054781'
    communes.loc[
        communes['code_geo'].str.startswith('13'),
        'code_parent'
    ] = '200054807'
    communes.loc[
        communes['code_geo'].str.startswith('69'),
        'code_parent'
    ] = '200046977'
    communes['code_parent'].fillna(
        communes['code_geo'].str.slice(0, 2),
        inplace=True
    )
    communes['libelle_geo'].fillna('NA', inplace=True)
    communes['echelle_geo'] = 'commune'
    print("Done with géo")
    libelles_parents = pd.concat([departements, epci, communes, sections])
    del sections
    del communes
    del epci
    del departements
    libelles_parents["libelle_geo"] = (
        libelles_parents["libelle_geo"]
        .fillna("NA")
        .apply(unidecode)
    )
    # on crée l'ensemble des occurrences échelles X mois pour le merge
    dup_libelle = libelles_parents.append(
        [libelles_parents] * (12 * (max(years) - min(years) + 1) - 1),
        ignore_index=True
    ).sort_values(['code_geo', 'code_parent'])
    dup_libelle['annee_mois'] = [
        f'{y}-{"0"+str(m) if m<10 else m}'
        for y in range(min(years), max(years) + 1) for m in range(1, 13)
    ] * len(libelles_parents)
    del libelles_parents
    print("Done with libellés")
    # right merge pour avoir toutes les occurrences de toutes les échelles
    # (cf API)
    dup_libelle.set_index(['code_geo', 'annee_mois'], inplace=True)
    export = export.join(
        dup_libelle, on=['code_geo', 'annee_mois'],
        how='outer'
    )
    del dup_libelle
    print("Done with merge")
    if len(years) > 5:
        export = export.loc[
            export['annee_mois'].between(
                f'{min(years)}-07',
                f'{max(years)}-06'
            )
        ]
    mask = export['code_geo'] == 'nation'
    export.loc[mask, ['code_parent', 'libelle_geo', 'echelle_geo']] = [
        ['-', 'nation', 'nation'] for k in range(sum(mask))
    ]
    del mask
    gc.collect()

    export.to_csv(
        DATADIR + "/stats_dvf_api.csv",
        sep=",",
        encoding="utf8",
        index=False,
        float_format="%.0f",
    )
    print("Done with first export (API table)")

    mask = export[[c for c in export.columns if any([s in c for s in ['nb_', 'moy_', 'med_']])]]
    mask = mask.isna().all(axis=1)
    light_export = export.loc[~(mask)]
    del mask

    light_export = export.dropna()
    light_export.to_csv(
        DATADIR + "/stats_dvf.csv",
        sep=",",
        encoding="utf8",
        index=False,
        float_format="%.0f",
    )


def create_distribution():
    # on récupère toutes les échelles
    echelles = pd.read_csv(
        DATADIR + "/stats_dvf_api.csv",
        sep=",",
        encoding="utf8",
        usecols=['echelle_geo', 'code_geo']
    )
    echelles = echelles.drop_duplicates()
    # on récupère les données DVF
    years = sorted(
        [
            int(f.replace("full_", "").replace(".csv", ""))
            for f in os.listdir(DATADIR)
            if "full_" in f
        ]
    )
    dvf = pd.DataFrame(None)
    epci = pd.read_csv(
        DATADIR + "/epci.csv",
        sep=",",
        encoding="utf8",
        dtype=str
    )
    to_keep = [
        "id_mutation",
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
        print("Loading ", year)
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
        df = df_.drop_duplicates()
        df["code_section"] = df["id_parcelle"].str[:10]
        df = df.drop("id_parcelle", axis=1)

        natures_of_interest = [
            "Vente",
            "Vente en l'état futur d'achèvement",
            "Adjudication",
        ]
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
            (df["nature_mutation"].isin(natures_of_interest)) &
            (df["code_type_local"].isin([1, 2, 4]))
        ]
        del df
        # drop mutations multitypes pour les prix au m², impossible de
        # classer une mutation qui contient X maisons et Y appartements
        # par exemple
        # à voir pour la suite : quid des mutations avec dépendances, dont
        # le le prix est un prix de lot ? prix_m2 = prix_lot/surface_bien ?
        multitypes = ventes[["id_mutation", "code_type_local"]].value_counts()
        multitypes_ = multitypes.unstack()
        mutations_drop = multitypes_.loc[
            sum(
                [multitypes_[c].isna() for c in multitypes_.columns]
            ) < len(multitypes_.columns) - 1
        ].index
        ventes_nodup = ventes.loc[
            ~(ventes["id_mutation"].isin(mutations_drop)) &
            ~(ventes["code_type_local"].isna())
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
            ventes_nodup["valeur_fonciere"] /
            ventes_nodup["surface_totale_mutation"]
        )
        ventes_nodup["prix_m2"] = ventes_nodup["prix_m2"].replace(
            [np.inf, -np.inf], np.nan
        )

        # pas de prix ou pas de surface
        ventes_nodup = ventes_nodup.dropna(subset=["prix_m2"])
        dvf = pd.concat([dvf, ventes_nodup])
        del ventes_nodup

    dvf = pd.merge(
        dvf,
        epci[['code_commune', 'code_epci']],
        on="code_commune",
        how="left"
    )
    echelles_of_interest = ["departement", "epci", "commune", ]
    # "section"]
    dvf = dvf[['code_' + e for e in echelles_of_interest] + ['prix_m2']]
    nb_tranches = 10
    threshold = 100
    # échelle nationale
    bins = np.quantile(dvf['prix_m2'].unique(), [k / nb_tranches for k in range(nb_tranches + 1)])
    q = [[int(bins[k]), int(bins[k + 1])] for k in range(nb_tranches)]
    size = (q[-1][0] - q[0][1]) / (nb_tranches - 2)
    intervalles = [q[0]] +\
        [[q[0][1] + size * k, q[0][1] + size * (k + 1)]
            for k in range(nb_tranches - 2)] +\
        [q[-1]]
    volumes = pd.cut(dvf['prix_m2'],
                     bins=[i[0] for i in intervalles] + [intervalles[-1][1]]
                     ).value_counts().sort_index().to_list()
    tranches = [{
        'code_geo': 'nation',
        'xaxis': intervalles,
        'yaxis': volumes
    }]
    for e in echelles_of_interest:
        codes_geo = set(echelles.loc[echelles['echelle_geo'] == e, 'code_geo'])
        restr_dvf = dvf[[f'code_{e}', 'prix_m2']].set_index(f'code_{e}')['prix_m2']
        idx = set(restr_dvf.index)
        operations = len(codes_geo)
        print(e, operations)
        for i, code in enumerate(codes_geo):
            if i % (operations // 10) == 0 and i > 0:
                print(round(i / operations * 100), '%')
            if code in idx:
                prix = restr_dvf.loc[code]
                if not isinstance(prix, pd.core.series.Series):
                    prix = pd.Series(prix)
                if len(prix) >= threshold:
                    # 1er et dernier quantiles gardés
                    # on coupe le reste des données en tranches égales de prix (!= volumes)
                    # .unique() pour éviter des bornes identique => ValueError
                    bins = np.quantile(prix.unique(), [k / nb_tranches for k in range(nb_tranches + 1)])
                    q = [[int(bins[k]), int(bins[k + 1])] for k in range(nb_tranches)]
                    size = (q[-1][0] - q[0][1]) / (nb_tranches - 2)
                    intervalles = [q[0]] +\
                        [[q[0][1] + size * k, q[0][1] + size * (k + 1)]
                            for k in range(nb_tranches - 2)] +\
                        [q[-1]]
                    volumes = pd.cut(prix,
                                     bins=[i[0] for i in intervalles] + [intervalles[-1][1]]
                                     ).value_counts().sort_index().to_list()
                    tranches.append({
                        'code_geo': code,
                        'xaxis': intervalles,
                        'yaxis': volumes
                    })
                else:
                    tranches.append({
                        'code_geo': code,
                        'xaxis': None,
                        'yaxis': None
                    })
            else:
                tranches.append({
                    'code_geo': code,
                    'xaxis': None,
                    'yaxis': None
                })
    output_tranches = pd.DataFrame(tranches)
    output_tranches.to_csv(
        DATADIR + "/distribution_prix.csv",
        sep=",",
        encoding="utf8",
        index=False,
    )


def send_stats_to_minio():
    send_files(
        MINIO_URL=MINIO_URL,
        MINIO_BUCKET=MINIO_BUCKET_DATA_PIPELINE_OPEN,
        MINIO_USER=SECRET_MINIO_DATA_PIPELINE_USER,
        MINIO_PASSWORD=SECRET_MINIO_DATA_PIPELINE_PASSWORD,
        list_files=[
            {
                "source_path": f"{DATADIR}/",
                "source_name": "stats_dvf.csv",
                "dest_path": "dvf/",
                "dest_name": "stats_dvf.csv",
            }
        ],
    )


def send_distribution_to_minio():
    send_files(
        MINIO_URL=MINIO_URL,
        MINIO_BUCKET=MINIO_BUCKET_DATA_PIPELINE_OPEN,
        MINIO_USER=SECRET_MINIO_DATA_PIPELINE_USER,
        MINIO_PASSWORD=SECRET_MINIO_DATA_PIPELINE_PASSWORD,
        list_files=[
            {
                "source_path": f"{DATADIR}/",
                "source_name": "distribution_prix.csv",
                "dest_path": "dvf/",
                "dest_name": "distribution_prix.csv",
            }
        ],
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
        f"\n- publié [sur {'demo.' if AIRFLOW_ENV == 'dev' else ''}data.gouv.fr]"
        f"({url}{dataset_id})"
        f"\n- données upload [sur Minio]({MINIO_URL}/buckets/{MINIO_BUCKET_DATA_PIPELINE_OPEN}/browse)"
    )
