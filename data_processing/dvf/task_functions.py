from airflow.hooks.base import BaseHook
from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_HOME,
    AIRFLOW_DAG_TMP,
    DATAGOUV_SECRET_API_KEY,
    AIRFLOW_ENV,
    MINIO_URL,
    MINIO_BUCKET_DATA_PIPELINE,
    SECRET_MINIO_DATA_PIPELINE_USER,
    SECRET_MINIO_DATA_PIPELINE_PASSWORD,
)
from datagouvfr_data_pipelines.utils.postgres import (
    execute_sql_file,
    copy_file
)
from datagouvfr_data_pipelines.utils.datagouv import post_resource
from datagouvfr_data_pipelines.utils.mattermost import send_message
from datagouvfr_data_pipelines.utils.minio import send_files
import gc
import glob
from unidecode import unidecode
import numpy as np
import os
import shutil
import zipfile
import pandas as pd
import requests
import time
import json

DAG_FOLDER = "datagouvfr_data_pipelines/data_processing/"
DATADIR = f"{AIRFLOW_DAG_TMP}dvf/data"
DPEDIR = f"{DATADIR}/dpe/"

conn = BaseHook.get_connection("POSTGRES_DEV_DVF")


def create_copro_table():
    execute_sql_file(
        conn.host,
        conn.port,
        conn.schema,
        conn.login,
        conn.password,
        [
            {
                "source_path": f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}dvf/sql/",
                "source_name": "create_copro_table.sql",
            }
        ],
        'dvf',
    )


def populate_copro_table():
    new_cols = [
        "epci",
        "commune",
        "numero_immatriculation",
        "type_syndic",
        "identification_representant_legal",
        "siret_representant_legal",
        "code_ape",
        "commune_representant_legal",
        "mandat_en_cours_copropriete",
        "nom_usage_copropriete",
        "adresse_reference",
        "numero_et_voie_adresse_reference",
        "code_postal_adresse_reference",
        "commune_adresse_reference",
        "adresse_complementaire_1",
        "adresse_complementaire_2",
        "adresse_complementaire_3",
        "nombre_adresses_complementaires",
        "long",
        "lat",
        "date_reglement_copropriete",
        "residence_service",
        "syndicat_cooperatif",
        "syndicat_principal_ou_secondaire",
        "si_secondaire_numero_immatriculation_principal",
        "nombre_asl_rattache_syndicat_coproprietaires",
        "nombre_aful_rattache_syndicat_coproprietaires",
        "nombre_unions_syndicats_rattache_syndicat_coproprietaires",
        "nombre_total_lots",
        "nombre_total_lots_usage_habitation_bureaux_ou_commerces",
        "nombre_lots_usage_habitation",
        "nombre_lots_stationnement",
        "nombre_arretes_code_sante_publique_en_cours",
        "nombre_arretes_peril_parties_communes_en_cours",
        "nombre_arretes_equipements_communs_en_cours",
        "periode_construction",
        "reference_cadastrale_1",
        "code_insee_commune_1",
        "prefixe_1",
        "section_1",
        "numero_parcelle_1",
        "reference_cadastrale_2",
        "code_insee_commune_2",
        "prefixe_2",
        "section_2",
        "numero_parcelle_2",
        "reference_cadastrale_3",
        "code_insee_commune_3",
        "prefixe_3",
        "section_3",
        "numero_parcelle_3",
        "nombre_parcelles_cadastrales",
        "nom_qp",
        "code_qp",
        "copro_dans_acv",
        "copro_dans_pvd",
    ]
    copro = pd.read_csv(f"{DATADIR}/copro.csv", dtype=str)
    assert len(new_cols) == len(copro.columns)
    mapping = {old: new for (old, new) in zip(copro.columns, new_cols)}
    copro = copro.rename(mapping, axis=1)
    # remove abnormalities
    for c in copro.columns:
        if 'insee' in c:
            copro = copro.loc[copro[c].str.len() == 5]
    copro.to_csv(f"{DATADIR}/copro_clean.csv", index=False)
    populate_utils([f"{DATADIR}/copro_clean.csv"], "dvf.copro", True)


def create_dpe_table():
    execute_sql_file(
        conn.host,
        conn.port,
        conn.schema,
        conn.login,
        conn.password,
        [
            {
                "source_path": f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}dvf/sql/",
                "source_name": "create_dpe_table.sql",
            }
        ],
        'dvf',
    )


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
        'dvf',
    )


def index_dvf_table():
    execute_sql_file(
        conn.host,
        conn.port,
        conn.schema,
        conn.login,
        conn.password,
        [
            {
                "source_path": f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}dvf/sql/",
                "source_name": "index_dvf_table.sql",
            }
        ],
        'dvf',
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
        'dvf',
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
        'dvf',
    )


def populate_utils(files, table, header):
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
        PG_SCHEMA='dvf',
        header=header,
    )


def populate_distribution_table():
    populate_utils([f"{DATADIR}/distribution_prix.csv"], "dvf.distribution_prix", True)


def populate_dvf_table():
    # files = glob.glob(f"{DATADIR}/full_*.csv")
    # # adding section_prefixe column for API
    # for file in files:
    #     df = pd.read_csv(file, dtype=str)
    #     df['section_prefixe'] = df['id_parcelle'].str.slice(5, 10)
    #     df.to_csv(file.replace("full_", "enriched_"), index=False)
    files = glob.glob(f"{DATADIR}/full*.csv")
    populate_utils(files, "dvf.dvf", True)


def alter_dvf_table():
    execute_sql_file(
        conn.host,
        conn.port,
        conn.schema,
        conn.login,
        conn.password,
        [
            {
                "source_path": f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}dvf/sql/",
                "source_name": "alter_dvf_table.sql",
            }
        ],
        'dvf',
    )


def populate_stats_dvf_table():
    populate_utils([f"{DATADIR}/stats_dvf_api.csv"], "dvf.stats_dvf", True)


def populate_dpe_table():
    populate_utils([f"{DATADIR}/all_dpe.csv"], "dvf.dpe", False)


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
    

def process_dpe():
    cols_dpe = [
        'batiment_groupe_id',
        'identifiant_dpe',
        'type_batiment_dpe',
        'periode_construction_dpe',
        'annee_construction_dpe',
        'date_etablissement_dpe',
        'nombre_niveau_logement',
        'nombre_niveau_immeuble',
        'surface_habitable_immeuble',
        'surface_habitable_logement',
        'classe_bilan_dpe',
        'classe_emission_ges',
    ]
    cols_parcelles = [
        "batiment_groupe_id",
        "parcelle_id"
    ]
    dep_folders = os.listdir(DPEDIR)
    for dep in dep_folders:
        print(dep)
        dpe = pd.read_csv(
            f'{DPEDIR}{dep}/batiment_groupe_dpe_representatif_logement.csv',
            dtype=str,
            usecols=cols_dpe
        )
        dpe['date_etablissement_dpe'] = dpe['date_etablissement_dpe'].str.slice(0, 10)
        dpe['surface_habitable_logement'] = dpe['surface_habitable_logement'].apply(
            lambda x: round(float(x), 2)
        )
        parcelles = pd.read_csv(
            f'{DPEDIR}{dep}/rel_batiment_groupe_parcelle.csv',
            dtype=str,
            usecols=cols_parcelles
        )
        dpe_parcelled = pd.merge(
            dpe,
            parcelles,
            on='batiment_groupe_id',
            how='left'
        )
        dpe_parcelled = dpe_parcelled.dropna(subset=['parcelle_id'])
        dpe_parcelled.to_csv(
            DATADIR + "/all_dpe.csv",
            mode='a',
            sep=",",
            index=False,
            encoding="utf8",
            header=False
        )


def index_dpe_table():
    execute_sql_file(
        conn.host,
        conn.port,
        conn.schema,
        conn.login,
        conn.password,
        [
            {
                "source_path": f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}dvf/sql/",
                "source_name": "index_dpe_table.sql",
            }
        ],
        'dvf',
    )

def process_dvf_stats(ti):
    years = sorted(
        [
            int(f.replace("full_", "").replace(".csv", ""))
            for f in os.listdir(DATADIR)
            if "full_" in f and ".gz" not in f
        ]
    )
    export = {}
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
        print("Starting with", year)
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
        export_intermediary = []

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
            all_month["annee_mois"] = f'{year}-{"0"+str(m) if m < 10 else m}'
            export_intermediary.append(all_month)
            del all_month
            del dfs_dict
            del general
        export[year] = pd.concat(export_intermediary)
        del export_intermediary
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
    for year in years:
        print("Final process for " + str(year))
        dup_libelle = libelles_parents.append(
            [libelles_parents] * 11,
            ignore_index=True
        ).sort_values(['code_geo', 'code_parent'])
        dup_libelle['annee_mois'] = [
            f'{year}-{"0"+str(m) if m < 10 else m}'
            for m in range(1, 13)
        ] * len(libelles_parents)
        dup_libelle.set_index(['code_geo', 'annee_mois'], inplace=True)
        export[year] = export[year].join(
            dup_libelle, on=['code_geo', 'annee_mois'],
            how='outer'
        )
        if len(years) > 5 and year in [min(years), max(years)]:
            export[year] = export[year].loc[
                export[year]['annee_mois'].between(
                    f'{min(years)}-07',
                    f'{max(years)}-06'
                )
            ]
        mask = export[year]['code_geo'] == 'nation'
        export[year].loc[mask, ['code_parent', 'libelle_geo', 'echelle_geo']] = [
            ['-', 'nation', 'nation'] for k in range(sum(mask))
        ]
        del mask
        libelles_biens = [unidecode(types_bien.get(t).split(" ")[0].lower()) for t in types_of_interest]
        prefixes = ['nb_ventes_', 'moy_prix_m2_', 'med_prix_m2_']
        reordered_columns = ['code_geo'] +\
            [pref + lib for lib in libelles_biens for pref in prefixes] +\
            ['annee_mois', 'libelle_geo', 'code_parent', 'echelle_geo']
        export[year] = export[year][reordered_columns]
        export[year].to_csv(
            DATADIR + "/stats_dvf_api.csv",
            sep=",",
            encoding="utf8",
            index=False,
            float_format="%.0f",
            mode='w' if year == min(years) else 'a',
            header=True if year == min(years) else False
        )
        print("Done with first export (API table)")

        mask = export[year][[
            c for c in export[year].columns if any([s in c for s in ['nb_', 'moy_', 'med_']])
        ]]
        mask = mask.isna().all(axis=1)
        light_export = export[year].loc[~(mask)]
        del mask

        light_export.to_csv(
            DATADIR + "/stats_dvf.csv",
            sep=",",
            encoding="utf8",
            index=False,
            float_format="%.0f",
            mode='w' if year == min(years) else 'a',
            header=True if year == min(years) else False
        )
        del export[year]
        print("Done with year " + str(year))


def create_distribution():
    def process_borne(borne, borne_inf, borne_sup):
        # handle rounding of bounds
        if round(borne, -2) <= borne_inf or round(borne, -2) >= borne_sup:
            return round(borne)
        else:
            return round(borne, -2)

    def distrib_from_prix(
        prix,
        nb_tranches=10,
        arrondi=True
    ):
        # 1er et dernier quantiles gardés
        # on coupe le reste des données en tranches égales de prix (!= volumes)
        # .unique() pour éviter des bornes identique => ValueError
        bins = np.quantile(prix.unique(), [k / nb_tranches for k in range(nb_tranches + 1)])
        q = [[int(bins[k]), int(bins[k + 1])] for k in range(nb_tranches)]
        size = (q[-1][0] - q[0][1]) / (nb_tranches - 2)
        # to include the minimum price if it is equal to the lower bound, lower bound-1
        inf = q[0][0] - 1 if min(prix) == int(min(prix)) else q[0][0]
        # due to int, the upper bound is rounded down so +1 to include it
        sup = q[-1][1] + 1
        intervalles = [[inf, q[0][1]]] +\
            [[q[0][1] + size * k, q[0][1] + size * (k + 1)]
                for k in range(nb_tranches - 2)] +\
            [[q[-1][0], sup]]
        if arrondi:
            # keep min and max values unchanged
            borne_inf = intervalles[0][0]
            borne_sup = intervalles[-1][1]
            intervalles = [[borne_inf, process_borne(intervalles[0][1], borne_inf, borne_sup)]] + [
                [process_borne(i[0], borne_inf, borne_sup), process_borne(i[1], borne_inf, borne_sup)]
                for i in intervalles[1:-1]
            ] + [[process_borne(intervalles[-1][0], borne_inf, borne_sup), borne_sup]]
        bins = [i[0] for i in intervalles] + [intervalles[-1][1]]
        # handle case where rounding creates indentical bins
        if len(bins) != len(set(bins)):
            # check how many times bounds appear
            count_bins = pd.Series(bins).value_counts().sort_index()
            # get ranges between redundant bounds and next ones
            ranges = [
                (count_bins.index[k + 1] - count_bins.index[k]) / count_bins.values[k]
                for k in range(len(count_bins) - 1)
            ]
            # create new bins from bounds and intervals
            new_bins = [bins[0]]
            for idx, b in enumerate(count_bins.values[:-1]):
                for k in range(b):
                    new_bins.append(new_bins[-1] + ranges[idx])
            bins = list(map(round, new_bins))
        volumes = pd.cut(
            prix,
            bins=bins
        ).value_counts().sort_index().to_list()
        return intervalles, volumes

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
            if "full_" in f and ".gz" not in f
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
    types_of_interest = {
        'appartement': [2],
        'maison': [1],
        'apt+maison': [1, 2],
        'local': [4],
    }
    dvf = dvf[['code_' + e for e in echelles_of_interest] + ['code_type_local'] + ['prix_m2']]
    threshold = 100
    tranches = []
    for t in types_of_interest:
        restr_type_dvf = dvf.loc[
            dvf['code_type_local'].isin(types_of_interest[t])
        ].drop('code_type_local', axis=1)
        # échelle nationale
        intervalles, volumes = distrib_from_prix(restr_type_dvf['prix_m2'])
        echelle_dict = {
            'code_geo': 'nation',
            'type_local': t,
            'xaxis': intervalles,
            'yaxis': volumes
        }
        tranches.append(echelle_dict)
        # autres échelles
        for e in echelles_of_interest:
            codes_geo = set(echelles.loc[echelles['echelle_geo'] == e, 'code_geo'])
            restr_dvf = restr_type_dvf[[f'code_{e}', 'prix_m2']].set_index(f'code_{e}')['prix_m2']
            idx = set(restr_dvf.index)
            operations = len(codes_geo)
            print(e, t)
            for i, code in enumerate(codes_geo):
                if i % (operations // 10) == 0 and i > 0:
                    print(round(i / operations * 100), '%')
                if code in idx:
                    prix = restr_dvf.loc[code]
                    if not isinstance(prix, pd.core.series.Series):
                        prix = pd.Series(prix)
                    if len(prix) >= threshold:
                        intervalles, volumes = distrib_from_prix(prix)
                        tranches.append({
                            'code_geo': code,
                            'type_local': t,
                            'xaxis': intervalles,
                            'yaxis': volumes
                        })
                    else:
                        tranches.append({
                            'code_geo': code,
                            'type_local': t,
                            'xaxis': None,
                            'yaxis': None
                        })
                else:
                    tranches.append({
                        'code_geo': code,
                        'type_local': t,
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
    print(MINIO_URL)
    print('il est là :', MINIO_BUCKET_DATA_PIPELINE, '<=')
    print(SECRET_MINIO_DATA_PIPELINE_USER)
    send_files(
        MINIO_URL=MINIO_URL,
        MINIO_BUCKET=MINIO_BUCKET_DATA_PIPELINE,
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
        MINIO_BUCKET=MINIO_BUCKET_DATA_PIPELINE,
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
        f"\n- données upload [sur Minio]({MINIO_URL}/buckets/{MINIO_BUCKET_DATA_PIPELINE}/browse)"
    )
