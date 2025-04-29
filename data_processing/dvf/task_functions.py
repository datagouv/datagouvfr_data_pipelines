from datetime import datetime
from functools import reduce
import glob
import json
import logging
import os

import gc
import numpy as np
import pandas as pd
import requests
from unidecode import unidecode

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_HOME,
    AIRFLOW_DAG_TMP,
    AIRFLOW_ENV,
    MINIO_URL,
    MINIO_BUCKET_DATA_PIPELINE,
    MINIO_BUCKET_DATA_PIPELINE_OPEN,
)
from datagouvfr_data_pipelines.utils.postgres import PostgresClient
from datagouvfr_data_pipelines.utils.datagouv import post_remote_resource, DATAGOUV_URL
from datagouvfr_data_pipelines.utils.filesystem import File
from datagouvfr_data_pipelines.utils.mattermost import send_message
from datagouvfr_data_pipelines.utils.minio import MinIOClient

DAG_FOLDER = "datagouvfr_data_pipelines/data_processing/"
DATADIR = f"{AIRFLOW_DAG_TMP}dvf/data"
DPEDIR = f"{DATADIR}/dpe/"
schema = "dvf"

pgclient = PostgresClient(
    conn_name="POSTGRES_DVF",
    schema=schema,
)
minio_restricted = MinIOClient(bucket=MINIO_BUCKET_DATA_PIPELINE)
minio_open = MinIOClient(bucket=MINIO_BUCKET_DATA_PIPELINE_OPEN)


def get_year_interval() -> tuple[int, int]:
    today = datetime.today()
    # data updates happen in April and October
    if 4 <= today.month < 10:
        return today.year - 5, today.year - 1
    return today.year - 5, today.year


def build_table_name(table: str) -> str:
    # modify this to suit your local database structure
    return f'{schema}.{table}' if AIRFLOW_ENV == "prod" else table


def create_copro_table() -> None:
    pgclient.execute_sql_file(
        file=File(
            source_path=f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}dvf/sql/",
            source_name="create_copro_table.sql",
        ),
    )


def create_dpe_table() -> None:
    pgclient.execute_sql_file(
        file=File(
            source_path=f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}dvf/sql/",
            source_name="create_dpe_table.sql",
        ),
    )


def create_dvf_table() -> None:
    pgclient.execute_sql_file(
        file=File(
            source_path=f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}dvf/sql/",
            source_name="create_dvf_table.sql",
        ),
    )


def index_dvf_table() -> None:
    pgclient.execute_sql_file(
        file=File(
            source_path=f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}dvf/sql/",
            source_name="index_dvf_table.sql",
        ),
    )


def create_stats_dvf_table() -> None:
    pgclient.execute_sql_file(
        file=File(
            source_path=f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}dvf/sql/",
            source_name="create_stats_dvf_table.sql",
        ),
    )


def create_distribution_table() -> None:
    pgclient.execute_sql_file(
        file=File(
            source_path=f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}dvf/sql/",
            source_name="create_distribution_table.sql",
        ),
    )


def create_whole_period_table() -> None:
    pgclient.execute_sql_file(
        file=File(
            source_path=f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}dvf/sql/",
            source_name="create_whole_period_table.sql",
        ),
    )


def populate_copro_table() -> None:
    mapping = {
        "EPCI": "epci",
        "Commune": "commune",
        "Numéro d'immatriculation": "numero_immatriculation",
        "Type de syndic : bénévole / professionnel / non connu": "type_syndic",
        "Identification du représentant légal  (raison sociale et le numéro SIRET du syndic professionnel ou Civilité/prénom/ nom du syndic bénévole ou coopératif)": "identification_representant_legal",
        "SIRET du représentant légal": "siret_representant_legal",
        "Code APE": "code_ape",
        "Commune du représentant légal": "commune_representant_legal",
        "Mandat en cours dans la copropriété": "mandat_en_cours_copropriete",
        "Nom d’usage de la copropriété": "nom_usage_copropriete",
        "Adresse de référence": "adresse_reference",
        "Numéro et Voie (adresse de référence)": "numero_et_voie_adresse_reference",
        "Code postal (adresse de référence)": "code_postal_adresse_reference",
        "Commune (adresse de référence)": "commune_adresse_reference",
        "Adresse complémentaire 1": "adresse_complementaire_1",
        "Adresse complémentaire 2": "adresse_complementaire_2",
        "Adresse complémentaire 3": "adresse_complementaire_3",
        "Nombre d'adresses complémentaires": "nombre_adresses_complementaires",
        "long": "long",
        "lat": "lat",
        "Date du règlement de copropriété": "date_reglement_copropriete",
        "Résidence service": "residence_service",
        "Syndicat coopératif": "syndicat_cooperatif",
        "Syndicat principal ou syndicat secondaire": "syndicat_principal_ou_secondaire",
        "Si secondaire, n° d’immatriculation du principal": "si_secondaire_numero_immatriculation_principal",
        "Nombre d’ASL auxquelles est rattaché le syndicat de copropriétaires": (
            "nombre_asl_rattache_syndicat_coproprietaires"
        ),
        "Nombre d’AFUL auxquelles est rattaché le syndicat de copropriétaires": (
            "nombre_aful_rattache_syndicat_coproprietaires"
        ),
        "Nombre d’Unions de syndicats auxquelles est rattaché le syndicat de copropriétaires": (
            "nombre_unions_syndicats_rattache_syndicat_coproprietaires"
        ),
        "Nombre total de lots": "nombre_total_lots",
        "Nombre total de lots à usage d’habitation, de bureaux ou de commerces": (
            "nombre_total_lots_usage_habitation_bureaux_ou_commerces"
        ),
        "Nombre de lots à usage d’habitation": "nombre_lots_usage_habitation",
        "Nombre de lots de stationnement": "nombre_lots_stationnement",
        # "Nombre d'arrêtés relevant du code de la santé publique en cours": (
        #     "nombre_arretes_code_sante_publique_en_cours"
        # ),
        # "Nombre d'arrêtés de péril sur les parties communes en cours": (
        #     "nombre_arretes_peril_parties_communes_en_cours"
        # ),
        # "Nombre d'arrêtés sur les équipements communs en cours": (
        #     "nombre_arretes_equipements_communs_en_cours"
        # ),
        "Période de construction": "periode_construction",
        "Référence Cadastrale 1": "reference_cadastrale_1",
        "Référence Cadastrale 2": "reference_cadastrale_2",
        "Référence Cadastrale 3": "reference_cadastrale_3",
        "Nombre de parcelles cadastrales": "nombre_parcelles_cadastrales",
        "nom_qp_2024": "nom_qp",
        "code_qp_2024": "code_qp",
        "Copro dans ACV": "copro_dans_acv",
        "Copro dans PVD": "copro_dans_pvd",
    }
    copro = pd.read_csv(
        f"{DATADIR}/copro.csv",
        dtype=str,
        usecols=mapping.keys(),
    )
    copro = copro.rename(mapping, axis=1)
    copro = copro.loc[copro['commune'].str.len() == 5]
    copro.to_csv(f"{DATADIR}/copro_clean.csv", index=False)
    pgclient.copy_file(
        file=File(source_path=f"{DATADIR}/", source_name="copro_clean.csv"),
        table=build_table_name("copro"),
        has_header=True,
    )


def populate_distribution_table() -> None:
    pgclient.copy_file(
        file=File(source_path=f"{DATADIR}/", source_name="distribution_prix.csv"),
        table=build_table_name("distribution_prix"),
        has_header=True,
    )


def populate_dvf_table() -> None:
    files = glob.glob(f"{DATADIR}/full*.csv")
    for file in files:
        *path, file = file.split("/")
        logging.info(f"Populating {file}")
        pgclient.copy_file(
            file=File(source_path="/".join(path), source_name=file),
            table=build_table_name("dvf"),
            has_header=True,
        )


def alter_dvf_table() -> None:
    pgclient.execute_sql_file(
        File(
            source_path=f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}dvf/sql/",
            source_name="alter_dvf_table.sql",
        ),
    )


def populate_stats_dvf_table() -> None:
    pgclient.copy_file(
        file=File(source_path=f"{DATADIR}/", source_name="stats_dvf_api.csv"),
        table=build_table_name("stats_dvf"),
        has_header=True,
    )


def populate_dpe_table() -> None:
    pgclient.copy_file(
        file=File(source_path=f"{DATADIR}/", source_name="all_dpe.csv"),
        table=build_table_name("dpe"),
        has_header=False,
    )


def populate_whole_period_table() -> None:
    pgclient.copy_file(
        file=File(source_path=f"{DATADIR}/", source_name="stats_whole_period.csv"),
        table=build_table_name("stats_whole_period"),
        has_header=True,
    )


def get_epci() -> None:
    epci = requests.get(
        "https://unpkg.com/@etalab/decoupage-administratif/data/epci.json"
    ).json()
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
        epci_list, columns=["code_commune", "code_epci", "libelle_geo"],
    ).to_csv(DATADIR + "/epci.csv", sep=",", encoding="utf8", index=False)


def process_dpe() -> None:
    cols_dpe = {
        'batiment_groupe_id': str,
        # 'identifiant_dpe',
        # 'type_batiment_dpe',
        'periode_construction_dpe': "category",
        # 'annee_construction_dpe',
        # 'date_etablissement_dpe',
        # 'nombre_niveau_logement',
        'nombre_niveau_immeuble': str,
        'surface_habitable_immeuble': str,
        # 'surface_habitable_logement',
        'classe_bilan_dpe': "category",
        'classe_emission_ges': "category",
    }
    cols_parcelles = [
        "batiment_groupe_id",
        "parcelle_id",
    ]
    logging.info("Import des batiment_id pour imports segmentés")
    # these files are too big to be loaded at once
    # ids look like this: "bdnb-bg-5CWD-3J5Q-VEGE"
    # they seem to be in even groups if considering the "bdnb-bg-X" prefix
    # we'll loop through these to merge subparts and append them
    # if this becomes too heavy we can move down one more character for prefixes
    bat_id = pd.read_csv(
        DATADIR + '/csv/batiment_groupe_dpe_representatif_logement.csv',
        usecols=["batiment_groupe_id"],
        sep=",",
    )
    prefixes = list(bat_id['batiment_groupe_id'].str.slice(0, 9).unique())
    logging.info(f"{len(prefixes)} prefixes to process")
    del bat_id
    logging.info("Imports et traitements DPE x parcelles par batch...")
    chunk_size = 100000
    for idx, pref in enumerate(prefixes):
        iter_dpe = pd.read_csv(
            DATADIR + '/csv/batiment_groupe_dpe_representatif_logement.csv',
            dtype=cols_dpe,
            usecols=cols_dpe.keys(),
            sep=",",
            iterator=True,
            chunksize=chunk_size,
        )
        # dpe['date_etablissement_dpe'] = dpe['date_etablissement_dpe'].str.slice(0, 10)
        # dpe['surface_habitable_logement'] = dpe['surface_habitable_logement'].apply(
        #     lambda x: round(float(x), 2)
        # )
        dpe = pd.concat([
            chunk.loc[chunk["batiment_groupe_id"].str.startswith(pref)]
            for chunk in iter_dpe
        ])
        del iter_dpe
        logging.info(f"> Processing {pref}: {len(dpe)} values ({idx + 1}/{len(prefixes)})")
        dpe.set_index('batiment_groupe_id', inplace=True)
        iter_parcelles = pd.read_csv(
            DATADIR + '/csv/rel_batiment_groupe_parcelle.csv',
            dtype=str,
            usecols=cols_parcelles,
            sep=",",
            iterator=True,
            chunksize=chunk_size,
        )
        parcelles = pd.concat([
            chunk.loc[chunk["batiment_groupe_id"].str.startswith(pref)]
            for chunk in iter_parcelles
        ])
        del iter_parcelles
        parcelles.set_index('batiment_groupe_id', inplace=True)
        logging.info("Merging...")
        dpe_parcelled = dpe.join(
            parcelles,
            on='batiment_groupe_id',
            how='left',
        )
        del dpe
        del parcelles
        dpe_parcelled.reset_index(inplace=True)
        dpe_parcelled = dpe_parcelled.dropna(subset=['parcelle_id'])
        dpe_parcelled.to_csv(
            DATADIR + "/all_dpe.csv",
            sep=",",
            index=False,
            encoding="utf8",
            header=False,
            mode="w" if idx == 0 else "a",
        )
        del dpe_parcelled


def index_dpe_table() -> None:
    pgclient.execute_sql_file(
        File(
            source_path=f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}dvf/sql/",
            source_name="index_dpe_table.sql",
        ),
    )


def process_dvf_stats() -> None:
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
        dtype=str,
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
    natures_of_interest = [
        "Vente",
        "Vente en l'état futur d'achèvement",
        "Adjudication",
    ]
    types_of_interest = [1, 2, 4]
    echelles_of_interest = ["departement", "epci", "commune", "section"]
    for year in years:
        logging.info(f"Starting with {year}")
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
        # multi-type
        ventes = df.loc[
            (df["nature_mutation"].isin(natures_of_interest))
            & (df["code_type_local"].isin(types_of_interest))
        ]
        del df
        ventes["month"] = ventes["date_mutation"].apply(lambda x: int(x.split("-")[1]))
        logging.info(f"Après déduplication et filtre types et natures : {len(ventes)}")

        # on ne garde que les ventes d'un seul bien
        # cf historique pour les ventes multi-types
        count_ventes = ventes['id_mutation'].value_counts().reset_index()
        liste_ventes_monobien = count_ventes.loc[count_ventes["count"] == 1, "id_mutation"]
        ventes_nodup = ventes.loc[ventes['id_mutation'].isin(liste_ventes_monobien)]
        logging.info(f"Après filtrage des ventes de plusieurs biens : {len(ventes_nodup)}")

        ventes_nodup["prix_m2"] = ventes_nodup["valeur_fonciere"] / ventes_nodup["surface_reelle_bati"]
        ventes_nodup["prix_m2"] = ventes_nodup["prix_m2"].replace([np.inf, -np.inf], np.nan)

        # pas de prix ou pas de surface
        ventes_nodup = ventes_nodup.dropna(subset=["prix_m2"])

        # garde fou pour les valeurs aberrantes
        ventes_nodup = ventes_nodup.loc[ventes_nodup['prix_m2'] < 100000]
        logging.info(f"Après retrait des ventes sans prix au m² et valeurs aberrantes : {len(ventes_nodup)}")
        export_intermediary = []

        # avoid unnecessary steps due to half years
        month_range = range(1, 13)
        if len(years) == 6:
            if year == min(years):
                month_range = range(7, 13)
            elif year == max(years):
                month_range = range(1, 7)

        for m in month_range:
            dfs_dict = {}
            for echelle in echelles_of_interest:
                grouped = ventes_nodup.groupby(
                    [f"code_{echelle}", "month", "type_local"]
                )["prix_m2"]

                nb = grouped.count()
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

                # appartement + maison
                combined = ventes_nodup.loc[
                    ventes_nodup["code_type_local"].isin([1, 2])
                ].groupby(
                    [f"code_{echelle}", "month"]
                )["prix_m2"]

                nb = combined.count()
                nb_ = (
                    nb.loc[nb.index.get_level_values(1) == m]
                    .unstack()
                    .reset_index()
                )
                nb_.columns = [f'code_{echelle}', 'nb_ventes_apt_maison']

                mean = combined.mean()
                mean_ = (
                    mean.loc[mean.index.get_level_values(1) == m]
                    .unstack()
                    .reset_index()
                )
                mean_.columns = [f'code_{echelle}', 'moy_prix_m2_apt_maison']

                median = combined.median()
                median_ = (
                    median.loc[median.index.get_level_values(1) == m]
                    .unstack()
                    .reset_index()
                )
                median_.columns = [f'code_{echelle}', 'med_prix_m2_apt_maison']

                merged = pd.merge(merged, nb_, on=[f"code_{echelle}"], how="outer")
                merged = pd.merge(merged, mean_, on=[f"code_{echelle}"])
                merged = pd.merge(merged, median_, on=[f"code_{echelle}"])

                for c in merged.columns:
                    if any([k in c for k in ["moy_", "med_"]]):
                        merged[c] = merged[c].round()

                merged.rename(
                    columns={f"code_{echelle}": "code_geo"},
                    inplace=True,
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
                        (ventes["code_type_local"] == t)
                        & (ventes["month"] == m)
                    ]
                )
                general[
                    "moy_prix_m2_" + unidecode(
                        types_bien[t].split(" ")[0].lower()
                    )
                ] = np.round(
                    ventes_nodup.loc[
                        (ventes_nodup["code_type_local"] == t)
                        & (ventes_nodup["month"] == m)
                    ]["prix_m2"].mean()
                )
                general[
                    "med_prix_m2_" + unidecode(
                        types_bien[t].split(" ")[0].lower()
                    )
                ] = np.round(
                    ventes_nodup.loc[
                        (ventes_nodup["code_type_local"] == t)
                        & (ventes_nodup["month"] == m)
                    ]["prix_m2"].median()
                )

            general["nb_ventes_apt_maison"] = len(
                ventes.loc[
                    (ventes["code_type_local"].isin([1, 2]))
                    & (ventes["month"] == m)
                ]
            )
            general["moy_prix_m2_apt_maison"] = np.round(
                ventes_nodup.loc[
                    (ventes_nodup["code_type_local"].isin([1, 2]))
                    & (ventes_nodup["month"] == m)
                ]["prix_m2"].mean()
            )
            general["med_prix_m2_apt_maison"] = np.round(
                ventes_nodup.loc[
                    (ventes_nodup["code_type_local"].isin([1, 2]))
                    & (ventes_nodup["month"] == m)
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
        logging.info(f"Done with {year}")

    # on ajoute les colonnes libelle_geo et code_parent
    with open(DATADIR + "/sections.txt", 'r') as f:
        sections = [s.replace('\n', '') for s in f.readlines()]
    sections = pd.DataFrame(
        set(sections) | sections_from_dvf,
        columns=['code_geo'],
    )
    sections['code_geo'] = sections['code_geo'].str.slice(0, 10)
    sections = sections.drop_duplicates()
    sections['code_parent'] = sections['code_geo'].str.slice(0, 5)
    sections['libelle_geo'] = sections['code_geo']
    sections['echelle_geo'] = 'section'
    departements = pd.read_csv(
        DATADIR + "/departements.csv", dtype=str, usecols=["DEP", "LIBELLE"],
    )
    departements = departements.rename(
        {"DEP": "code_geo", "LIBELLE": "libelle_geo"}, axis=1,
    )
    departements['code_parent'] = 'nation'
    departements['echelle_geo'] = 'departement'
    epci_communes = epci[["code_commune", "code_epci"]]
    epci["code_parent"] = epci["code_commune"].apply(
        lambda code: code[:2] if code[:2] != "97" else code[:3]
    )
    epci = epci.drop("code_commune", axis=1)
    epci = epci.drop_duplicates(subset=["code_epci", "code_parent"]).rename(
        {"code_epci": "code_geo"}, axis=1,
    )
    epci['echelle_geo'] = 'epci'

    communes = pd.read_csv(
        DATADIR + "/communes.csv",
        dtype=str,
        usecols=["TYPECOM", "COM", "LIBELLE"],
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
    logging.info("Done with géo")
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
    libelles_biens = [
        unidecode(types_bien.get(t).split(" ")[0].lower()) for t in types_of_interest
    ] + ['apt_maison']
    prefixes = ['nb_ventes_', 'moy_prix_m2_', 'med_prix_m2_']
    reordered_columns = ['code_geo'] +\
        [pref + lib for lib in libelles_biens for pref in prefixes] +\
        ['annee_mois', 'libelle_geo', 'code_parent', 'echelle_geo']
    logging.info(reordered_columns)
    for year in years:
        logging.info("Final process for " + str(year))
        dup_libelle = pd.concat(
            [libelles_parents for _ in range(12)]
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
            ['-', 'nation', 'nation'] for _ in range(sum(mask))
        ]
        del mask
        export[year] = export[year][reordered_columns]
        export[year].to_csv(
            DATADIR + "/stats_dvf_api.csv",
            sep=",",
            encoding="utf8",
            index=False,
            float_format="%.0f",
            mode='w' if year == min(years) else 'a',
            header=True if year == min(years) else False,
        )
        logging.info("Done with first export (API table)")

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
            header=True if year == min(years) else False,
        )
        del export[year]
        logging.info("Done with year " + str(year))


def create_distribution_and_stats_whole_period() -> None:
    def process_borne(borne: float, borne_inf: int, borne_sup: int) -> int:
        # handle rounding of bounds
        if round(borne, -2) <= borne_inf or round(borne, -2) >= borne_sup:
            return round(borne)
        else:
            return round(borne, -2)

    def distrib_from_prix(
        prix: pd.Series,
        nb_tranches: int = 10,
        arrondi: bool = True,
    ) -> tuple[list, list]:
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
        # handle case where rounding creates identical bins
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
                for _ in range(b):
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
        usecols=['code_geo', 'echelle_geo', 'code_parent', 'libelle_geo'],
        dtype=str,
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
    dvf = []
    epci = pd.read_csv(
        DATADIR + "/epci.csv",
        sep=",",
        encoding="utf8",
        dtype=str,
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
        logging.info(f"Starting with {year}")
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

        # filtres sur les ventes et les types de biens à considérer
        # choix : les terres et/ou dépendances ne rendent pas une mutation
        # multitype
        ventes = df.loc[
            (df["nature_mutation"].isin(natures_of_interest))
            & (df["code_type_local"].isin([1, 2, 4]))
        ]
        del df

        # on ne garde que les ventes d'un seul bien
        # cf historique pour les ventes multi-types
        count_ventes = ventes['id_mutation'].value_counts().reset_index()
        liste_ventes_monobien = count_ventes.loc[count_ventes["count"] == 1, "id_mutation"]
        ventes_nodup = ventes.loc[ventes['id_mutation'].isin(liste_ventes_monobien)]

        ventes_nodup["prix_m2"] = ventes_nodup["valeur_fonciere"] / ventes_nodup["surface_reelle_bati"]
        ventes_nodup["prix_m2"] = ventes_nodup["prix_m2"].replace([np.inf, -np.inf], np.nan)

        # pas de prix ou pas de surface
        ventes_nodup = ventes_nodup.dropna(subset=["prix_m2"])
        dvf.append(ventes_nodup)
        del ventes_nodup

    dvf = pd.concat(dvf, ignore_index=True)
    dvf = pd.merge(
        dvf,
        epci[['code_commune', 'code_epci']],
        on="code_commune",
        how="left",
    )
    # bool is for distribution calculation
    echelles_of_interest = {
        "departement": True,
        "epci": True,
        "commune": True,
        "section": False,
    }
    types_of_interest = {
        'appartement': [2],
        'maison': [1],
        'apt_maison': [1, 2],
        'local': [4],
    }
    dvf = dvf[['code_' + e for e in echelles_of_interest] + ['code_type_local', 'prix_m2']]
    threshold = 100
    tranches = []
    stats_period = []
    for t in types_of_interest:
        restr_type_dvf = dvf.loc[
            dvf['code_type_local'].isin(types_of_interest[t])
        ].drop('code_type_local', axis=1)
        # échelle nationale
        intervalles, volumes = distrib_from_prix(restr_type_dvf['prix_m2'])
        tranches.append({
            'code_geo': 'nation',
            'type_local': t,
            'xaxis': intervalles,
            'yaxis': volumes
        })
        # autres échelles
        type_stats = [pd.DataFrame([{
            'code_geo': 'nation',
            f'nb_ventes_whole_{t}': len(restr_type_dvf),
            f'moy_prix_m2_whole_{t}': restr_type_dvf['prix_m2'].mean(),
            f'med_prix_m2_whole_{t}': restr_type_dvf['prix_m2'].median(),
        }])]
        for e in echelles_of_interest:
            logging.info(f"Starting {e} {t}")
            # stats
            grouped = restr_type_dvf[[f'code_{e}', 'prix_m2']].groupby(f'code_{e}')
            nb = grouped.count().reset_index()
            nb.columns = ['code_geo', f'nb_ventes_whole_{t}']
            mean = grouped.mean().reset_index()
            mean.columns = ['code_geo', f'moy_prix_m2_whole_{t}']
            median = grouped.median().reset_index()
            median.columns = ['code_geo', f'med_prix_m2_whole_{t}']
            merged = pd.merge(nb, mean, on='code_geo')
            merged = pd.merge(merged, median, on='code_geo')
            type_stats.append(merged)
            logging.info("- Done with stats")

            # distribution
            if echelles_of_interest[e]:
                codes_geo = set(echelles.loc[echelles['echelle_geo'] == e, 'code_geo'])
                restr_dvf = restr_type_dvf[[f'code_{e}', 'prix_m2']].set_index(f'code_{e}')['prix_m2']
                idx = set(restr_dvf.index)
                operations = len(codes_geo)
                for i, code in enumerate(codes_geo):
                    if i % (operations // 10) == 0 and i > 0:
                        logging.info(f"{int(round(i / operations * 100, -1))}%")
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
                                'yaxis': volumes,
                            })
                        else:
                            tranches.append({
                                'code_geo': code,
                                'type_local': t,
                                'xaxis': None,
                                'yaxis': None,
                            })
                    else:
                        tranches.append({
                            'code_geo': code,
                            'type_local': t,
                            'xaxis': None,
                            'yaxis': None,
                        })
                logging.info("- Done with distribution")
            else:
                logging.info("- No distribution")
        stats_period.append(pd.concat(type_stats))
    output_tranches = pd.DataFrame(tranches)
    output_tranches.to_csv(
        DATADIR + "/distribution_prix.csv",
        sep=",",
        encoding="utf8",
        index=False,
    )
    logging.info("Done exporting distribution")
    stats_period = reduce(lambda x, y: pd.merge(x, y, on='code_geo', how='outer'), stats_period)
    stats_period = pd.merge(echelles, stats_period, on='code_geo', how='outer')
    logging.info("Check répartition échelles")
    logging.info("Réel")
    logging.info(echelles['echelle_geo'].value_counts(dropna=False))
    logging.info("Dans stats")
    logging.info(stats_period['echelle_geo'].value_counts(dropna=False))
    stats_period.to_csv(
        DATADIR + "/stats_whole_period.csv",
        sep=",",
        encoding="utf8",
        index=False,
        float_format='%.0f',
    )


def send_stats_to_minio() -> None:
    minio_open.send_files(
        list_files=[
            File(
                source_path=f"{DATADIR}/",
                source_name=f"{file}.csv",
                dest_path="dvf/",
                dest_name=f"{file}.csv",
            ) for file in ["stats_dvf", "stats_whole_period"]
        ],
    )


def send_distribution_to_minio() -> None:
    minio_restricted.send_files(
        list_files=[
            File(
                source_path=f"{DATADIR}/",
                source_name="distribution_prix.csv",
                dest_path="dvf/",
                dest_name="distribution_prix.csv",
            ),
        ],
    )


def publish_stats_dvf(ti) -> None:
    with open(f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}dvf/config/dgv.json") as fp:
        data = json.load(fp)
    post_remote_resource(
        dataset_id=data["mensuelles"][AIRFLOW_ENV]["dataset_id"],
        resource_id=data["mensuelles"][AIRFLOW_ENV]["resource_id"],
        payload={
            "url": (
                f"https://object.files.data.gouv.fr/{MINIO_BUCKET_DATA_PIPELINE_OPEN}"
                f"/{AIRFLOW_ENV}/dvf/stats_dvf.csv"
            ),
            "filesize": os.path.getsize(os.path.join(DATADIR, "stats_dvf.csv")),
            "title": "Statistiques mensuelles DVF",
            "format": "csv",
            "description": (
                "Statistiques mensuelles sur les données DVF"
                f" (dernière modification : {datetime.today()})"
            ),
        },
    )
    logging.info("Done with stats mensuelles")
    post_remote_resource(
        dataset_id=data["totales"][AIRFLOW_ENV]["dataset_id"],
        resource_id=data["totales"][AIRFLOW_ENV]["resource_id"],
        payload={
            "url": (
                f"https://object.files.data.gouv.fr/{MINIO_BUCKET_DATA_PIPELINE_OPEN}"
                f"/{AIRFLOW_ENV}/dvf/stats_whole_period.csv"
            ),
            "filesize": os.path.getsize(os.path.join(DATADIR, "stats_whole_period.csv")),
            "title": "Statistiques totales DVF",
            "format": "csv",
            "description": (
                "Statistiques sur 5 ans sur les données DVF"
                f" (dernière modification : {datetime.today()})"
            ),
        },
    )
    ti.xcom_push(key="dataset_id", value=data['mensuelles'][AIRFLOW_ENV]["dataset_id"])


def notification_mattermost(ti) -> None:
    dataset_id = ti.xcom_pull(key="dataset_id", task_ids="publish_stats_dvf")
    send_message(
        f"Stats DVF générées :"
        f"\n- intégré en base de données"
        f"\n- publié [sur {'demo.' if AIRFLOW_ENV == 'dev' else ''}data.gouv.fr]"
        f"({DATAGOUV_URL}/fr/datasets/{dataset_id})"
        f"\n- données upload [sur Minio]({MINIO_URL}/buckets/{MINIO_BUCKET_DATA_PIPELINE_OPEN}/browse)"
    )
