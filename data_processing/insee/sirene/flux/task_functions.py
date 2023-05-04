from datetime import datetime
import gzip
import pandas as pd
import requests
from datagouvfr_data_pipelines.utils.minio import send_files
from datagouvfr_data_pipelines.utils.mattermost import send_message
from datagouvfr_data_pipelines.config import (
    SECRET_INSEE_BEARER,
    AIRFLOW_DAG_TMP,
    MINIO_URL,
    MINIO_BUCKET_DATA_PIPELINE,
    SECRET_MINIO_DATA_PIPELINE_USER,
    SECRET_MINIO_DATA_PIPELINE_PASSWORD,
)

CURRENT_MONTH = datetime.today().strftime("%Y-%m")


def flatten_dict(dd, separator="_", prefix=""):
    return (
        {
            prefix + separator + k if prefix else k: v
            for kk, vv in dd.items()
            for k, v in flatten_dict(vv, separator, kk).items()
        }
        if isinstance(dd, dict)
        else {prefix: dd}
    )


def call_insee_api(endpoint, property):
    cursor = "*"
    data = []
    headers = {"Authorization": f"Bearer {SECRET_INSEE_BEARER}"}
    toContinue = True
    cpt = 0
    while toContinue:
        cpt += 1000
        if cpt % 10000 == 0:
            print(cpt)

        res = requests.get(endpoint + cursor, headers=headers).json()
        if (
            "curseurSuivant" in res["header"]
            and "curseur" in res["header"]
            and res["header"]["curseur"] != res["header"]["curseurSuivant"]
        ):
            cursor = res["header"]["curseurSuivant"]
        else:
            toContinue = False
        data = data + res[property]
    return data


def get_stock_non_diffusible(ti):
    endpoint = (
        "https://api.insee.fr/entreprises/sirene/V3/siret"
        "?q=statutDiffusionUniteLegale%3AP"
        "&champs=siren&nombre=1000&curseur="
    )
    data = call_insee_api(endpoint, "etablissements")
    df = pd.DataFrame(data)
    df.to_csv(
        f"{AIRFLOW_DAG_TMP}sirene_flux/stock_non_diffusible.csv.gz", index=False, compression="gzip"
    )
    ti.xcom_push(key="nb_stock_non_diffusible", value=str(df.shape[0]))


def get_current_flux_non_diffusible(ti):
    endpoint = (
        "https://api.insee.fr/entreprises/sirene/V3/siren"
        "?q=statutDiffusionUniteLegale%3AP"
        "%20AND%20dateDernierTraitementUniteLegale%3A"
        f"{CURRENT_MONTH}&champs=siren&nombre=1000&curseur="
    )
    data = call_insee_api(endpoint, "unitesLegales")
    df = pd.DataFrame(data)
    df.to_csv(
        f"{AIRFLOW_DAG_TMP}sirene_flux/flux_non_diffusible_{CURRENT_MONTH}.csv.gz",
        index=False,
        compression="gzip"
    )
    ti.xcom_push(key="nb_flux_non_diffusible", value=str(df.shape[0]))


def get_current_flux_unite_legale(ti):
    endpoint = (
        "https://api.insee.fr/entreprises/sirene/V3/siren?q=dateDernierTraitementUniteLegale%3A"
        f"{CURRENT_MONTH}"
        "&champs=siren,dateCreationUniteLegale,sigleUniteLegale,prenomUsuelUniteLegale,"
        "identifiantAssociationUniteLegale,trancheEffectifsUniteLegale,dateDernierTraitementUniteLegale,"
        "categorieEntreprise,etatAdministratifUniteLegale,nomUniteLegale,nomUsageUniteLegale,"
        "denominationUniteLegale,denominationUsuelle1UniteLegale,denominationUsuelle2UniteLegale,"
        "denominationUsuelle3UniteLegale,categorieJuridiqueUniteLegale,activitePrincipaleUniteLegale,"
        "economieSocialeSolidaireUniteLegale,statutDiffusionUniteLegale,societeMissionUniteLegale"
        "&nombre=1000&curseur="
    )
    data = call_insee_api(endpoint, "unitesLegales")
    flux = []
    for d in data:
        row = d.copy()
        if "periodesUniteLegale" in row:
            for item in row["periodesUniteLegale"][0]:
                row[item] = row["periodesUniteLegale"][0][item]
            del row["periodesUniteLegale"]
        flux.append(flatten_dict(row))

    df = pd.DataFrame(flux)
    df.to_csv(
        f"{AIRFLOW_DAG_TMP}sirene_flux/flux_unite_legale_{CURRENT_MONTH}.csv.gz",
        index=False,
        compression="gzip",
    )
    ti.xcom_push(key="nb_flux_unite_legale", value=str(df.shape[0]))


def get_current_flux_etablissement(ti):
    endpoint = (
        "https://api.insee.fr/entreprises/sirene/V3/siret?q=dateDernierTraitementEtablissement%3A"
        f"{CURRENT_MONTH}&champs=siren,siret,dateCreationEtablissement,trancheEffectifsEtablissement,"
        "activitePrincipaleRegistreMetiersEtablissement,etablissementSiege,numeroVoieEtablissement,"
        "libelleVoieEtablissement,codePostalEtablissement,libelleCommuneEtablissement,"
        "libelleCedexEtablissement,typeVoieEtablissement,codeCommuneEtablissement,codeCedexEtablissement,"
        "complementAdresseEtablissement,distributionSpecialeEtablissement,complementAdresse2Etablissement,"
        "indiceRepetition2Etablissement,libelleCedex2Etablissement,codeCedex2Etablissement,"
        "numeroVoie2Etablissement,typeVoie2Etablissement,libelleVoie2Etablissement,"
        "codeCommune2Etablissement,libelleCommune2Etablissement,distributionSpeciale2Etablissement,"
        "dateDebut,etatAdministratifEtablissement,enseigne1Etablissement,enseigne1Etablissement,"
        "enseigne2Etablissement,enseigne3Etablissement,denominationUsuelleEtablissement,"
        "activitePrincipaleEtablissement,indiceRepetitionEtablissement,libelleCommuneEtrangerEtablissement,"
        "codePaysEtrangerEtablissement,libellePaysEtrangerEtablissement,"
        "libelleCommuneEtranger2Etablissement,codePaysEtranger2Etablissement,"
        "libellePaysEtranger2Etablissement&nombre=1000&curseur="
    )
    data = call_insee_api(endpoint, "etablissements")
    flux = []
    for d in data:
        row = d.copy()
        if "periodesEtablissement" in row:
            for item in row["periodesEtablissement"][0]:
                row[item] = row["periodesEtablissement"][0][item]
            del row["periodesEtablissement"]
        flux.append(flatten_dict(row))

    data.clear()
    
    # We save csv.gz by batch of 100 000 for memory
    df = pd.DataFrame(columns=[c for c in flux[0]])
    for column in df.columns:
        for prefix in ["adresseEtablissement_", "adresse2Etablissement_"]:
            if prefix in column:
                df = df.rename(columns={column: column.replace(prefix, "")})
    df.to_csv(
        f"{AIRFLOW_DAG_TMP}sirene_flux/flux_etablissement_{CURRENT_MONTH}.csv",
        index=False
    )
    first = 0
    for i in range(len(flux)):
        if i != 0 and i % 100000 == 0:
            fluxinter = flux[first:i]
            df = pd.DataFrame(fluxinter)
            df.to_csv(
                f"{AIRFLOW_DAG_TMP}sirene_flux/flux_etablissement_{CURRENT_MONTH}.csv",
                mode="a",
                index=False,
                header=False
            )
            first = i
    
    fluxinter = flux[first:len(flux)]
    df = pd.DataFrame(fluxinter)
    df.to_csv(
        f"{AIRFLOW_DAG_TMP}sirene_flux/flux_etablissement_{CURRENT_MONTH}.csv",
        mode="a",
        index=False,
        header=False
    )

    with open(
        f"{AIRFLOW_DAG_TMP}sirene_flux/flux_etablissement_{CURRENT_MONTH}.csv",
        "rb"
    ) as orig_file:
        with gzip.open(
            f"{AIRFLOW_DAG_TMP}sirene_flux/flux_etablissement_{CURRENT_MONTH}.csv.gz",
            "wb"
        ) as zipped_file:
            zipped_file.writelines(orig_file)

    ti.xcom_push(key="nb_flux_etablissement", value=str(df.shape[0]))


def send_flux_minio():
    send_files(
        MINIO_URL=MINIO_URL,
        MINIO_BUCKET=MINIO_BUCKET_DATA_PIPELINE,
        MINIO_USER=SECRET_MINIO_DATA_PIPELINE_USER,
        MINIO_PASSWORD=SECRET_MINIO_DATA_PIPELINE_PASSWORD,
        list_files=[
            {
                "source_path": f"{AIRFLOW_DAG_TMP}sirene_flux/",
                "source_name": f"flux_unite_legale_{CURRENT_MONTH}.csv.gz",
                "dest_path": "insee/sirene/sirene_flux/",
                "dest_name": f"flux_unite_legale_{CURRENT_MONTH}.csv.gz",
            },
            {
                "source_path": f"{AIRFLOW_DAG_TMP}sirene_flux/",
                "source_name": f"flux_etablissement_{CURRENT_MONTH}.csv.gz",
                "dest_path": "insee/sirene/sirene_flux/",
                "dest_name": f"flux_etablissement_{CURRENT_MONTH}.csv.gz",
            },
            {
                "source_path": f"{AIRFLOW_DAG_TMP}sirene_flux/",
                "source_name": f"flux_non_diffusible_{CURRENT_MONTH}.csv.gz",
                "dest_path": "insee/sirene/sirene_flux/",
                "dest_name": f"flux_non_diffusible_{CURRENT_MONTH}.csv.gz",
            }
        ],
    )


def send_stock_minio():
    send_files(
        MINIO_URL=MINIO_URL,
        MINIO_BUCKET=MINIO_BUCKET_DATA_PIPELINE,
        MINIO_USER=SECRET_MINIO_DATA_PIPELINE_USER,
        MINIO_PASSWORD=SECRET_MINIO_DATA_PIPELINE_PASSWORD,
        list_files=[
            {
                "source_path": f"{AIRFLOW_DAG_TMP}sirene_flux/",
                "source_name": "stock_non_diffusible.csv.gz",
                "dest_path": "insee/sirene/sirene_flux/",
                "dest_name": "stock_non_diffusible.csv.gz",
            }
        ],
    )


def send_notification(ti):
    nb_stock_non_diffusible = ti.xcom_pull(
        key="nb_stock_non_diffusible", task_ids="get_stock_non_diffusible"
    )
    nb_flux_non_diffusible = ti.xcom_pull(
        key="nb_flux_non_diffusible", task_ids="get_current_flux_non_diffusible"
    )
    nb_flux_unite_legale = ti.xcom_pull(
        key="nb_flux_unite_legale", task_ids="get_current_flux_unite_legale"
    )
    nb_flux_etablissement = ti.xcom_pull(
        key="nb_flux_etablissement", task_ids="get_current_flux_etablissement"
    )
    send_message(
        text=(
            "Données Flux Sirene mise à jour - Disponible sur le Minio - Bucket "
            f"{MINIO_BUCKET_DATA_PIPELINE}\n"
            f"- {nb_stock_non_diffusible} unités légales non diffusibles\n"
            f"- {nb_flux_non_diffusible} unités légales non diffusible modifiées ce mois-ci\n"
            f"- {nb_flux_unite_legale}  unités légales modifiés ce mois-ci\n"
            f"- {nb_flux_etablissement} établissements modifiés ce mois-ci"
        )
    )
