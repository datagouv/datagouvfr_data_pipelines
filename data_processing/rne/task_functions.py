from datagouvfr_data_pipelines.utils.download import download_files
import os
import pandas as pd
import json
import zipfile
from datagouvfr_data_pipelines.utils.mattermost import send_message
import logging
from datagouvfr_data_pipelines.utils.minio import send_files
from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
    MINIO_URL,
    MINIO_BUCKET_DATA_PIPELINE,
    SECRET_MINIO_DATA_PIPELINE_USER,
    SECRET_MINIO_DATA_PIPELINE_PASSWORD,
)

DAG_FOLDER = "datagouvfr_data_pipelines/data_processing/"
TMP_FOLDER = f"{AIRFLOW_DAG_TMP}rne/"
DATADIR = f"{TMP_FOLDER}data"
ZIP_FILE_PATH = f"{TMP_FOLDER}rne.zip"
EXTRACTED_FILES_PATH = f"{TMP_FOLDER}extracted/"


def get_rne_stock():
    download_files(
        [
            {
                "url": "https://www.inpi.fr/sites/default/files/rne/stock_rne.zip",
                "dest_path": f"{TMP_FOLDER}",
                "dest_name": "rne.zip",
            },
        ]
    )


def unzip_files():
    with zipfile.ZipFile(ZIP_FILE_PATH, mode="r") as z:
        z.extractall(EXTRACTED_FILES_PATH)


def process_rne_files(**kwargs):
    list_all_dirig_pm = []
    list_all_dirig_pp = []

    # Process each JSON file in the directory
    for filename in os.listdir(EXTRACTED_FILES_PATH):
        if filename.endswith(".json"):
            file_path = os.path.join(EXTRACTED_FILES_PATH, filename)
            with open(file_path, "r", encoding="utf-8") as json_file:
                logging.info(f"%%%%%%%%%%%%{file_path}")
                json_data = json.load(json_file)
                list_dirigeants_pp, list_dirigeants_pm = extract_dirigeants_data(
                    json_data
                )
                # Concatenate dirigeants_pp to the bigger list dirigeants
                list_all_dirig_pp.extend(list_dirigeants_pp)
                list_all_dirig_pm.extend(list_dirigeants_pm)

    # Convert lists to dataframes
    df_dirig_pp = pd.DataFrame(list_all_dirig_pp)
    df_dirig_pm = pd.DataFrame(list_all_dirig_pm)

    # Clean dataframes
    clean_df_dirig_pp, clean_df_dirig_pm = clean_dirigeants_rna(
        df_dirig_pp, df_dirig_pm
    )

    count_dirigeants_pp = clean_df_dirig_pp.shape[0]
    count_dirigeants_pm = df_dirig_pm.shape[0]

    kwargs["ti"].xcom_push(key="count_dirigeants_pp", value=count_dirigeants_pp)
    kwargs["ti"].xcom_push(key="count_dirigeants_pm", value=count_dirigeants_pm)
    logging.info(f"********* Count dirigeants pp: {count_dirigeants_pp}")
    logging.info(f"********* Count dirigeants pm: {count_dirigeants_pm}")

    if not os.path.exists(DATADIR):
        logging.info(f"**********Creating {DATADIR}")
        os.makedirs(DATADIR)
    # export to csv
    clean_df_dirig_pp.to_csv(
        os.path.join(DATADIR, "dirigeants_pp.csv"), index=False, encoding="utf8"
    )
    clean_df_dirig_pm.to_csv(
        os.path.join(DATADIR, "dirigeants_pm.csv"), index=False, encoding="utf8"
    )


def extract_dirigeants_data(json_data):
    """Extract and categorize "dirigeants" by type."""
    list_dirigeants_pp = []
    list_dirigeants_pm = []

    for entity in json_data:
        siren = entity.get("siren")
        date_maj = entity.get("updatedAt")
        dirigeants = (
            entity.get("formality", {})
            .get("content", {})
            .get("personneMorale", {})
            .get("composition", {})
            .get("pouvoirs", [])
        )

        for dirigeant in dirigeants:
            type_de_personne = dirigeant.get("typeDePersonne", None)

            if type_de_personne == "INDIVIDU":
                individu = dirigeant.get("individu", {}).get("descriptionPersonne", {})
                adresse_domicile = dirigeant.get("individu", {}).get(
                    "adresseDomicile", {}
                )

                dirigeant_pp = {
                    "siren": siren,
                    "date_mise_a_jour": date_maj,
                    "date_de_naissance": individu.get("dateDeNaissance", None),
                    "role": individu.get("role", None),
                    "nom": individu.get("nom", None),
                    "nom_usage": individu.get("nomUsage", None),
                    "prenoms": individu.get("prenoms", None),
                    "genre": individu.get("genre", None),
                    "nationalite": individu.get("nationalite", None),
                    "situation_matrimoniale": individu.get(
                        "situationMatrimoniale", None
                    ),
                    "pays": adresse_domicile.get("pays", None),
                    "code_pays": adresse_domicile.get("codePays", None),
                    "code_postal": adresse_domicile.get("codePostal", None),
                    "commune": adresse_domicile.get("commune", None),
                    "code_insee_commune": adresse_domicile.get(
                        "codeInseeCommune", None
                    ),
                    "voie": adresse_domicile.get("voie", None),
                }

                list_dirigeants_pp.append(dirigeant_pp)

            elif type_de_personne == "ENTREPRISE":
                entreprise = dirigeant.get("entreprise", {})
                adresse_entreprise = dirigeant.get("adresseEntreprise", {})

                dirigeant_pm = {
                    "siren": siren,
                    "date_mise_a_jour": date_maj,
                    "denomination": entreprise.get("denomination", None),
                    "siren_dirigeant": entreprise.get("siren", None),
                    "role": entreprise.get("roleEntreprise", None),
                    "forme_juridique": entreprise.get("formeJuridique", None),
                    "pays": adresse_entreprise.get("pays", None),
                    "code_pays": adresse_entreprise.get("codePays", None),
                    "code_postal": adresse_entreprise.get("codePostal", None),
                    "commune": adresse_entreprise.get("commune", None),
                    "code_insee_commune": adresse_entreprise.get(
                        "codeInseeCommune", None
                    ),
                    "voie": adresse_entreprise.get("voie", None),
                }

                list_dirigeants_pm.append(dirigeant_pm)
    return list_dirigeants_pp, list_dirigeants_pm


def clean_dirigeants_rna(df_dirigeants_pp, df_dirigeants_pm):
    df_dirigeants_pp["prenoms"] = df_dirigeants_pp["prenoms"].apply(
        lambda x: " ".join(x) if x is not None else None
    )
    # Sort the DataFrame by 'siren' and 'role'
    df_dirigeants_pp = df_dirigeants_pp.sort_values(
        by=["siren", "nom", "prenoms", "role"]
    )
    # Drop Duplicates
    df_dirigeants_pp = df_dirigeants_pp.drop_duplicates(
        subset=["siren", "nom", "prenoms", "date_de_naissance"], keep="first"
    )

    df_dirigeants_pm = df_dirigeants_pm.sort_values(
        by=["siren", "denomination", "siren_dirigeant", "role"]
    )

    df_dirigeants_pm = df_dirigeants_pm.drop_duplicates(
        subset=["siren", "siren_dirigeant", "denomination"], keep="first"
    )

    return df_dirigeants_pp, df_dirigeants_pm


def send_rne_to_minio():
    logging.info("Saving files in MinIO.....")
    send_files(
        MINIO_URL=MINIO_URL,
        MINIO_BUCKET=MINIO_BUCKET_DATA_PIPELINE,
        MINIO_USER=SECRET_MINIO_DATA_PIPELINE_USER,
        MINIO_PASSWORD=SECRET_MINIO_DATA_PIPELINE_PASSWORD,
        list_files=[
            {
                "source_path": f"{DATADIR}/",
                "source_name": "dirigeants_pp.csv",
                "dest_path": "rne/",
                "dest_name": "dirigeants_pp.csv",
            },
            {
                "source_path": f"{DATADIR}/",
                "source_name": "dirigeants_pm.csv",
                "dest_path": "rne/",
                "dest_name": "dirigeants_pm.csv",
            },
        ],
    )


def send_notification_mattermost(**kwargs):
    dirig_pp = kwargs["ti"].xcom_pull(
        key="count_dirigeants_pp", task_ids="process_files"
    )
    dirig_pm = kwargs["ti"].xcom_pull(
        key="count_dirigeants_pm", task_ids="process_files"
    )
    send_message(
        f"Données stock RNE mise à jour sur Minio "
        f"- Bucket {MINIO_BUCKET_DATA_PIPELINE} :"
        f"\n - Nombre dirigeants pp : {dirig_pp} "
        f"\n - Nombre dirigeants pm : {dirig_pm} "
    )
