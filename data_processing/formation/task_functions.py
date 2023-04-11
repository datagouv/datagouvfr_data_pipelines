import datetime
import json
import pandas as pd
import requests

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_HOME,
    AIRFLOW_DAG_TMP,
    DATAGOUV_URL,
    MINIO_URL,
    MINIO_BUCKET_DATA_PIPELINE_OPEN,
    SECRET_MINIO_DATA_PIPELINE_USER,
    SECRET_MINIO_DATA_PIPELINE_PASSWORD,
)
from datagouvfr_data_pipelines.utils.minio import send_files, compare_files
from datagouvfr_data_pipelines.utils.mattermost import send_message


def download_latest_data(ti):
    requests.packages.urllib3.disable_warnings()
    requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS += ":HIGH:!DH:!aNULL"
    try:
        requests.packages.urllib3.contrib.pyopenssl.util.ssl_.DEFAULT_CIPHERS += (
            ":HIGH:!DH:!aNULL"
        )
    except AttributeError:
        pass

    with open(
        f"{AIRFLOW_DAG_HOME}datagouvfr_data_pipelines/data_processing/formation/config/resource.json",
        "r",
    ) as fp:
        res = json.load(fp)

    with requests.get(
        res["resource_id"],
        verify=False,  # Check if SSL is restored
        stream=True,
    ) as r:
        with open(f"{AIRFLOW_DAG_TMP}formation/{res['name']}.csv", "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

    ti.xcom_push(key="resource", value=res)


def concat_spe(row):
    return [x for x in [row["spe1"], row["spe2"], row["spe3"]] if x == x]


def convert_date(val):
    if val == val:
        return datetime.datetime.strptime(val, "%d/%m/%Y").strftime("%Y-%m-%d")
    return None


def process_organismes_formation(ti):
    res = ti.xcom_pull(key="resource", task_ids="download_latest_data")
    df = pd.read_csv(
        f"{AIRFLOW_DAG_TMP}formation/{res['name']}.csv", sep=";", dtype=str
    )
    df = df.rename(
        columns={
            "Numéro Déclaration Activité": "id_nda",
            "Code SIREN": "siren",
            "Siret Etablissement Déclarant": "siret",
            "Actions de formations": "cert_adf",
            "Bilans de compétences": "cert_bdc",
            "VAE": "cert_vae",
            "Actions de formations par apprentissage": "cert_app",
            "Date dernière déclaration": "date_derniere_declaration",
            "Début d'exercice": "date_debut_exercice",
            "Fin d'exercice": "date_fin_exercice",
            "Code Spécialité 1": "spe1",
            "Code Spécialité 2": "spe2",
            "Code Spécialité 3": "spe3",
            "Nombre de stagiaires": "nb_stagiaires",
            "Nombre de stagiaires confiés par un autre Organisme de formation": "nb_stagiaires_autres_of",
            "Effectifs de formateurs": "nb_formateurs",
            "Certifications": "certifications",
        }
    )
    df = df[
        [
            "id_nda",
            "denomination",
            "siren",
            "siret",
            "cert_adf",
            "cert_bdc",
            "cert_vae",
            "cert_app",
            "date_derniere_declaration",
            "date_debut_exercice",
            "date_fin_exercice",
            "spe1",
            "spe2",
            "spe3",
            "nb_stagiaires",
            "nb_stagiaires_autres_of",
            "nb_formateurs",
            "certifications",
        ]
    ]
    df["spe"] = df.apply(lambda row: concat_spe(row), axis=1)
    df["date_derniere_declaration"] = df["date_derniere_declaration"].apply(
        lambda x: convert_date(x)
    )
    df["date_debut_exercice"] = df["date_debut_exercice"].apply(
        lambda x: convert_date(x)
    )
    df["date_fin_exercice"] = df["date_fin_exercice"].apply(lambda x: convert_date(x))

    df = df.drop(["spe1", "spe2", "spe3"], axis=1)

    df.to_csv(f"{AIRFLOW_DAG_TMP}formation/{res['name']}_clean.csv", index=False)

    ti.xcom_push(key="nb_of", value=str(df["id_nda"].nunique()))
    ti.xcom_push(key="nb_siret", value=str(df["siret"].nunique()))


def send_file_to_minio(ti):
    res = ti.xcom_pull(key="resource", task_ids="download_latest_data")
    send_files(
        MINIO_URL=MINIO_URL,
        MINIO_BUCKET=MINIO_BUCKET_DATA_PIPELINE_OPEN,
        MINIO_USER=SECRET_MINIO_DATA_PIPELINE_USER,
        MINIO_PASSWORD=SECRET_MINIO_DATA_PIPELINE_PASSWORD,
        list_files=[
            {
                "source_path": f"{AIRFLOW_DAG_TMP}formation/",
                "source_name": f"{res['name']}_clean.csv",
                "dest_path": "formation/new/",
                "dest_name": f"{res['name']}_clean.csv",
            }
        ],
    )


def compare_files_minio(ti):
    res = ti.xcom_pull(key="resource", task_ids="download_latest_data")
    is_same = compare_files(
        MINIO_URL=MINIO_URL,
        MINIO_BUCKET=MINIO_BUCKET_DATA_PIPELINE_OPEN,
        MINIO_USER=SECRET_MINIO_DATA_PIPELINE_USER,
        MINIO_PASSWORD=SECRET_MINIO_DATA_PIPELINE_PASSWORD,
        file_path_1="formation/new/",
        file_name_2=f"{res['name']}_clean.csv",
        file_path_2="formation/latest/",
        file_name_1=f"{res['name']}_clean.csv",
    )
    if is_same:
        return False

    if is_same is None:
        print("First time in this Minio env. Creating")

    send_files(
        MINIO_URL=MINIO_URL,
        MINIO_BUCKET=MINIO_BUCKET_DATA_PIPELINE_OPEN,
        MINIO_USER=SECRET_MINIO_DATA_PIPELINE_USER,
        MINIO_PASSWORD=SECRET_MINIO_DATA_PIPELINE_PASSWORD,
        list_files=[
            {
                "source_path": f"{AIRFLOW_DAG_TMP}formation/",
                "source_name": f"{res['name']}_clean.csv",
                "dest_path": "formation/latest/",
                "dest_name": f"{res['name']}_clean.csv",
            }
        ],
    )

    return True


def send_notification(ti):
    nb_of = ti.xcom_pull(key="nb_of", task_ids="process_organismes_formation")
    nb_siret = ti.xcom_pull(key="nb_siret", task_ids="process_organismes_formation")
    send_message(
        text=(
            ":mega: Données organismes formations mises à jour.\n"
            f"- {nb_of} organismes de formation référencés\n"
            f"- {nb_siret} établissements (siret) représentés\n"
            f"- Données stockées sur Minio - Bucket {MINIO_BUCKET_DATA_PIPELINE_OPEN}"
        )
    )
