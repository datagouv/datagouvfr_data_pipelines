from datetime import datetime
import pandas as pd
from airflow.decorators import task

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
    S3_BUCKET_DATA_PIPELINE_OPEN,
)
from datagouvfr_data_pipelines.utils.download import download_files
from datagouvfr_data_pipelines.utils.filesystem import File
from datagouvfr_data_pipelines.utils.s3 import S3Client
from datagouvfr_data_pipelines.utils.mattermost import send_message
from datagouvfr_data_pipelines.utils.datagouv import local_client

s3_open = S3Client(bucket=S3_BUCKET_DATA_PIPELINE_OPEN)

TMP_FOLDER = f"{AIRFLOW_DAG_TMP}formation/"


@task()
def download_latest_data(**context):
    config = {
        "resource_id": "ac59a0f5-fa83-4b82-bf12-3c5806d4f19f",
        "name": "organismes_formation",
    }
    download_files(
        list_urls=[
            File(
                url=f"{local_client.base_url}/api/1/datasets/r/{config['resource_id']}",
                dest_path=TMP_FOLDER,
                dest_name=f"{config['name']}.csv",
            )
        ]
    )
    context["ti"].xcom_push(key="resource", value=config)


def concat_spe(row):
    return [x for x in [row["spe1"], row["spe2"], row["spe3"]] if isinstance(x, str)]


def convert_date(val):
    if isinstance(val, str):
        return datetime.strptime(val, "%d/%m/%Y").strftime("%Y-%m-%d")
    return None


@task()
def process_organismes_formation(**context):
    res = context["ti"].xcom_pull(key="resource", task_ids="download_latest_data")
    df = pd.read_csv(
        f"{AIRFLOW_DAG_TMP}formation/{res['name']}.csv",
        sep=";",
        dtype=str,
    )
    mapping = {
        "numeroDeclarationActivite": "id_nda",
        "denomination": "denomination",
        "siren": "siren",
        "siretEtablissementDeclarant": "siret",
        "certifications.actionsDeFormation": "cert_adf",
        "certifications.bilansDeCompetences": "cert_bdc",
        "certifications.VAE": "cert_vae",
        "certifications.actionsDeFormationParApprentissage": "cert_app",
        "informationsDeclarees.dateDerniereDeclaration": "date_derniere_declaration",
        "informationsDeclarees.debutExercice": "date_debut_exercice",
        "informationsDeclarees.finExercice": "date_fin_exercice",
        "informationsDeclarees.specialitesDeFormation.codeSpecialite1": "spe1",
        "informationsDeclarees.specialitesDeFormation.codeSpecialite2": "spe2",
        "informationsDeclarees.specialitesDeFormation.codeSpecialite3": "spe3",
        "informationsDeclarees.nbStagiaires": "nb_stagiaires",
        "informationsDeclarees.nbStagiairesConfiesParUnAutreOF": "nb_stagiaires_autres_of",
        "informationsDeclarees.effectifFormateurs": "nb_formateurs",
    }
    df = df.rename(columns=mapping)
    df = df[list(mapping.values())]
    df["spe"] = df.apply(lambda row: concat_spe(row), axis=1)

    for col in [
        "date_derniere_declaration",
        "date_debut_exercice",
        "date_fin_exercice",
    ]:
        df[col] = df[col].apply(lambda x: convert_date(x))

    df = df.drop(["spe1", "spe2", "spe3"], axis=1)

    df.to_csv(f"{TMP_FOLDER}{res['name']}_clean.csv", index=False)

    context["ti"].xcom_push(key="nb_of", value=str(df["id_nda"].nunique()))
    context["ti"].xcom_push(key="nb_siret", value=str(df["siret"].nunique()))


@task()
def send_file_to_s3(**context):
    res = context["ti"].xcom_pull(key="resource", task_ids="download_latest_data")
    s3_open.send_file(
        File(
            source_path=TMP_FOLDER,
            source_name=f"{res['name']}_clean.csv",
            dest_path="formation/new/",
            dest_name=f"{res['name']}_clean.csv",
        )
    )


def compare_files_s3(**context):
    res = context["ti"].xcom_pull(key="resource", task_ids="download_latest_data")
    is_same = s3_open.are_files_identical(
        file_1=File(
            source_path="formation/new/",
            source_name=f"{res['name']}_clean.csv",
            remote_source=True,
        ),
        file_2=File(
            source_path="formation/latest/",
            source_name=f"{res['name']}_clean.csv",
            remote_source=True,
        ),
    )
    if is_same:
        return False

    if is_same is None:
        print("First time in this S3 env. Creating")

    s3_open.send_file(
        File(
            source_path=TMP_FOLDER,
            source_name=f"{res['name']}_clean.csv",
            dest_path="formation/latest/",
            dest_name=f"{res['name']}_clean.csv",
        ),
        ignore_airflow_env=True,
    )

    return True


@task()
def send_notification(**context):
    nb_of = context["ti"].xcom_pull(
        key="nb_of", task_ids="process_organismes_formation"
    )
    nb_siret = context["ti"].xcom_pull(
        key="nb_siret", task_ids="process_organismes_formation"
    )
    send_message(
        text=(
            ":mega: Données organismes formations mises à jour.\n"
            f"- {nb_of} organismes de formation référencés\n"
            f"- {nb_siret} établissements (siret) représentés\n"
            f"- Données stockées sur S3 - Bucket {S3_BUCKET_DATA_PIPELINE_OPEN}"
        )
    )
