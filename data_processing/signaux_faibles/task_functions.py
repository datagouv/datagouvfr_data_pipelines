import pandas as pd

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
    MINIO_URL,
    MINIO_BUCKET_DATA_PIPELINE_OPEN,
    SECRET_MINIO_DATA_PIPELINE_USER,
    SECRET_MINIO_DATA_PIPELINE_PASSWORD,
)
from datagouvfr_data_pipelines.utils.datagouv import get_resource
from datagouvfr_data_pipelines.utils.minio import send_files, compare_files
from datagouvfr_data_pipelines.utils.mattermost import send_message


def download_signaux_faibles():
    get_resource(
        resource_id="9d213815-1649-4527-9eb4-427146ef2e5b",
        file_to_store={
            "dest_path": f"{AIRFLOW_DAG_TMP}signaux_faibles_ratio_financiers/",
            "dest_name": "bilans_entreprises.csv",
        },
    )
    print("download done!")


def process_signaux_faibles(ti):
    fields = ["siren", "chiffre_d_affaires", "resultat_net", "date_cloture_exercice"]
    df_bilans = pd.read_csv(
        f"{AIRFLOW_DAG_TMP}signaux_faibles_ratio_financiers/bilans_entreprises.csv",
        dtype=str,
        sep=";",
        usecols=fields
    )
    df_bilans["chiffre_d_affaires"] = df_bilans["chiffre_d_affaires"].astype(float)
    df_bilans["resultat_net"] = df_bilans["resultat_net"].astype(float)
    df_bilans = df_bilans.sort_values(by=["date_cloture_exercice"])
    df_bilans = df_bilans.drop_duplicates(subset=["siren"], keep="last")
    df_bilans = df_bilans[["siren", "chiffre_d_affaires", "resultat_net"]]
    df_bilans.to_csv(
        f"{AIRFLOW_DAG_TMP}signaux_faibles_ratio_financiers/"
        "synthese_bilans.csv",
        index=False
    )

    ti.xcom_push(key="nb_siren", value=str(df_bilans["siren"].shape[0]))


def send_file_to_minio():
    send_files(
        MINIO_URL=MINIO_URL,
        MINIO_BUCKET=MINIO_BUCKET_DATA_PIPELINE_OPEN,
        MINIO_USER=SECRET_MINIO_DATA_PIPELINE_USER,
        MINIO_PASSWORD=SECRET_MINIO_DATA_PIPELINE_PASSWORD,
        list_files=[
            {
                "source_path": f"{AIRFLOW_DAG_TMP}signaux_faibles_ratio_financiers/",
                "source_name": "synthese_bilans.csv",
                "dest_path": "signaux_faibles/new/",
                "dest_name": "synthese_bilans.csv",
            },
        ],
    )


def compare_files_minio():
    is_same = compare_files(
        MINIO_URL=MINIO_URL,
        MINIO_BUCKET=MINIO_BUCKET_DATA_PIPELINE_OPEN,
        MINIO_USER=SECRET_MINIO_DATA_PIPELINE_USER,
        MINIO_PASSWORD=SECRET_MINIO_DATA_PIPELINE_PASSWORD,
        file_path_1="signaux_faibles/new/",
        file_name_2="synthese_bilans.csv",
        file_path_2="signaux_faibles/latest/",
        file_name_1="synthese_bilans.csv",
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
                "source_path": f"{AIRFLOW_DAG_TMP}signaux_faibles_ratio_financiers/",
                "source_name": "synthese_bilans.csv",
                "dest_path": "signaux_faibles/latest/",
                "dest_name": "synthese_bilans.csv",
            },
        ],
    )

    return True


def send_notification(ti):
    nb_siren = ti.xcom_pull(key="nb_siren", task_ids="process_agence_bio")
    send_message(
        text=(
            ":mega: Données Signaux faibles mises à jour.\n"
            f"- {nb_siren} unités légales référencés\n"
            f"- Données stockées sur Minio - Bucket {MINIO_BUCKET_DATA_PIPELINE_OPEN}"
        )
    )
