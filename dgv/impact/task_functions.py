from airflow.hooks.base import BaseHook
from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
    DATAGOUV_SECRET_API_KEY,
    AIRFLOW_ENV,
    MINIO_URL,
    MINIO_BUCKET_DATA_PIPELINE_OPEN,
    SECRET_MINIO_DATA_PIPELINE_USER,
    SECRET_MINIO_DATA_PIPELINE_PASSWORD,
)
from datagouvfr_data_pipelines.utils.mattermost import send_message
from datagouvfr_data_pipelines.utils.minio import send_files
from datagouvfr_data_pipelines.utils.datagouv import get_all_from_api_query
import pandas as pd
import os
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
import numpy as np

TMP_FOLDER = f"{AIRFLOW_DAG_TMP}dgv_impact/"
DATADIR = f"{TMP_FOLDER}data"


def calculate_metrics():
    # quality score
    print("Calculating average quality score")
    df_datasets = pd.read_csv(
        # this is the catalog
        "https://www.data.gouv.fr/fr/datasets/r/f868cca6-8da1-4369-a78d-47463f19a9a3",
        dtype=str,
        sep=";"
    )
    df_datasets["metric.views"] = df_datasets["metric.views"].astype(float)
    df_datasets = df_datasets.sort_values(by="metric.views", ascending=False)
    final = df_datasets[:1000]
    final["quality_score"] = final["quality_score"].astype(float)
    average_quality_score = round(100 * final["quality_score"].mean(), 2)

    # response time
    print("Calculating average response time")
    r = get_all_from_api_query("https://www.data.gouv.fr/api/1/discussions/")
    oneyearago = date.today() - relativedelta(years=1)
    oneyearago = oneyearago.strftime("%Y-%m-%d")
    nb_discussions = 0
    nb_discussions_with_answer = 0
    time_to_answer = []
    actual_date = date.today().strftime("%Y-%m-%d")
    while actual_date > oneyearago:
        item = next(r)
        actual_date = item["discussion"][0]["posted_on"]
        if actual_date > oneyearago and item["subject"]["class"] == "Dataset":
            nb_discussions += 1
            if len(item["discussion"]) > 1:
                nb_discussions_with_answer += 1
                date_format = "%Y-%m-%dT%H:%M:%S"
                # big assumption here: we consider the first response, whoever is responding
                # and whatever they are saying. For further improvements we could refine this
                # by only considering the first response by an admin of the dataset
                # /!\ could be tricky with automated discussions (for archived datasets for instance)
                first_date = datetime.strptime(item["discussion"][0]["posted_on"][:19], date_format)
                second_date = datetime.strptime(item["discussion"][1]["posted_on"][:19], date_format)
                ecart = second_date - first_date
                ecart_jour = ecart.days + (ecart.seconds / (3600 * 24))
                # arbitrary threshold of 30 days max
                if ecart_jour > 30:
                    time_to_answer.append(30)
                else:
                    time_to_answer.append(ecart_jour)
            else:
                time_to_answer.append(30)
    average_time_to_answer = round(np.mean(time_to_answer), 2)

    data = [
        {
            'nom_service_public_numerique': 'data.gouv.fr',
            'indicateur': 'Score qualité moyen 1000 JdD les plus vus',
            'valeur': average_quality_score,
            'unite_mesure': '%',
            'est_cible': False,
            'frequence_calcul': 'mensuelle',
            'date': datetime.today().strftime("%Y-%m-%d"),
            'est_periode': False,
            'date_debut': '',
            'est_automatise': True,
            'source_collecte': 'script',
            'mode_calcul': 'moyenne',
            'commentaires': ''
        },
        {
            'nom_service_public_numerique': 'data.gouv.fr',
            'indicateur': 'Délai moyen de réponse à une discussion',
            'valeur': average_time_to_answer,
            'unite_mesure': 'jour',
            'est_cible': False,
            'frequence_calcul': 'mensuelle',
            'date': datetime.today().strftime("%Y-%m-%d"),
            'est_periode': True,
            'date_debut': oneyearago,
            'est_automatise': True,
            'source_collecte': 'script',
            'mode_calcul': 'moyenne',
            'commentaires': 'les délais sont écrétés à 30 jours'
        },
    ]
    df = pd.DataFrame(data)
    history = pd.read_csv(f'{DATADIR}/history.csv')
    final = pd.concat([df, history])
    df.to_csv(os.path.join(DATADIR, "statistiques_impact_datagouvfr.csv"), index=False, encoding="utf8")


def send_stats_to_minio():
    send_files(
        MINIO_URL=MINIO_URL,
        MINIO_BUCKET=MINIO_BUCKET_DATA_PIPELINE_OPEN,
        MINIO_USER=SECRET_MINIO_DATA_PIPELINE_USER,
        MINIO_PASSWORD=SECRET_MINIO_DATA_PIPELINE_PASSWORD,
        list_files=[
            {
                "source_path": f"{DATADIR}/",
                "source_name": "statistiques_impact_datagouvfr.csv",
                "dest_path": "dgv/impact/",
                # à changer avant le passage en prod
                "dest_name": "new_statistiques_impact_datagouvfr.csv",
            }
        ],
    )


def send_notification_mattermost():
    send_message(
        text=(
            ":mega: KPI de data.gouv mises à jour.\n"
            f"- Données stockées sur Minio - [Bucket {MINIO_BUCKET_DATA_PIPELINE_OPEN}]"
            f"(https://console.object.files.data.gouv.fr/browser/{MINIO_BUCKET_DATA_PIPELINE_OPEN}/{AIRFLOW_ENV}/dgv/impact)\n"
            # f"- Données publiées [sur data.gouv.fr]({DATAGOUV_URL}/fr/datasets/XXXXXXXXXXXX)"
        )
    )
