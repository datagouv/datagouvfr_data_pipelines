from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
    AIRFLOW_DAG_HOME,
    DATAGOUV_SECRET_API_KEY,
    AIRFLOW_ENV,
    MINIO_URL,
    MINIO_BUCKET_DATA_PIPELINE_OPEN,
    SECRET_MINIO_DATA_PIPELINE_USER,
    SECRET_MINIO_DATA_PIPELINE_PASSWORD,
)
from datagouvfr_data_pipelines.utils.mattermost import send_message
from datagouvfr_data_pipelines.utils.minio import send_files
from datagouvfr_data_pipelines.utils.datagouv import (
    get_all_from_api_query,
    post_remote_resource,
    update_dataset_or_resource_metadata,
    DATAGOUV_URL
)
import pandas as pd
import os
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
import numpy as np
import requests
import json

TMP_FOLDER = f"{AIRFLOW_DAG_TMP}dgv_impact/"
DATADIR = f"{TMP_FOLDER}data"


def calculate_quality_score(ti):
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
    kpi = {
        'administration_rattachement': 'DINUM',
        'nom_service_public_numerique': 'data.gouv.fr',
        'indicateur': 'Score qualité moyen 1000 JdD les plus vus',
        'valeur': average_quality_score,
        'unite_mesure': '%',
        'est_cible': False,
        'frequence_monitoring': 'mensuelle',
        'date': datetime.today().strftime("%Y-%m-%d"),
        'est_periode': False,
        'date_debut': '',
        'est_automatise': True,
        'source_collecte': 'script',
        'code_insee': '',
        'dataviz_wish': 'barchart',
        'commentaires': ''
    }
    ti.xcom_push(key='kpi', value=kpi)


def calculate_time_for_legitimate_answer(ti):
    print("Calculating average time for legitimate answer")
    datagouv_team = requests.get(
        "https://www.data.gouv.fr/api/1/organizations/646b7187b50b2a93b1ae3d45/"
    ).json()
    datagouv_team = [m['user']['id'] for m in datagouv_team['members']]

    r = get_all_from_api_query("https://www.data.gouv.fr/api/1/discussions/?sort=-created")
    end_date = datetime.today().strftime("%Y-%m-%d")
    oneyearago = date.today() - relativedelta(years=1)
    start_date = oneyearago.strftime("%Y-%m-%d")
    nb_discussions = 0
    nb_discussions_with_legit_answer = 0
    time_to_answer = []
    k = 0
    for discussion in r:
        if discussion['created'] > end_date:
            continue
        elif discussion['created'] < start_date:
            break
        if discussion['subject']['class'] == 'Dataset':
            k += 1
            if k % 100 == 0:
                print(f"   > {k} discussions processed")
            nb_discussions += 1
            if len(discussion['discussion']) > 1:
                # getting legit users
                r = requests.get(
                    f"https://www.data.gouv.fr/api/1/datasets/{discussion['subject']['id']}/"
                )
                if not r.ok:
                    print(f"Not OK: https://www.data.gouv.fr/api/1/datasets/{discussion['subject']['id']}/")
                    continue
                dataset = r.json()
                if dataset.get('organization', None):
                    dataset_supervisors = requests.get(
                        f"https://www.data.gouv.fr/api/1/organizations/{dataset['organization']['id']}/"
                    ).json()
                    dataset_supervisors = [m['user']['id'] for m in dataset_supervisors['members']]
                else:
                    dataset_supervisors = [dataset['owner']['id']] if dataset['owner'] else []
                legit = datagouv_team + dataset_supervisors

                # getting time to legit response
                opening_date = discussion['discussion'][0]['posted_on'][:10]
                answered_date = None
                for comment in discussion['discussion'][1:]:
                    if comment['posted_by']['id'] in legit:
                        answered_date = comment['posted_on'][:10]
                        nb_discussions_with_legit_answer += 1
                if not answered_date:
                    time_to_answer.append(30)
                else:
                    opening_date = datetime.strptime(opening_date, "%Y-%m-%d")
                    answered_date = datetime.strptime(answered_date, "%Y-%m-%d")
                    delai = answered_date - opening_date
                    time_to_answer.append(min(delai.days, 30))
            else:
                time_to_answer.append(30)
    average_time_to_answer = round(np.mean(time_to_answer), 2)
    print(f"Taux de réponses légitimes : {round(nb_discussions_with_legit_answer/nb_discussions*100)}%")
    kpi = {
        'administration_rattachement': 'DINUM',
        'nom_service_public_numerique': 'data.gouv.fr',
        'indicateur': 'Délai moyen pour une réponse légitime à une discussion',
        'valeur': average_time_to_answer,
        'unite_mesure': 'jour',
        'est_cible': False,
        'frequence_monitoring': 'mensuelle',
        'date': end_date,
        'est_periode': True,
        'date_debut': start_date,
        'est_automatise': True,
        'source_collecte': 'script',
        'code_insee': '',
        'dataviz_wish': 'barchart',
        'commentaires': 'les délais sont écrétés à 30 jours'
    }
    ti.xcom_push(key='kpi', value=kpi)


def gather_kpis(ti):
    data = [
        ti.xcom_pull(key='kpi', task_ids=t)
        for t in [
            'calculate_quality_score',
            'calculate_time_for_legitimate_answer'
        ]
    ]
    df = pd.DataFrame(data)
    df.to_csv(
        os.path.join(DATADIR, f"stats_{datetime.today().strftime('%Y-%m-%d')}.csv"),
        index=False,
        encoding="utf8"
    )
    history = pd.read_csv(f'{DATADIR}/history.csv')
    final = pd.concat([df, history])
    final.to_csv(os.path.join(DATADIR, "statistiques_impact_datagouvfr.csv"), index=False, encoding="utf8")


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
                "dest_name": "statistiques_impact_datagouvfr.csv",
            },
            # saving millésimes in case of an emergency
            {
                "source_path": f"{DATADIR}/",
                "source_name": f"stats_{datetime.today().strftime('%Y-%m-%d')}.csv",
                "dest_path": "dgv/impact/",
                "dest_name": f"stats_{datetime.today().strftime('%Y-%m-%d')}.csv",
            },
        ],
    )


def publish_datagouv(DAG_FOLDER):
    with open(f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}config/dgv.json") as fp:
        data = json.load(fp)
    post_remote_resource(
        api_key=DATAGOUV_SECRET_API_KEY,
        remote_url=f"https://object.files.data.gouv.fr/{MINIO_BUCKET_DATA_PIPELINE_OPEN}/{AIRFLOW_ENV}/dgv/impact/statistiques_impact_datagouvfr.csv",
        dataset_id=data[AIRFLOW_ENV]['dataset_id'],
        resource_id=data[AIRFLOW_ENV]['resource_id'],
        filesize=os.path.getsize(os.path.join(DATADIR, "statistiques_impact_datagouvfr.csv")),
        title="Indicateurs d'impact de data.gouv.fr",
        format="csv",
        description=f"Dernière modification : {datetime.today()})",
    )
    update_dataset_or_resource_metadata(
        api_key=DATAGOUV_SECRET_API_KEY,
        payload={"temporal_coverage": {
            "end": datetime(2023, 11, 16).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "start": datetime.today().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        }},
        dataset_id=data[AIRFLOW_ENV]['dataset_id']
    )


def send_notification_mattermost(DAG_FOLDER):
    with open(f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}config/dgv.json") as fp:
        data = json.load(fp)
    send_message(
        text=(
            ":mega: KPI de data.gouv mises à jour.\n"
            f"- Données stockées sur Minio - [Bucket {MINIO_BUCKET_DATA_PIPELINE_OPEN}]"
            f"(https://console.object.files.data.gouv.fr/browser/{MINIO_BUCKET_DATA_PIPELINE_OPEN}/{AIRFLOW_ENV}/dgv/impact)\n"
            f"- Données publiées [sur data.gouv.fr]({DATAGOUV_URL}/fr/datasets/{data[AIRFLOW_ENV]['dataset_id']})"
        )
    )
