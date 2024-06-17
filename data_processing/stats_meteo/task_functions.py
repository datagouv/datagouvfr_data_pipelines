from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
    AIRFLOW_ENV,
)
from datagouvfr_data_pipelines.utils.mattermost import send_message
from datagouvfr_data_pipelines.utils.minio import MinIOClient
from datetime import datetime
import requests
import pandas as pd
import json

DATADIR = f"{AIRFLOW_DAG_TMP}stats_meteo/data"
minio_meteo = MinIOClient(bucket='meteofrance')


def gather_meteo_stats(ti):
    # on récupère tous les datasets de meteo.data.gouv
    datasets = requests.get(
        "https://www.data.gouv.fr/api/1/topics/6571f222129681e83de11aa2"
    ).json()["datasets"]
    # pour chaque dataset on récupère la métrique du mois précédent
    # qu'on indique comme monthly visit
    # on concatène tous les mois pour avoir la somme des visites
    # pour chaque dataset depuis le début
    arr = []
    for dataset in datasets:
        mydict = {}
        r2 = requests.get(
            "https://metric-api.data.gouv.fr/api/datasets/data/?dataset_id__exact=" + dataset["id"]
        ).json()["data"]
        mydict["dataset_id"] = dataset["id"]
        mydict["monthly_visit"] = r2[len(r2) - 2]["monthly_visit"]
        mydict["all_visit"] = 0
        mydict["monthly_download_resource"] = (
            r2[len(r2) - 2]["monthly_download_resource"]
        )
        mydict["all_download_resource"] = 0
        for i in range(len(r2)):
            if len(r2) > i:
                if r2[i]["monthly_visit"]:
                    mydict["all_visit"] += r2[i]["monthly_visit"]
                if r2[i]["monthly_download_resource"]:
                    mydict["all_download_resource"] += r2[i]["monthly_download_resource"]
        r3 = requests.get(
            f"https://www.data.gouv.fr/api/1/datasets/{dataset['id']}/",
            headers={"X-fields": "title"}
        ).json()
        mydict["dataset_title"] = r3["title"]
        arr.append(mydict)
    df = pd.DataFrame(arr)
    df = df[[
        "dataset_title",
        "dataset_id",
        "monthly_visit",
        "all_visit",
        "monthly_download_resource",
        "all_download_resource"
    ]]
    filename = f"meteo.data.gouv.fr-downloads-{datetime.now().strftime('%Y-%m-%d')}"
    df.to_csv(
        f"{DATADIR}/{filename}.csv",
        index=False
    )
    df.to_json(
        f"{DATADIR}/{filename}.json",
        orient="records",
    )
    with open(f"{DATADIR}/{filename}.json", "r") as f:
        stats = json.load(f)
    stats = {
        "stats_globales": {
            "total_monthly_download": df["monthly_download_resource"].sum(),
            "global_total_download": df["all_download_resource"].sum(),
        },
        "stats_detaillees": stats,
    }
    ti.xcom_push(key="filename", value=filename)


def send_to_minio(ti):
    filename = ti.xcom_pull(key="filename", task_ids="gather_meteo_stats")
    minio_meteo.send_files(
        list_files=[
            {
                "source_path": f"{DATADIR}/",
                "source_name": f"{filename}.{ext}",
                "dest_path": f"metrics/{AIRFLOW_ENV}/",
                "dest_name": f"{filename}.{ext}",
            } for ext in ["csv", "json"]
        ],
        ignore_airflow_env=True,
    )


def send_notification():
    send_message(
        text=(
            ":bar_chart: :partly_sunny_rain: Statistiques mensuelles "
            "de meteo.data.gouv disponibles sur Minio"
        )
    )
