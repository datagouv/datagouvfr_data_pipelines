from datetime import datetime
import requests
import pandas as pd
import json
from io import StringIO

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
    AIRFLOW_ENV,
    MATOMO_TOKEN,
)
from datagouvfr_data_pipelines.utils.datagouv import prod_client
from datagouvfr_data_pipelines.utils.filesystem import File
from datagouvfr_data_pipelines.utils.mattermost import send_message
from datagouvfr_data_pipelines.utils.minio import MinIOClient

DATADIR = f"{AIRFLOW_DAG_TMP}stats_meteo/data"
minio_meteo = MinIOClient(bucket="meteofrance")

MATOMO_PARAMS = {
    "module": "API",
    "format": "CSV",
    "idSite": 292,
    "period": "month",
    "method": "Actions.getPageUrls",
    "filter_limit": 100,
    "format_metrics": 1,
    "expanded": 1,
    "translateColumnNames": 1,
    "language": "fr",
    "token_auth": MATOMO_TOKEN,
    # "date": "{start},{end}",
    # "label": "{label}",
}


def gather_meteo_stats(ti):
    print("> Stats détaillées")
    # on récupère tous les datasets de meteo.data.gouv
    datasets = [
        el["element"]["id"]
        for el in prod_client.get_all_from_api_query(
            "api/2/topics/6571f222129681e83de11aa2/elements/?class=Dataset"
        )
    ]
    # pour chaque dataset on récupère la métrique du mois précédent
    # qu'on indique comme monthly visit
    # on concatène tous les mois pour avoir la somme des visites
    # pour chaque dataset depuis le début
    arr = []
    for did in datasets:
        mydict = {}
        r2 = requests.get(
            "https://metric-api.data.gouv.fr/api/datasets/data/?dataset_id__exact="
            + did
        ).json()["data"]
        mydict["dataset_id"] = did
        mydict["monthly_visit"] = r2[len(r2) - 2]["monthly_visit"]
        mydict["all_visit"] = 0
        mydict["monthly_download_resource"] = r2[len(r2) - 2][
            "monthly_download_resource"
        ]
        mydict["all_download_resource"] = 0
        for i in range(len(r2)):
            if len(r2) > i:
                if r2[i]["monthly_visit"]:
                    mydict["all_visit"] += r2[i]["monthly_visit"]
                if r2[i]["monthly_download_resource"]:
                    mydict["all_download_resource"] += r2[i][
                        "monthly_download_resource"
                    ]
        r3 = requests.get(
            f"https://www.data.gouv.fr/api/1/datasets/{did}/",
            headers={"X-fields": "title"},
        ).json()
        mydict["dataset_title"] = r3["title"]
        arr.append(mydict)
    df = pd.DataFrame(arr)
    df = df[
        [
            "dataset_title",
            "dataset_id",
            "monthly_visit",
            "all_visit",
            "monthly_download_resource",
            "all_download_resource",
        ]
    ]
    filename = f"meteo.data.gouv.fr-downloads-{datetime.now().strftime('%Y-%m-%d')}"
    df.to_csv(f"{DATADIR}/{filename}.csv", index=False)
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

    # visites sur meteo.data.gouv.fr
    print("> Stats mensuelles")
    start_date = "2023-12-01"
    today = datetime.today().strftime("%Y-%m-%d")
    r = requests.post(
        "https://stats.data.gouv.fr/index.php",
        data=MATOMO_PARAMS
        | {
            "date": f"{start_date},{today}",
            "label": "",
        },
    )
    df = pd.read_csv(StringIO(r.text))
    df = df.groupby("Date")["Visiteurs uniques (résumé quotidien)"].sum().reset_index()
    df.to_csv(f"{DATADIR}/visites_meteo.csv", index=False)


def send_to_minio(ti):
    filename = ti.xcom_pull(key="filename", task_ids="gather_meteo_stats")
    minio_meteo.send_files(
        list_files=[
            File(
                source_path=f"{DATADIR}/",
                source_name=f"{filename}.{ext}",
                dest_path=f"metrics/{AIRFLOW_ENV}/",
                dest_name=f"{filename}.{ext}",
            )
            for ext in ["csv", "json"]
        ]
        + [
            File(
                source_path=f"{DATADIR}/",
                source_name="visites_meteo.csv",
                dest_path=f"metrics/{AIRFLOW_ENV}/",
                dest_name="visites_meteo.csv",
            )
        ],
        ignore_airflow_env=True,
    )


def send_notification(ti):
    filename = ti.xcom_pull(key="filename", task_ids="gather_meteo_stats")
    url = f"https://object.files.data.gouv.fr/meteofrance/metrics/{AIRFLOW_ENV}/"
    send_message(
        text=(
            "##### :bar_chart: :partly_sunny_rain: Statistiques mensuelles "
            "de meteo.data.gouv disponibles sur Minio :"
            f"\n- Statistiques détaillées (en [csv]({url + filename}.csv) "
            f"et en [json]({url + filename}.json))"
            f"\n- [Visites mensuelles]({url}visites_meteo.csv)"
        )
    )
