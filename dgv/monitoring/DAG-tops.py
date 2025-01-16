from airflow.models import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from dateutil.relativedelta import relativedelta
from datetime import timedelta, datetime, date
import requests
import pandas as pd

from datagouvfr_data_pipelines.config import (
    AIRFLOW_ENV,
    MATTERMOST_DATAGOUV_REPORTING,
    MINIO_BUCKET_DATA_PIPELINE_OPEN,
)
from datagouvfr_data_pipelines.utils.datagouv import DATAGOUV_MATOMO_ID
from datagouvfr_data_pipelines.utils.mattermost import send_message
from datagouvfr_data_pipelines.utils.utils import (
    check_if_first_day_of_month,
    check_if_monday,
)
from datagouvfr_data_pipelines.utils.minio import MinIOClient

DAG_NAME = "dgv_tops"
MINIO_PATH = "dgv/"

BASE_URL = "https://stats.data.gouv.fr/index.php"
minio_open = MinIOClient(bucket=MINIO_BUCKET_DATA_PIPELINE_OPEN)

PARAMS_TOPS = {
    "module": "API",
    "idSite": DATAGOUV_MATOMO_ID,
    # "date": "yesterday",
    # "period": "day",
    "format": "json",
    "method": "Actions.getPageUrls",
    "expanded": 1,
    "flat": 1,
    "filter_limit": 12,
    "filter_column": "label",
}

PARAMS_GENERAL = {
    "module": "API",
    "idSite": DATAGOUV_MATOMO_ID,
    # "date": "2021-11-01,2021-12-01",
    # "period": "range",
    "format": "json",
    "method": "Actions.get",
    "expanded": 1,
    "flat": 1,
}


def compute_top(pattern, date, title):
    textTop = ""
    PARAMS_TOPS["period"] = "range"
    PARAMS_TOPS["date"] = date
    PARAMS_TOPS["filter_pattern"] = f"/fr/{pattern}/"
    print(PARAMS_TOPS)
    r = requests.get(BASE_URL, params=PARAMS_TOPS)
    arr = []
    for data in r.json():
        if "url" in data:
            if data["url"] not in [
                "https://www.data.gouv.fr/fr/datasets/",
                "https://www.data.gouv.fr/fr/reuses/",
            ]:
                r2 = requests.get(
                    data["url"].replace(
                        "https://www.data.gouv.fr/fr/",
                        "https://www.data.gouv.fr/api/1/",
                    )
                )
                print(data)
                arr.append({
                    "value": data["nb_visits"],
                    "url": data["url"],
                    "name": r2.json().get("title", data["url"])
                })
                textTop = (
                    textTop
                    + f"`{data['nb_visits']}`".ljust(10)
                    + data["url"]
                    + "\n"
                )
    mydict = {
        "nom": title,
        "unite": "visites",
        "values": arr[:10],
        "date_maj": datetime.today().strftime("%Y-%m-%d"),
    }
    return textTop, mydict


def compute_general(date, start, pageviews, uniq_pageviews, downloads):
    print(date)
    PARAMS_GENERAL["date"] = date
    PARAMS_GENERAL["period"] = "range" if "," in date else "day"
    print(PARAMS_GENERAL)
    r = requests.get(BASE_URL, params=PARAMS_GENERAL)
    print(r.json())
    for data in r.json():
        print(data)
        pageviews.append({
            "date": start,
            "value": data["nb_pageviews"],
        })
        uniq_pageviews.append({
            "date": start,
            "value": data["nb_uniq_pageviews"],
        })
        downloads.append({
            "date": start,
            "value": data["nb_downloads"],
        })
    return pageviews, uniq_pageviews, downloads


def get_top(ti, **kwargs):
    piwik_info = kwargs.get("templates_dict")
    end = datetime.strptime(piwik_info["date"], "%Y-%m-%d")
    if piwik_info["period"] == "day":
        start = end + relativedelta(days=-7)
    elif piwik_info["period"] == "week":
        start = end + relativedelta(months=-1)
    elif piwik_info["period"] == "month":
        start = end + relativedelta(months=-12)

    textTop, mydict = compute_top(
        piwik_info["type"],
        start.strftime("%Y-%m-%d") + "," + end.strftime("%Y-%m-%d"),
        piwik_info["title"],
    )
    ti.xcom_push(key="top_" + piwik_info["type"], value=textTop)
    ti.xcom_push(key="top_" + piwik_info["type"] + "_dict", value=mydict)


def getstats(dates, period):
    pageviews = []
    uniq_pageviews = []
    downloads = []
    for d in dates:
        start_date = datetime.strptime(str(d)[:10], "%Y-%m-%d")
        if period == "month":
            end_date = start_date + relativedelta(months=+1)
            matomodate = (
                start_date.strftime("%Y-%m-%d") + "," + end_date.strftime("%Y-%m-%d")
            )
        if period == "week":
            matomodate = start_date.strftime("%Y-%m-%d")
        if period == "day":
            matomodate = start_date.strftime("%Y-%m-%d")
        pageviews, uniq_pageviews, downloads = compute_general(
            matomodate, str(d)[:10], pageviews, uniq_pageviews, downloads
        )
    return pageviews, uniq_pageviews, downloads


def publish_top_mattermost(ti, **kwargs):
    publish_info = kwargs.get("templates_dict")
    print(publish_info)
    for _class in ["datasets", "reuses"]:
        top = ti.xcom_pull(
            key=f"top_{_class}", task_ids=f"get_top_{_class}_" + publish_info["period"]
        )
        header = (
            ":rolled_up_newspaper: **Top 10 jeux de données** - "
            if _class == "datasets"
            else ":artist: **Top 10 réutilisations** - "
        )
        message = (
            header
            + f"{publish_info['periode']} (visites)\n\n{top}"
        )
        send_message(message, MATTERMOST_DATAGOUV_REPORTING)


def send_tops_to_minio(ti, **kwargs):
    publish_info = kwargs.get("templates_dict")
    for _class in ["datasets", "reuses"]:
        top = ti.xcom_pull(
            key=f"top_{_class}_dict", task_ids=f"get_top_{_class}_" + publish_info["period"]
        )
        minio_open.dict_to_bytes_to_minio(top, publish_info["minio"] + f"top_{_class}.json")


def send_stats_to_minio(ti, **kwargs):
    piwik_info = kwargs.get("templates_dict")
    end = datetime.strptime(piwik_info["date"], "%Y-%m-%d")
    if piwik_info["period"] == "day":
        start = end + relativedelta(days=-7)
        dates = pd.date_range(start, end, freq="D")
    elif piwik_info["period"] == "week":
        start = end + relativedelta(months=-1)
        dates = pd.date_range(start, end, freq="D")
        print("----")
        print(dates)
    elif piwik_info["period"] == "month":
        start = end + relativedelta(months=-12)
        dates = pd.date_range(start, end, freq="MS")
    pageviews, uniq_pageviews, downloads = getstats(dates, piwik_info["period"])
    today = date.today().strftime("%Y-%m-%d")
    mydict = {
        "nom": "Nombre de visites",
        "unite": "visites",
        "values": pageviews,
        "date_maj": today,
    }
    minio_open.dict_to_bytes_to_minio(mydict, piwik_info["minio"] + "visits.json")

    mydict = {
        "nom": "Nombre de visiteurs uniques",
        "unite": "visiteurs",
        "values": uniq_pageviews,
        "date_maj": today,
    }
    minio_open.dict_to_bytes_to_minio(mydict, piwik_info["minio"] + "uniq_visits.json")

    mydict = {
        "nom": "Nombre de téléchargements",
        "unite": "téléchargements",
        "values": downloads,
        "date_maj": today,
    }
    minio_open.dict_to_bytes_to_minio(mydict, piwik_info["minio"] + "downloads.json")


default_args = {
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id=DAG_NAME,
    schedule_interval="15 6 * * *",
    start_date=datetime(2024, 8, 10),
    dagrun_timeout=timedelta(minutes=60),
    tags=["tops", "datagouv", "piwik"],
    default_args=default_args,
    catchup=False,
) as dag:
    get_top_datasets_day = PythonOperator(
        task_id="get_top_datasets_day",
        python_callable=get_top,
        templates_dict={
            "type": "datasets",
            "date": "{{ ds }}",
            "period": "day",
            "title": "Top 10 des jeux de données",
        },
    )

    get_top_reuses_day = PythonOperator(
        task_id="get_top_reuses_day",
        python_callable=get_top,
        templates_dict={
            "type": "reuses",
            "date": "{{ ds }}",
            "period": "day",
            "title": "Top 10 des réutilisations",
        },
    )

    publish_top_day_mattermost = PythonOperator(
        task_id="publish_top_day_mattermost",
        python_callable=publish_top_mattermost,
        templates_dict={"period": "day", "periode": "Journée d'hier"},
    )

    send_top_day_to_minio = PythonOperator(
        task_id="send_top_day_to_minio",
        python_callable=send_tops_to_minio,
        templates_dict={
            "period": "day",
            "minio": f"{AIRFLOW_ENV}/{MINIO_PATH}piwik_tops_daily/{{ ds }}/",
        },
    )

    send_stats_day_to_minio = PythonOperator(
        task_id="send_stats_day_to_minio",
        python_callable=send_stats_to_minio,
        templates_dict={
            "period": "day",
            "minio": f"{AIRFLOW_ENV}/{MINIO_PATH}piwik_stats_daily/{{ ds }}/",
            "date": "{{ ds }}",
            "title_end": "sur les trois derniers mois",
        },
    )

    check_if_monday = ShortCircuitOperator(
        task_id="check_if_monday", python_callable=check_if_monday
    )

    get_top_datasets_week = PythonOperator(
        task_id="get_top_datasets_week",
        python_callable=get_top,
        templates_dict={
            "type": "datasets",
            "period": "week",
            "date": "{{ ds }}",
            "title": "Top 10 des jeux de données",
        },
    )

    get_top_reuses_week = PythonOperator(
        task_id="get_top_reuses_week",
        python_callable=get_top,
        templates_dict={
            "type": "reuses",
            "period": "week",
            "date": "{{ ds }}",
            "title": "Top 10 des réutilisations",
        },
    )

    publish_top_week_mattermost = PythonOperator(
        task_id="publish_top_week_mattermost",
        python_callable=publish_top_mattermost,
        templates_dict={"period": "week", "periode": "Semaine dernière"},
    )

    send_top_week_to_minio = PythonOperator(
        task_id="send_top_week_to_minio",
        python_callable=send_tops_to_minio,
        templates_dict={
            "period": "week",
            "minio": f"{AIRFLOW_ENV}/{MINIO_PATH}piwik_tops_weekly/{{ ds }}/",
        },
    )

    send_stats_week_to_minio = PythonOperator(
        task_id="send_stats_week_to_minio",
        python_callable=send_stats_to_minio,
        templates_dict={
            "period": "week",
            "minio": f"{AIRFLOW_ENV}/{MINIO_PATH}piwik_stats_weekly/{{ ds }}/",
            "date": "{{ ds }}",
            "title_end": "sur les six derniers mois",
        },
    )

    check_if_first_day_of_month = ShortCircuitOperator(
        task_id="check_if_first_day_of_month",
        python_callable=check_if_first_day_of_month,
    )

    get_top_datasets_month = PythonOperator(
        task_id="get_top_datasets_month",
        python_callable=get_top,
        templates_dict={
            "type": "datasets",
            "period": "month",
            "date": "{{ ds }}",
            "title": "Top 10 des jeux de données",
        },
    )

    get_top_reuses_month = PythonOperator(
        task_id="get_top_reuses_month",
        python_callable=get_top,
        templates_dict={
            "type": "reuses",
            "period": "month",
            "date": "{{ ds }}",
            "title": "Top 10 des réutilisations",
        },
    )

    publish_top_month_mattermost = PythonOperator(
        task_id="publish_top_month_mattermost",
        python_callable=publish_top_mattermost,
        templates_dict={"period": "month", "periode": "Mois dernier"},
    )
    send_top_month_to_minio = PythonOperator(
        task_id="send_top_month_to_minio",
        python_callable=send_tops_to_minio,
        templates_dict={
            "period": "month",
            "minio": f"{AIRFLOW_ENV}/{MINIO_PATH}piwik_tops_monthly/{{ ds }}/",
        },
    )

    send_stats_month_to_minio = PythonOperator(
        task_id="send_stats_month_to_minio",
        python_callable=send_stats_to_minio,
        templates_dict={
            "period": "month",
            "minio": f"{AIRFLOW_ENV}/{MINIO_PATH}piwik_stats_monthly/{{ ds }}/",
            "date": "{{ ds }}",
            "title_end": "sur les deux dernières années",
        },
    )

    publish_top_day_mattermost.set_upstream(get_top_datasets_day)
    publish_top_day_mattermost.set_upstream(get_top_reuses_day)

    send_top_day_to_minio.set_upstream(publish_top_day_mattermost)
    send_stats_day_to_minio.set_upstream(publish_top_day_mattermost)

    check_if_monday.set_upstream(send_top_day_to_minio)
    get_top_datasets_week.set_upstream(check_if_monday)
    get_top_reuses_week.set_upstream(check_if_monday)

    publish_top_week_mattermost.set_upstream(get_top_datasets_week)
    publish_top_week_mattermost.set_upstream(get_top_reuses_week)
    send_top_week_to_minio.set_upstream(publish_top_week_mattermost)
    send_stats_week_to_minio.set_upstream(publish_top_week_mattermost)

    check_if_first_day_of_month.set_upstream(send_top_day_to_minio)
    get_top_datasets_month.set_upstream(check_if_first_day_of_month)
    get_top_reuses_month.set_upstream(check_if_first_day_of_month)

    publish_top_month_mattermost.set_upstream(get_top_datasets_month)
    publish_top_month_mattermost.set_upstream(get_top_reuses_month)
    send_top_month_to_minio.set_upstream(publish_top_month_mattermost)
    send_stats_month_to_minio.set_upstream(publish_top_month_mattermost)
