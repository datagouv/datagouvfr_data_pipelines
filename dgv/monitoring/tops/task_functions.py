import logging
from datetime import datetime

import pandas as pd
import requests
from airflow.decorators import task
from datagouvfr_data_pipelines.config import (
    MATOMO_TOKEN,
    MATTERMOST_DATAGOUV_REPORTING,
    S3_BUCKET_DATA_PIPELINE_OPEN,
)
from datagouvfr_data_pipelines.utils.datagouv import DATAGOUV_MATOMO_ID
from datagouvfr_data_pipelines.utils.mattermost import send_message
from datagouvfr_data_pipelines.utils.s3 import S3Client
from dateutil.relativedelta import relativedelta

BASE_URL = "https://stats.data.gouv.fr/index.php"
s3_open = S3Client(bucket=S3_BUCKET_DATA_PIPELINE_OPEN)

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
    "token_auth": MATOMO_TOKEN,
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
    "token_auth": MATOMO_TOKEN,
}


def build_start(end: datetime, freq: str) -> datetime:
    if freq == "day":
        # we want only one day
        return end
    elif freq == "week":
        # 6 days back, 7 days in total
        return end + relativedelta(days=-6)
    elif freq == "month":
        return end + relativedelta(months=-1)
    elif freq == "year":
        return end + relativedelta(years=-1)
    raise ValueError


def compute_top(_class, date, title):
    textTop = ""
    r = requests.post(
        BASE_URL,
        data=PARAMS_TOPS
        | {
            "period": "range",
            "date": date,
            "filter_pattern": f"/{_class}/",
        },
    )
    logging.info(r.url)
    r.raise_for_status()
    arr = []
    for data in r.json():
        if "url" in data and data["url"] not in [
            "https://www.data.gouv.fr/fr/datasets/",
            "https://www.data.gouv.fr/fr/reuses/",
        ]:
            logging.info(data)
            url = (
                # handling switch to no more /fr/
                data["url"].replace(
                    "https://www.data.gouv.fr/fr/",
                    "https://www.data.gouv.fr/api/1/",
                )
                if "https://www.data.gouv.fr/fr/" in data["url"]
                else data["url"].replace(
                    "https://www.data.gouv.fr/",
                    "https://www.data.gouv.fr/api/1/",
                )
            )
            r2 = requests.get(url)
            _stop = False
            while not r2.ok:
                # handling cases like https://www.data.gouv.fr/fr/reuses/{id}/discussions/
                url = "/".join(url.split("/")[:-1])
                if not url.startswith("https://www.data.gouv.fr/api/1/"):
                    logging.warning(f"Could not fetch info for: {data['url']}")
                    _stop = True
                    break
                r2 = requests.get(url)
            if _stop:
                continue
            arr.append(
                {
                    "value": data["nb_visits"],
                    "url": data["url"],
                    "name": r2.json().get("title", data["url"]),
                }
            )
            textTop += f"`{data['nb_visits']}`".ljust(10) + data["url"] + "\n"
    mydict = {
        "name": title,
        "unit": "visites",
        "values": arr[:10],
        "date_maj": datetime.today().strftime("%Y-%m-%d"),
    }
    return textTop, mydict


def compute_general(date):
    logging.info(date)
    r = requests.post(
        BASE_URL,
        data=PARAMS_GENERAL
        | {
            "date": date,
            "period": "range" if "," in date else "day",
        },
    )
    logging.info(r.url)
    r.raise_for_status()
    pageviews, uniq_pageviews, downloads = [], [], []
    for data in r.json():
        logging.info(data)
        pageviews.append(
            {
                "date": date,
                "value": data["nb_pageviews"],
            }
        )
        uniq_pageviews.append(
            {
                "date": date,
                "value": data["nb_uniq_pageviews"],
            }
        )
        downloads.append(
            {
                "date": date,
                "value": data["nb_downloads"],
            }
        )
    return pageviews, uniq_pageviews, downloads


@task()
def get_top(date: str, period: str, _type: str, title: str, **context):
    end = datetime.strptime(date, "%Y-%m-%d")
    start = build_start(end, period)
    textTop, mydict = compute_top(
        _type,
        start.strftime("%Y-%m-%d") + "," + end.strftime("%Y-%m-%d"),
        title,
    )
    context["ti"].xcom_push(key=f"top_{_type}", value=textTop)
    context["ti"].xcom_push(key=f"top_{_type}_dict", value=mydict)


def get_stats(dates: list[datetime], period: str) -> tuple[list, list, list]:
    pageviews = []
    uniq_pageviews = []
    downloads = []
    for d in dates:
        start_date = datetime.strptime(str(d)[:10], "%Y-%m-%d")
        if period in ["month", "year"]:
            end_date = start_date + relativedelta(**{f"{period}s": 1})
            matomodate = (
                start_date.strftime("%Y-%m-%d") + "," + end_date.strftime("%Y-%m-%d")
            )
        elif period in ["day", "week"]:
            matomodate = start_date.strftime("%Y-%m-%d")
        pv, upv, dl = compute_general(matomodate)
        pageviews.append(pv)
        uniq_pageviews.append(upv)
        downloads.append(dl)
    return pageviews, uniq_pageviews, downloads


@task()
def publish_top_mattermost(period: str, label: str, **context):
    for _class in ["datasets", "reuses"]:
        top = context["ti"].xcom_pull(
            key=f"top_{_class}", task_ids=f"get_top_{_class}_{period}"
        )
        header = (
            ":rolled_up_newspaper: **Top 10 jeux de données** - "
            if _class == "datasets"
            else ":artist: **Top 10 réutilisations** - "
        )
        message = header + f"{label} (visites)\n\n{top}"
        send_message(message, MATTERMOST_DATAGOUV_REPORTING)


@task()
def send_tops_to_s3(period: str, s3: str, **context):
    for _class in ["datasets", "reuses"]:
        top = context["ti"].xcom_pull(
            key=f"top_{_class}_dict",
            task_ids=f"get_top_{_class}_{period}",
        )
        s3_open.send_dict_as_file(top, s3 + f"top_{_class}.json")


@task()
def send_stats_to_s3(date: str, period: str, s3: str):
    end = datetime.strptime(date, "%Y-%m-%d")
    start = build_start(end, period)
    dates = pd.date_range(
        start,
        end,
        freq=("MS" if period == "month" else "YS" if period == "year" else "D"),
    )
    pageviews, uniq_pageviews, downloads = get_stats(dates, period)
    mydict = {
        "name": "Nombre de visites",
        "unit": "visites",
        "values": pageviews,
        "date_maj": date,
    }
    s3_open.send_dict_as_file(mydict, s3 + "visits.json")

    mydict = {
        "name": "Nombre de visiteurs uniques",
        "unit": "visiteurs",
        "values": uniq_pageviews,
        "date_maj": date,
    }
    s3_open.send_dict_as_file(mydict, s3 + "uniq_visits.json")

    mydict = {
        "name": "Nombre de téléchargements",
        "unit": "téléchargements",
        "values": downloads,
        "date_maj": date,
    }
    s3_open.send_dict_as_file(mydict, s3 + "downloads.json")
