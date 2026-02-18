import json
import logging
from collections import defaultdict
from datetime import datetime, timedelta
from io import StringIO
from time import sleep
from typing import Iterator

import numpy as np
import pandas as pd
import requests
from airflow.decorators import task
from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
    MATOMO_TOKEN,
)
from datagouvfr_data_pipelines.utils.crisp import (
    get_all_conversations,
    get_all_spam_conversations,
)
from datagouvfr_data_pipelines.utils.datagouv import (
    DATAGOUV_MATOMO_ID,
    local_client,
)
from datagouvfr_data_pipelines.utils.filesystem import File
from datagouvfr_data_pipelines.utils.s3 import S3Client
from datagouvfr_data_pipelines.utils.utils import list_months_between

DAG_NAME = "dgv_dashboard"
TMP_FOLDER = f"{AIRFLOW_DAG_TMP}{DAG_NAME}/"
one_year_ago = datetime.today() - timedelta(days=365)
groups = [
    k + "@data.gouv.fr"
    for k in [
        "support",
        "ouverture",
        "moissonnage",
        "certification",
        "geo",
        "cadastre",
        "candidature",
    ]
]
entreprises_api_url = "https://recherche-entreprises.api.gouv.fr/search?q="
# max 5 requests/second (rate limiting is 1/7)
rate_limiting_delay = 1 / 5

s3_open = S3Client(bucket="dataeng-open")
s3_destination_folder = "dashboard/"

MATOMO_PARAMS = {
    "module": "API",
    "format": "CSV",
    "period": "month",
    "method": "Actions.getPageUrls",
    "filter_limit": 100,
    "format_metrics": 1,
    "expanded": 1,
    "translateColumnNames": 1,
    "language": "fr",
    "token_auth": MATOMO_TOKEN,
    # "idSite": {site_id},
    # "date": "{start},{end}",
    # "label": "{label}",
}


@task()
def get_support_tickets(start_date: datetime, **context):
    def get_monthly_count(
        tickets: list[dict],
        created_at_key: str,
        segment_prefixes: list[str] | None = None,
    ) -> dict[str, int]:
        # turns a list of tickets into the monthly count
        months_count = defaultdict(int)
        for k in range(len(tickets)):
            tickets[k]["created_at_date"] = datetime.fromtimestamp(
                tickets[k][created_at_key] / 1000
            ).strftime("%Y-%m-%d")
            if segment_prefixes is None or any(
                seg.startswith(pref)
                for pref in segment_prefixes
                for seg in tickets[k]["meta"]["segments"]
            ):
                months_count[tickets[k]["created_at_date"][:7]] += 1
        return months_count

    # to be removed in April 2026
    if datetime.today().strftime("%Y-%m") > "2026-04":
        raise ValueError("Time to remove Zammad tickets")
    remaining_zammad_tickets = {
        "all": {
            "2024-07": 1697,
            "2024-08": 1310,
            "2024-09": 1933,
            "2024-10": 1908,
            "2024-11": 1848,
            "2024-12": 1657,
            "2025-01": 2143,
            "2025-02": 1966,
            "2025-03": 2124,
            "2025-04": 1000,
        },
        "hs": {
            "2024-07": 54,
            "2024-08": 38,
            "2024-09": 43,
            "2024-10": 33,
            "2024-11": 14,
            "2024-12": 22,
            "2025-01": 35,
            "2025-02": 43,
            "2025-03": 39,
            "2025-04": 14,
        },
        "spam": {
            "2024-07": 534,
            "2024-08": 429,
            "2024-09": 278,
            "2024-10": 516,
            "2024-11": 474,
            "2024-12": 238,
            "2025-01": 318,
            "2025-02": 326,
            "2025-03": 427,
            "2025-04": 232,
        },
    }

    not_spam_tickets = get_all_conversations()
    # /!\ spam tickets are deleted after 1 month in Crisp, so this will be underestimated
    spam_tickets = get_all_spam_conversations()
    tickets = {
        "all": get_monthly_count(not_spam_tickets, "created_at"),
        "hs": get_monthly_count(not_spam_tickets, "created_at", ["hors sujet"]),
        "spam": get_monthly_count(spam_tickets, "timestamp"),
    }
    # all includes spam
    tickets["all"] = {
        # assuming there will always be at least one normal ticket per month
        month: tickets["all"].get(month, 0) + tickets["spam"].get(month, 0)
        for month in tickets["all"].keys()
    }

    # adding remaining zammad tickets for now
    for scope in remaining_zammad_tickets.keys():
        for month in remaining_zammad_tickets[scope].keys():
            tickets[scope][month] = (
                tickets[scope].get(month, 0) + remaining_zammad_tickets[scope][month]
            )

    # restrain to one year
    start_month = start_date.strftime("%Y-%m")
    tickets = {
        scope: dict(
            sorted(
                {
                    month: count
                    for month, count in tickets[scope].items()
                    if month >= start_month
                }.items(),
                key=lambda i: i[0],
            )
        )
        for scope in tickets.keys()
    }

    context["ti"].xcom_push(key="tickets", value=tickets)
    context["ti"].xcom_push(key="months", value=list(tickets["all"].keys()))


def fill_url(
    start: str,
    end: str,
    site_id: str,
    label: str,
) -> str:
    return (
        f"https://stats.data.gouv.fr/index.php?module=API&format=CSV&idSite={site_id}"
        f"&period=month&date={start},{end}&method=Actions.getPageUrls&label={label}"
        "&filter_limit=100&format_metrics=1&expanded=1&translateColumnNames=1&language=fr"
        "&token_auth=anonymous"
    )


@task()
def get_visits(
    start_date: datetime,
    end_date: datetime = datetime.today() - timedelta(days=1),
    **context,
) -> None:
    # url_stats_home_dgv = {
    #     "site_id": DATAGOUV_MATOMO_ID,
    #     "label": "fr",
    #     "title": "Homepage",
    # }

    months_to_process = list_months_between(start_date, end_date)
    if datetime.today().strftime("%Y-%m") > "2026-07":
        raise ValueError("Time to remove old support URL")
    old_url_stats_support = {
        "site_id": "176",
        "label": "%40%252Findex",
        "title": "old_support",
    }
    # stats are spread across /support and /support/
    url_stats_support = {
        "site_id": DATAGOUV_MATOMO_ID,
        "label": "support",
        "title": "support",
    }
    url_stats_slash_support = {
        "site_id": DATAGOUV_MATOMO_ID,
        "label": "%2Fsupport",
        "title": "/support",
    }
    for k in [
        # not taking the stats from the homepage, no variation
        # url_stats_home_dgv,
        url_stats_support,
        url_stats_slash_support,
        old_url_stats_support,
    ]:
        r = requests.post(
            "https://stats.data.gouv.fr/index.php",
            data=MATOMO_PARAMS
            | {
                "date": f"{start_date.strftime('%Y-%m-%d')},{end_date.strftime('%Y-%m-%d')}",
                "label": k["label"],
                "idSite": k["site_id"],
            },
        )
        df = pd.read_csv(
            filepath_or_buffer=StringIO(r.text),
            usecols=["Date", "Vues de page uniques"],
        )

        # Filling missing months to 0 in case Matomo did not return them yet
        for month in months_to_process:
            if month not in df["Date"].values:
                logging.error(f"Missing month: {month}")
                new_values = pd.DataFrame(
                    {"Date": [month], "Vues de page uniques": [0]}
                )
                df = pd.concat([new_values, df], ignore_index=True)
                df = df.sort_values(by="Date")

        vues = df["Vues de page uniques"].to_list()
        logging.info(f"Vues : {vues}")
        context["ti"].xcom_push(key=k["title"], value=vues)


@task()
def gather_and_upload(**context) -> None:
    tickets = context["ti"].xcom_pull(key="tickets", task_ids="get_support_tickets")
    months = context["ti"].xcom_pull(key="months", task_ids="get_support_tickets")
    # homepage = context["ti"].xcom_pull(key="homepage", task_ids="get_visits")
    support = {
        k: context["ti"].xcom_pull(key=k, task_ids="get_visits")
        for k in ["support", "/support", "old_support"]
    }

    stats = pd.DataFrame(
        {
            # "Homepage": homepage,
            "Page support": [
                sum(support[k][i] for k in support.keys())
                for i in range(len(list(support.values())[0]))
            ],
            "Ouverture de ticket": tickets["all"],
            "Ticket hors-sujet": tickets["hs"],
            "Ticket spam": tickets["spam"],
        },
        index=months,
    ).T
    # removing current month from stats
    stats = stats[stats.columns[:-1]].fillna(0)
    stats.to_csv(TMP_FOLDER + "stats_support.csv")

    # sending to s3
    s3_open.send_file(
        File(
            source_path=TMP_FOLDER,
            source_name="stats_support.csv",
            dest_path=s3_destination_folder,
            dest_name="stats_support.csv",
        ),
        ignore_airflow_env=True,
    )


def is_certified(badges: Iterator[dict[str, str]]) -> bool:
    for b in badges:
        if b["kind"] == "certified":
            return True
    return False


def is_SP_or_CT(
    siret: str,
    session: requests.Session,
) -> tuple[bool, str | None]:
    issue = None
    if siret is None:
        return False, issue
    try:
        sleep(rate_limiting_delay)
        r = session.get(entreprises_api_url + siret).json()
    except Exception:
        logging.warning("Sleeping a bit more")
        sleep(1)
        r = session.get(entreprises_api_url + siret).json()
    if len(r["results"]) == 0:
        logging.warning("No match for: ", siret)
        issue = "pas de correspondance : " + siret
        return False, issue
    if len(r["results"]) > 1:
        logging.warning("Ambiguous: ", siret)
        issue = "SIRET ambigu : " + siret
    complements = r["results"][0]["complements"]
    return complements["collectivite_territoriale"] or complements[
        "est_service_public"
    ], issue


@task()
def get_and_upload_certification() -> None:
    session = requests.Session()
    orgas = local_client.get_all_from_api_query(
        "api/1/organizations/", mask="data{id,badges,business_number_id}"
    )
    certified = []
    SP_or_CT = []
    issues = []
    for o in orgas:
        if is_certified(o["badges"]):
            certified.append(o["id"])
        it_is, issue = is_SP_or_CT(o["business_number_id"], session)
        if it_is:
            SP_or_CT.append(o["id"])
        if issue:
            issues.append({o["id"]: issue})
    with open(TMP_FOLDER + "certified.json", "w") as f:
        json.dump(certified, f, indent=4)
    with open(TMP_FOLDER + "SP_or_CT.json", "w") as f:
        json.dump(SP_or_CT, f, indent=4)
    with open(TMP_FOLDER + "issues.json", "w") as f:
        json.dump(issues, f, indent=4)

    s3_open.send_files(
        list_files=[
            File(
                source_path=TMP_FOLDER,
                source_name=f,
                dest_path=s3_destination_folder
                + datetime.now().strftime("%Y-%m-%d")
                + "/",
                dest_name=f,
            )
            for f in ["certified.json", "SP_or_CT.json", "issues.json"]
        ],
        ignore_airflow_env=True,
    )


@task()
def get_and_upload_reuses_down() -> None:
    client = S3Client(bucket="data-pipeline-open")
    # getting latest data
    df = pd.read_csv(
        StringIO(
            client.get_file_content(
                "prod/bizdev/all_reuses_most_visits_KO_last_month.csv"
            )
        )
    )
    stats = pd.DataFrame(
        df["error"].apply(lambda x: x if x == "404" else "Autre erreur").value_counts()
    ).T
    stats["Date"] = [datetime.now().strftime("%Y-%m-%d")]
    stats["Total"] = [
        requests.get(
            "https://www.data.gouv.fr/api/1/reuses/",
            headers={"X-fields": "total"},
        ).json()["total"]
    ]

    # getting historical data
    output_file_name = "stats_reuses_down.csv"
    hist = pd.read_csv(
        StringIO(s3_open.get_file_content(s3_destination_folder + output_file_name))
    )
    start_len = len(hist)
    hist = pd.concat([hist, stats]).drop_duplicates("Date")
    # just in case
    assert start_len <= len(hist)
    hist.to_csv(TMP_FOLDER + output_file_name, index=False)
    s3_open.send_file(
        File(
            source_path=TMP_FOLDER,
            source_name=output_file_name,
            dest_path=s3_destination_folder,
            dest_name=output_file_name,
        ),
        ignore_airflow_env=True,
    )


@task()
def get_catalog_stats() -> None:
    datasets = []
    resources = []
    crawler = local_client.get_all_from_api_query(
        "api/1/datasets/",
        mask="data{id,harvest,quality,tags,resources{id,format,type}}",
    )
    processed = 0
    for c in crawler:
        datasets.append([c["id"], bool(c["harvest"]), c["quality"], c["tags"]])
        for r in c["resources"]:
            tmp = {k: r[k] for k in ["id", "format", "type"]}
            tmp["hvd"] = "hvd" in c["tags"]
            resources.append(tmp)
        processed += 1
        if processed % 1000 == 0:
            logging.info(f"> {processed} datasets processed")

    # getting datasets quality and count
    cats = list(max(datasets, key=lambda x: len(x[2]))[2].keys())
    dataset_quality = {
        k: {c: [] for c in cats} for k in ["all", "harvested", "local", "hvd"]
    }
    dataset_quality.update(
        {"count": {k: 0 for k in ["all", "harvested", "local", "hvd"]}}
    )
    for d in datasets:
        dataset_quality["count"]["all"] += 1
        dataset_quality["count"]["harvested" if d[1] else "local"] += 1
        if "hvd" in d[3]:
            dataset_quality["count"]["hvd"] += 1
        for c in cats:
            dataset_quality["all"][c].append(d[2].get(c, False) or False)
            dataset_quality["harvested" if d[1] else "local"][c].append(
                d[2].get(c, False) or False
            )
            if "hvd" in d[3]:
                dataset_quality["hvd"][c].append(d[2].get(c, False) or False)
    for k in dataset_quality.keys():
        for c in dataset_quality[k]:
            dataset_quality[k][c] = np.mean(dataset_quality[k][c])
    dataset_quality = {datetime.now().strftime("%Y-%m-%d"): dataset_quality}

    # getting resources types and formats
    resources_stats = {"all": {}, "hvd": {}}
    for r in resources:
        if r["format"] not in resources_stats["all"]:
            resources_stats["all"][r["format"]] = 0
        resources_stats["all"][r["format"]] += 1
        if r["hvd"]:
            if r["format"] not in resources_stats["hvd"]:
                resources_stats["hvd"][r["format"]] = 0
            resources_stats["hvd"][r["format"]] += 1

        if r["type"] not in resources_stats:
            resources_stats[r["type"]] = {}
        if r["format"] not in resources_stats[r["type"]]:
            resources_stats[r["type"]][r["format"]] = 0
        resources_stats[r["type"]][r["format"]] += 1

    resources_stats = {datetime.now().strftime("%Y-%m-%d"): resources_stats}

    hist_dq = json.loads(
        s3_open.get_file_content(s3_destination_folder + "datasets_quality.json")
    )
    hist_dq.update(dataset_quality)
    with open(TMP_FOLDER + "datasets_quality.json", "w") as f:
        json.dump(hist_dq, f, indent=4)

    hist_rs = json.loads(
        s3_open.get_file_content(s3_destination_folder + "resources_stats.json")
    )
    hist_rs.update(resources_stats)
    with open(TMP_FOLDER + "resources_stats.json", "w") as f:
        json.dump(hist_rs, f, indent=4)

    s3_open.send_files(
        list_files=[
            File(
                source_path=TMP_FOLDER,
                source_name=output_file_name,
                dest_path=s3_destination_folder,
                dest_name=output_file_name,
            )
            for output_file_name in ["resources_stats.json", "datasets_quality.json"]
        ],
        ignore_airflow_env=True,
    )


@task()
def get_hvd_dataservices_stats() -> None:
    crawler = local_client.get_all_from_api_query("api/1/dataservices/?tags=hvd")
    count = 0
    # we can add more fields to monitor later
    of_interest = {
        "base_api_url": 0,
        "contact_point": 0,
        "description": 0,
        "endpoint_description_url": 0,
        "license": 0,
    }
    for c in crawler:
        count += 1
        for i in of_interest:
            if i != "license":
                of_interest[i] += c.get(i) is not None
            # for now, as license is not None when not specified
            else:
                of_interest[i] += c[i] != "notspecified"

    dataservices_stats = {
        datetime.now().strftime("%Y-%m-%d"): {
            "metrics": of_interest,
            "count": count,
        },
    }

    hist_ds = json.loads(
        s3_open.get_file_content(
            s3_destination_folder + "hvd_dataservices_quality.json"
        )
    )
    hist_ds.update(dataservices_stats)
    with open(TMP_FOLDER + "hvd_dataservices_quality.json", "w") as f:
        json.dump(hist_ds, f, indent=4)

    s3_open.send_file(
        File(
            source_path=TMP_FOLDER,
            source_name="hvd_dataservices_quality.json",
            dest_path=s3_destination_folder,
            dest_name="hvd_dataservices_quality.json",
        ),
        ignore_airflow_env=True,
    )
