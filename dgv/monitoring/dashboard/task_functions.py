import json
import logging
from datetime import datetime, timedelta
from io import StringIO
from time import sleep
from typing import Iterator

import numpy as np
import pandas as pd
import requests
from airflow.models import TaskInstance

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
    SECRET_ZAMMAD_API_TOKEN,
    SECRET_ZAMMAD_API_URL,
)
from datagouvfr_data_pipelines.utils.datagouv import (
    get_all_from_api_query,
    # DATAGOUV_MATOMO_ID,
)
from datagouvfr_data_pipelines.utils.minio import File, MinIOClient
from datagouvfr_data_pipelines.utils.utils import list_months_between

DAG_NAME = "dgv_dashboard"
DATADIR = f"{AIRFLOW_DAG_TMP}{DAG_NAME}/data/"
one_year_ago = datetime.today() - timedelta(days=365)
groups = [
    k + "@data.gouv.fr"
    for k in ["support", "ouverture", "moissonnage", "certification", "geo", "cadastre", "candidature"]
]
entreprises_api_url = "https://recherche-entreprises.api.gouv.fr/search?q="
# max 5 requests/second (rate limiting is 1/7)
rate_limiting_delay = 1 / 5

minio_open = MinIOClient(bucket="dataeng-open")
minio_destination_folder = "dashboard/"


def try_to_get_ticket_count(
    year_month: str,
    tags: list[str] = [],
    per_page: int = 200,
) -> int:
    session = requests.Session()
    session.headers = {"Authorization": "Bearer " + SECRET_ZAMMAD_API_TOKEN}

    query = f"created_at:[{year_month}-01 TO {year_month}-31]"
    query += f" AND (group.name:{' OR group.name:'.join(groups)})"
    if tags:
        query += f" AND (tags:{' OR tags:'.join(tags)})"
    page = 1
    params: dict[str, str | int] = {
        "query": query,
        "page": page,
        "per_page": per_page,
    }
    res = session.get(f"{SECRET_ZAMMAD_API_URL}tickets/search", params=params).json()["tickets_count"]
    batch = res
    while batch == per_page:
        page += 1
        params["page"] = page
        batch = session.get(f"{SECRET_ZAMMAD_API_URL}tickets/search", params=params).json()["tickets_count"]
        res += batch
    logging.info(f"{res} tickets found for month {year_month} across {page} pages")
    return res


def get_monthly_tickets(
    year_month: str,
    tags: list[str] = [],
) -> int:
    # after investigation, it appears that *sometimes* the API doesn't return the
    # same amount of tickets for a given month if you change the "per_page" parameter
    # so if we end up in this situation, we try a bunch of values to get closer to the result
    # but it's still not perfectly accurate...
    nb_tickets: list[int] = []
    per_page = 200
    while len(nb_tickets) < 20 and not (len(nb_tickets) > 3 and len(set(nb_tickets)) == 1):
        nb_tickets.append(try_to_get_ticket_count(year_month, tags=tags, per_page=per_page))
        if len(nb_tickets) > 1:
            if nb_tickets[-1] > nb_tickets[-2]:
                per_page = round(per_page * 1.3)
            else:
                per_page = round(per_page / 1.2)
    logging.info(f"Number of tickets: {nb_tickets}")
    return max(nb_tickets)


def get_zammad_tickets(
    ti: TaskInstance,
    start_date: datetime,
    end_date: datetime = datetime.today(),
):
    hs_tags = [
        "HORS-SUJET",
        '"HORS SUJET"',
        "RNA",
        # quotes are mandatory if tag has blanks
        '"TITRE DE SEJOUR"',
        "DECES",
        "QUALIOPI",
        "IMPOT",
    ]

    spam_tags = [
        "SPAM",
        "spam",
    ]

    all_tickets, hs_tickets, spam_tickets = [], [], []
    months = list_months_between(start_date, end_date)
    for month in months:
        logging.info("Searching all tickets...")
        all_tickets.append(get_monthly_tickets(month))
        logging.info("Searching HS tickets...")
        hs_tickets.append(get_monthly_tickets(month, tags=hs_tags))
        logging.info("Searching spam tickets...")
        spam_tickets.append(get_monthly_tickets(month, tags=spam_tags))

    ti.xcom_push(key="all_tickets", value=all_tickets)
    ti.xcom_push(key="hs_tickets", value=hs_tickets)
    ti.xcom_push(key="spam_tickets", value=spam_tickets)
    ti.xcom_push(key="months", value=months)


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


def get_visits(
    ti: TaskInstance,
    start_date: datetime,
    end_date: datetime = datetime.today(),
) -> None:
    # url_stats_home_dgv = {
    #     "site_id": DATAGOUV_MATOMO_ID,
    #     "label": "fr",
    #     "title": "Homepage",
    # }

    months_to_process = list_months_between(start_date, end_date)
    url_stats_support = {
        "site_id": "176",
        "label": "%40%252Findex",
        "title": "support",
    }
    for k in [
        # not taking the stats from the homepage, no variation
        # url_stats_home_dgv,
        url_stats_support
    ]:
        r = requests.get(
            fill_url(
                start=start_date.strftime("%Y-%m-%d"),
                end=end_date.strftime("%Y-%m-%d"),
                site_id=k["site_id"],
                label=k["label"],
            )
        )
        df = pd.read_csv(
            filepath_or_buffer=StringIO(r.text),
            usecols=["Date", "Vues de page uniques"],
        )

        # Filling missing months to 0 in case Matomo did not return them yet
        for month in months_to_process:
            if month not in df["Date"].values:
                logging.error(f"Missing month: {month}")
                new_values = pd.DataFrame({"Date": [month], "Vues de page uniques": [0]})
                df = pd.concat([new_values, df], ignore_index=True)
                df = df.sort_values(by="Date")

        vues = df["Vues de page uniques"].to_list()
        logging.info(f"Vues : {vues}")
        ti.xcom_push(key=k["title"], value=vues)


def gather_and_upload(
    ti: TaskInstance,
) -> None:
    all_tickets = ti.xcom_pull(key="all_tickets", task_ids="get_zammad_tickets")
    hs_ticket = ti.xcom_pull(key="hs_tickets", task_ids="get_zammad_tickets")
    spam_tickets = ti.xcom_pull(key="spam_tickets", task_ids="get_zammad_tickets")
    months = ti.xcom_pull(key="months", task_ids="get_zammad_tickets")
    # homepage = ti.xcom_pull(key="homepage", task_ids="get_visits")
    support = ti.xcom_pull(key="support", task_ids="get_visits")

    stats = pd.DataFrame(
        {
            # 'Homepage': homepage,
            "Page support": support,
            "Ouverture de ticket": all_tickets,
            "Ticket hors-sujet": hs_ticket,
            "Ticket spam": spam_tickets,
        },
        index=months,
    ).T
    # removing current month from stats
    stats = stats[stats.columns[:-1]]
    stats.to_csv(DATADIR + "stats_support.csv")

    # sending to minio
    minio_open.send_files(
        list_files=[
            File(
                source_path=DATADIR,
                source_name="stats_support.csv",
                dest_path=minio_destination_folder,
                dest_name="stats_support.csv",
            )
        ],
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
    except Exception as _:
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
    return complements["collectivite_territoriale"] or complements["est_service_public"], issue


def get_and_upload_certification() -> None:
    session = requests.Session()
    orgas = get_all_from_api_query(
        "https://www.data.gouv.fr/api/1/organizations", mask="data{id,badges,business_number_id}"
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
    with open(DATADIR + "certified.json", "w") as f:
        json.dump(certified, f, indent=4)
    with open(DATADIR + "SP_or_CT.json", "w") as f:
        json.dump(SP_or_CT, f, indent=4)
    with open(DATADIR + "issues.json", "w") as f:
        json.dump(issues, f, indent=4)

    minio_open.send_files(
        list_files=[
            File(
                source_path=DATADIR,
                source_name=f,
                dest_path=minio_destination_folder + datetime.now().strftime("%Y-%m-%d") + "/",
                dest_name=f,
            )
            for f in ["certified.json", "SP_or_CT.json", "issues.json"]
        ],
        ignore_airflow_env=True,
    )


def get_and_upload_reuses_down() -> None:
    client = MinIOClient(bucket="data-pipeline-open")
    # getting latest data
    df = pd.read_csv(StringIO(client.get_file_content("prod/bizdev/all_reuses_most_visits_KO_last_month.csv")))
    stats = pd.DataFrame(df["error"].apply(lambda x: x if x == "404" else "Autre erreur").value_counts()).T
    stats["Date"] = [datetime.now().strftime("%Y-%m-%d")]
    stats["Total"] = [
        requests.get(
            "https://www.data.gouv.fr/api/1/reuses/",
            headers={"X-fields": "total"},
        ).json()["total"]
    ]

    # getting historical data
    output_file_name = "stats_reuses_down.csv"
    hist = pd.read_csv(StringIO(minio_open.get_file_content(minio_destination_folder + output_file_name)))
    start_len = len(hist)
    hist = pd.concat([hist, stats]).drop_duplicates("Date")
    # just in case
    assert start_len <= len(hist)
    hist.to_csv(DATADIR + output_file_name, index=False)
    minio_open.send_files(
        list_files=[
            File(
                source_path=DATADIR,
                source_name=output_file_name,
                dest_path=minio_destination_folder,
                dest_name=output_file_name,
            )
        ],
        ignore_airflow_env=True,
    )


def get_catalog_stats() -> None:
    datasets = []
    resources = []
    crawler = get_all_from_api_query(
        "https://www.data.gouv.fr/api/1/datasets/", mask="data{id,harvest,quality,tags,resources{id,format,type}}"
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
    dataset_quality = {k: {c: [] for c in cats} for k in ["all", "harvested", "local", "hvd"]}
    dataset_quality.update({"count": {k: 0 for k in ["all", "harvested", "local", "hvd"]}})
    for d in datasets:
        dataset_quality["count"]["all"] += 1
        dataset_quality["count"]["harvested" if d[1] else "local"] += 1
        if "hvd" in d[3]:
            dataset_quality["count"]["hvd"] += 1
        for c in cats:
            dataset_quality["all"][c].append(d[2].get(c, False) or False)
            dataset_quality["harvested" if d[1] else "local"][c].append(d[2].get(c, False) or False)
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

    hist_dq = json.loads(minio_open.get_file_content(minio_destination_folder + "datasets_quality.json"))
    hist_dq.update(dataset_quality)
    with open(DATADIR + "datasets_quality.json", "w") as f:
        json.dump(hist_dq, f, indent=4)

    hist_rs = json.loads(minio_open.get_file_content(minio_destination_folder + "resources_stats.json"))
    hist_rs.update(resources_stats)
    with open(DATADIR + "resources_stats.json", "w") as f:
        json.dump(hist_rs, f, indent=4)

    minio_open.send_files(
        list_files=[
            File(
                source_path=DATADIR,
                source_name=output_file_name,
                dest_path=minio_destination_folder,
                dest_name=output_file_name,
            )
            for output_file_name in ["resources_stats.json", "datasets_quality.json"]
        ],
        ignore_airflow_env=True,
    )


def get_hvd_dataservices_stats() -> None:
    crawler = get_all_from_api_query("https://www.data.gouv.fr/api/1/dataservices/?tags=hvd")
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

    hist_ds = json.loads(minio_open.get_file_content(minio_destination_folder + "hvd_dataservices_quality.json"))
    hist_ds.update(dataservices_stats)
    with open(DATADIR + "hvd_dataservices_quality.json", "w") as f:
        json.dump(hist_ds, f, indent=4)

    minio_open.send_files(
        list_files=[
            File(
                source_path=DATADIR,
                source_name="hvd_dataservices_quality.json",
                dest_path=minio_destination_folder,
                dest_name="hvd_dataservices_quality.json",
            )
        ],
        ignore_airflow_env=True,
    )
