from datetime import datetime, timedelta
import json
import os
import random

import aiohttp
from airflow.decorators import task
import asyncio
from langdetect import detect
import pandas as pd

from datagouvfr_data_pipelines.config import (
    AIRFLOW_ENV,
    AIRFLOW_DAG_TMP,
    S3_BUCKET_DATA_PIPELINE_OPEN,
    MATTERMOST_MODERATION_NOUVEAUTES,
    MATTERMOST_DATAGOUV_EDITO,
)
from datagouvfr_data_pipelines.utils.filesystem import File
from datagouvfr_data_pipelines.utils.mattermost import send_message
from datagouvfr_data_pipelines.utils.s3 import S3Client
from datagouvfr_data_pipelines.utils.datagouv import (
    local_client,
    SPAM_WORDS,
)
from datagouvfr_data_pipelines.utils.grist import GRIST_UI_URL, GristTable
from datagouvfr_data_pipelines.utils.retry import RequestRetry

DAG_NAME = "dgv_bizdev"
TMP_FOLDER = f"{AIRFLOW_DAG_TMP}{DAG_NAME}/"
api_metrics_url = "https://metric-api.data.gouv.fr/"
grist_edito = "4MdJUBsdSgjE"
grist_curation = "muvJRZ9cTGep"
s3_open = S3Client(bucket=S3_BUCKET_DATA_PIPELINE_OPEN)

today = datetime.today()
first_day_of_current_month = today.replace(day=1)
last_day_of_last_month = first_day_of_current_month - timedelta(days=1)
last_month = last_day_of_last_month.strftime("%Y-%m")

ignored_reuses = [
    "5cc31646634f415773630550",
    "6551ad5de16abd15501bb229",
    "60ba9c29b5cf00b439cf2db2",
    "60336c61169032c050241917",
    "622603f40afc69ae230ba3bb",
    "635c23d415ba3a284e9f6b55",
    "620619bf57530fafb331840f",
    "60b2d449fa46c495f9b6fcc4",
    "63ad24474cbeaae107477cf5",
    "5e3439408b4c412dc0d6d48c",
    "6066cb811b31b4046459152c",
    "63b6ac225e830edcc13a33bd",
    "62196bdfa6f0568851c3acf5",
    "5a2ebeb388ee3876ffcde5d4",
    "62a11452e6f309c60cd669b1",
    "62a11367699461f7c1eeb990",
    "62a11367699461f7c1eeb990",
    "5d0fff996f44410df0661086",
    "61e156cb03b7712be8a87767",
    "60c757480e323442ef3da882",
    "5dc005c66f44415fb80731a3",
    "63a34633c78e0630594e351f",
    "5b3cbc68c751df3c3e3aaa9c",
    "61bc9d5d0b4aa67cb0b239b2",
    "57ffa6d288ee3858375ff490",
    "64133a9ea43892071bef7084",
    "5c8b75b6634f41525cfa1629",
    "65406410e2c5c54317513866",
    "60edbed1dd659431fcf45e04",
]
ignored_users = [
    "534fff3ea3a7292c64a774e4",
]


async def url_error(url, session, method="head"):
    agents = [
        (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/99.0.4844.83 Safari/537.36"
        ),
        (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/99.0.4844.51 Safari/537.36"
        ),
        (
            "Mozilla/5.0 (iPad; CPU OS 12_2 like Mac OS X) "
            "AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148"
        ),
    ]
    try:
        _method = getattr(session, method)
        async with _method(
            url,
            timeout=15,
            allow_redirects=True,
            headers={"User-Agent": random.choice(agents)},
        ) as r:
            if r.status in [301, 302, 303, 401, 403, 404, 405, 500]:
                if method == "head":
                    # HEAD might not be allowed or correctly implemented, trying with GET
                    return await url_error(url, session, method="get")
            r.raise_for_status()
        return False
    except Exception as e:
        return getattr(e, "status", str(e))


async def crawl_reuses(reuses):
    async with aiohttp.ClientSession() as session:
        url_errors = await asyncio.gather(
            *[url_error(reuse["url"], session) for reuse in reuses]
        )

    unavailable_reuses = [
        (reuse, error) for reuse, error in zip(reuses, url_errors) if error
    ]
    return unavailable_reuses


def get_unavailable_reuses():
    print("Fetching reuse list from https://www.data.gouv.fr/api/1/reuses/")
    reuses = json.loads(
        pd.read_csv(
            "https://www.data.gouv.fr/api/1/datasets/r/970aafa0-3778-4d8b-b9d1-de937525e379",
            sep=";",
            usecols=["id", "url"],
        ).to_json(orient="records")
    )
    reuses = [r for r in reuses if r["id"] not in ignored_reuses]
    # gather on the whole reuse list is inconsistent (some unavailable reuses do not
    # appear), having a smaller batch size provides better results and keeps the duration
    # acceptable (compared to synchronous process)
    batch_size = 50
    print(f"Checking {len(reuses)} reuses with batch size of", batch_size)
    unavailable_reuses = []
    for k in range(len(reuses) // batch_size + 1):
        batch = reuses[k * batch_size : min((k + 1) * batch_size, len(reuses))]
        current = len(unavailable_reuses)
        unavailable_reuses += asyncio.run(crawl_reuses(batch))
        print(f"Batch n°{k + 1} errors: ", len(unavailable_reuses) - current)
    print("All errors:", len(unavailable_reuses))
    return unavailable_reuses


async def classify_user(user):
    try:
        if user["about"]:
            if any([sus in user["about"] for sus in ["http", "www."]]):
                return {
                    "type": "users",
                    "url": f"https://www.data.gouv.fr/admin/users/{user['id']}/profile",
                    "name_or_title": None,
                    "creator": None,
                    "id": user["id"],
                    "spam_word": "web address",
                    "nb_datasets_and_reuses": user["metrics"]["datasets"]
                    + user["metrics"]["reuses"],
                    "about": user["about"][:1000],
                }
            elif detect(user["about"]) != "fr":
                return {
                    "type": "users",
                    "url": f"https://www.data.gouv.fr/admin/users/{user['id']}/profile",
                    "name_or_title": None,
                    "creator": None,
                    "id": user["id"],
                    "spam_word": "language",
                    "nb_datasets_and_reuses": user["metrics"]["datasets"]
                    + user["metrics"]["reuses"],
                    "about": user["about"][:1000],
                }
            else:
                return None
    except Exception:
        return {
            "type": "users",
            "url": f"https://www.data.gouv.fr/admin/users/{user['id']}/profile",
            "name_or_title": None,
            "creator": None,
            "id": user["id"],
            "spam_word": "suspect error",
            "nb_datasets_and_reuses": user["metrics"]["datasets"]
            + user["metrics"]["reuses"],
            "about": user["about"][:1000],
        }


async def get_suspect_users():
    users = local_client.get_all_from_api_query(
        "api/1/users/",
        mask="data{id,about,metrics}",
    )
    tasks = [asyncio.create_task(classify_user(k)) for k in users]
    results = await asyncio.gather(*tasks)
    return results


@task()
def process_unavailable_reuses():
    unavailable_reuses = get_unavailable_reuses()
    restr_reuses = {d[0]["id"]: {"error": d[1]} for d in unavailable_reuses}
    if not restr_reuses:
        return
    for rid in restr_reuses:
        data = local_client.get_all_from_api_query(
            f"{api_metrics_url}api/reuses/data/?metric_month__exact={last_month}&reuse_id__exact={rid}",
            next_page="links.next",
            _ignore_base_url=True,
        )
        try:
            d = next(data)
            restr_reuses[rid].update({"monthly_visit": d["monthly_visit"]})
        except StopIteration:
            restr_reuses[rid].update({"monthly_visit": 0})

        r = RequestRetry.get("https://www.data.gouv.fr/api/1/reuses/" + rid).json()
        restr_reuses[rid].update(
            {
                "title": r.get("title", None),
                "page": r.get("page", None),
                "url": r.get("url", None),
                "creator": (
                    r.get("organization", None).get("name", None)
                    if r.get("organization", None)
                    else r.get("owner", {}).get("slug", None)
                    if r.get("owner", None)
                    else None
                ),
            }
        )
    df = pd.DataFrame(restr_reuses.values(), index=restr_reuses.keys())
    df = df.sort_values("monthly_visit", ascending=False)
    df.to_csv(
        TMP_FOLDER + "all_reuses_most_visits_KO_last_month.csv",
        float_format="%.0f",
        index=False,
    )
    GristTable(grist_curation, "Reutilisations_down").from_dataframe(df)


@task()
def process_empty_datasets():
    def get_last_month_visits(dataset_id: str):
        data = local_client.get_all_from_api_query(
            (
                f"{api_metrics_url}api/organizations/data/?metric_month__exact={last_month}"
                f"&dataset_id__exact={dataset_id}"
            ),
            next_page="links.next",
            _ignore_base_url=True,
        )
        try:
            n = next(data)
            return n["monthly_visit"]
        except Exception:
            return 0

    empty_datasets = pd.read_csv(
        "https://www.data.gouv.fr/api/1/datasets/r/f868cca6-8da1-4369-a78d-47463f19a9a3",
        sep=";",
        usecols=[
            "id",
            "title",
            "organization",
            "owner",
            "created_at",
            "resources_count",
        ],
    )
    empty_datasets = empty_datasets.loc[empty_datasets["resources_count"] == 0]
    del empty_datasets["resources_count"]
    empty_datasets["url"] = empty_datasets["id"].apply(
        lambda _id: f"https://www.data.gouv.fr/admin/datasets/{_id}"
    )
    empty_datasets["created_at"] = empty_datasets["created_at"].str.slice(0, 10)
    empty_datasets["organization_or_owner"] = empty_datasets.apply(
        lambda df: (
            df["organization"] if isinstance(df["organization"], str) else df["owner"]
        ),
        axis=1,
    )
    empty_datasets["last_month_visits"] = empty_datasets["id"].apply(
        get_last_month_visits
    )
    empty_datasets.rename({"id": "dataset_id"}, axis=1, inplace=True)
    empty_datasets = empty_datasets.sort_values("last_month_visits", ascending=False)
    empty_datasets.to_csv(
        TMP_FOLDER + "empty_datasets.csv", float_format="%.0f", index=False
    )
    GristTable(grist_curation, "Datasets_vides").from_dataframe(empty_datasets)


@task()
def process_potential_spam():
    search_types = [
        "datasets",
        "reuses",
        "organizations",
        "users",
    ]
    spam = []
    for obj in search_types:
        print("   - Starting with", obj)
        for word in SPAM_WORDS:
            data = local_client.get_all_from_api_query(
                f"api/1/{obj}/?q={word}",
                mask="data{badges,organization,owner,id,name,title,metrics,created_at,since}",
            )
            for d in data:
                should_add = True
                # si l'objet est ou provient d'une orga certifiée => pas spam
                if obj != "users":
                    if obj == "organizations":
                        badges = d.get("badges", [])
                    else:
                        badges = (
                            d["organization"].get("badges", [])
                            if d.get("organization", None)
                            else []
                        )
                    if "certified" in [badge["kind"] for badge in badges]:
                        should_add = False
                if obj in ["datasets", "reuses"]:
                    if d.get("owner") and d["owner"]["id"] in ignored_users:
                        should_add = False
                if should_add:
                    spam.append(
                        {
                            "type": obj,
                            "url": f"https://www.data.gouv.fr/admin/{obj}/{d['id']}",
                            "name_or_title": d.get("name", d.get("title", None)),
                            "creator": (
                                d["organization"].get("name", None)
                                if d.get("organization", None)
                                else d["owner"].get("slug", None)
                                if d.get("owner", None)
                                else None
                            ),
                            "created_at": d.get("created_at", d.get("since"))[:10],
                            "id": d["id"],
                            "spam_word": word,
                            "nb_datasets_and_reuses": (
                                None
                                if obj not in ["organizations", "users"]
                                else d["metrics"]["datasets"] + d["metrics"]["reuses"]
                            ),
                        }
                    )
    print("Détection d'utilisateurs suspects...")
    suspect_users = asyncio.run(get_suspect_users())
    spam.extend([u for u in suspect_users if u])
    df = pd.DataFrame(spam).drop_duplicates(subset="url")
    df.to_csv(TMP_FOLDER + "objects_with_spam_word.csv", index=False)
    GristTable(grist_curation, "Spam").from_dataframe(df)


curation_tasks = [
    process_unavailable_reuses(),
    process_empty_datasets(),
    process_potential_spam(),
]


# %%
@task()
def get_top_orgas_publish():
    # Top 50 des orgas ayant produits le plus de JDD
    datasets = pd.read_csv(
        "https://www.data.gouv.fr/api/1/datasets/r/f868cca6-8da1-4369-a78d-47463f19a9a3",
        sep=";",
        usecols=["organization", "organization_id", "created_at"],
    ).rename({"organization": "name"}, axis=1)
    threshold = (datetime.today() - timedelta(days=30)).strftime("%Y-%m-%d")
    restr = datasets.loc[datasets["created_at"] >= threshold]
    restr["url"] = restr["organization_id"].apply(
        lambda _id: f"www.data.gouv.fr/organizations/{_id}/"
    )
    count = (
        restr["organization_id"]
        .value_counts()
        .reset_index()
        .rename({"count": "publications"}, axis=1)
    )
    count = pd.merge(
        count,
        restr[["organization_id", "url"]].drop_duplicates(subset="organization_id"),
        on="organization_id",
        how="left",
    ).sort_values(by="publications", ascending=False)
    count[:50].to_csv(
        TMP_FOLDER + "top50_orgas_most_publications_30_days.csv", index=False
    )
    GristTable(grist_edito, "Top50_organisations_publications_1mois").from_dataframe(
        count
    )


@task()
def get_top_orgas_visits():
    # Top 50 des orgas les plus visités
    data = local_client.get_all_from_api_query(
        f"{api_metrics_url}api/organizations/data/?metric_month__exact={last_month}",
        next_page="links.next",
        _ignore_base_url=True,
    )
    orga_visited = {
        d["organization_id"]: {
            "monthly_visit_dataset": d["monthly_visit_dataset"],
            "monthly_download_resource": d["monthly_download_resource"],
        }
        for d in data
        if d["monthly_visit_dataset"] or d["monthly_download_resource"]
    }
    tmp = list(
        {
            k: v
            for k, v in sorted(
                orga_visited.items(),
                key=lambda x: (
                    -x[1]["monthly_visit_dataset"]
                    if x[1]["monthly_visit_dataset"]
                    else 0
                ),
            )
        }.keys()
    )[:50]
    tmp2 = list(
        {
            k: v
            for k, v in sorted(
                orga_visited.items(),
                key=lambda x: (
                    -x[1]["monthly_download_resource"]
                    if x[1]["monthly_download_resource"]
                    else 0
                ),
            )
        }.keys()
    )[:50]
    orga_visited = {k: orga_visited[k] for k in orga_visited if k in tmp or k in tmp2}
    for k in orga_visited:
        r = RequestRetry.get("https://www.data.gouv.fr/api/1/organizations/" + k).json()
        orga_visited[k].update(
            {"name": r.get("name", None), "url": r.get("page", None)}
        )
    df = pd.DataFrame(orga_visited.values(), index=orga_visited.keys())
    df.to_csv(TMP_FOLDER + "top50_orgas_most_visits_last_month.csv", index=False)
    GristTable(grist_edito, "Top50_organisations_visites_1mois").from_dataframe(df)


@task()
def get_top_datasets_visits():
    # Top 50 des JDD les plus visités
    data = local_client.get_all_from_api_query(
        f"{api_metrics_url}api/datasets/data/?metric_month__exact={last_month}&monthly_visit__sort=desc",
        next_page="links.next",
        _ignore_base_url=True,
    )
    datasets_visited = {}
    for k in range(50):
        d = next(data)
        datasets_visited.update(
            {d["dataset_id"]: {"monthly_visit": d["monthly_visit"]}}
        )
    datasets_visited = {
        k: v
        for k, v in sorted(
            datasets_visited.items(), key=lambda x: -x[1]["monthly_visit"]
        )
    }
    for k in datasets_visited:
        r = RequestRetry.get("https://www.data.gouv.fr/api/1/datasets/" + k).json()
        datasets_visited[k].update(
            {
                "title": r.get("title", None),
                "url": r.get("page", None),
                "organization_or_owner": (
                    r["organization"].get("name", None)
                    if r.get("organization", None)
                    else r["owner"].get("slug", None)
                    if r.get("owner", None)
                    else None
                ),
            }
        )
    df = pd.DataFrame(datasets_visited.values(), index=datasets_visited.keys())
    df.to_csv(TMP_FOLDER + "top50_datasets_most_visits_last_month.csv", index=False)
    GristTable(grist_edito, "Top50_datasets_visites_1mois").from_dataframe(df)


@task()
def get_top_resources_downloads():
    # Top 50 des ressources les plus téléchargées
    data = local_client.get_all_from_api_query(
        (
            f"{api_metrics_url}api/resources/data/?metric_month__exact={last_month}"
            "&monthly_download_resource__sort=desc"
        ),
        next_page="links.next",
        _ignore_base_url=True,
    )
    resources_downloaded = {}
    while len(resources_downloaded) < 50:
        d = next(data)
        if d["monthly_download_resource"]:
            if not RequestRetry.get(
                f"https://www.data.gouv.fr/api/1/datasets/{d['dataset_id']}/"
            ).ok:
                print("This dataset seems to have disappeared:", d["dataset_id"])
                continue
            r = RequestRetry.get(
                f"https://www.data.gouv.fr/api/2/datasets/resources/{d['resource_id']}/"
            ).json()
            d["dataset_id"] = r["dataset_id"] if r["dataset_id"] else "COMMUNAUTARY"
            resource_title = r["resource"]["title"]
            r2 = {}
            r3 = {}
            if d["dataset_id"] != "COMMUNAUTARY":
                r2 = RequestRetry.get(
                    f"https://www.data.gouv.fr/api/1/datasets/{d['dataset_id']}/"
                ).json()
                r3 = RequestRetry.get(
                    f"{api_metrics_url}api/datasets/data/?metric_month__exact={last_month}"
                    f"&dataset_id__exact={d['dataset_id']}"
                ).json()
            resources_downloaded.update(
                {
                    f"{d['dataset_id']}.{d['resource_id']}": {
                        "monthly_download_resourced": d["monthly_download_resource"],
                        "resource_title": resource_title,
                        "dataset_url": (
                            f"https://www.data.gouv.fr/datasets/{d['dataset_id']}/"
                            if d["dataset_id"] != "COMMUNAUTARY"
                            else None
                        ),
                        "dataset_title": r2.get("title", None),
                        "organization_or_owner": (
                            r2["organization"].get("name", None)
                            if r2.get("organization", None)
                            else r2["owner"].get("slug", None)
                            if r2.get("owner", None)
                            else None
                        ),
                        "dataset_total_resource_visits": (
                            r3["data"][0].get("monthly_download_resource", None)
                            if d["dataset_id"] != "COMMUNAUTARY" and r3["data"]
                            else None
                        ),
                    }
                }
            )
    resources_downloaded = {
        k: v
        for k, v in sorted(
            resources_downloaded.items(),
            key=lambda x: -x[1]["monthly_download_resourced"],
        )
    }
    df = pd.DataFrame(resources_downloaded.values(), index=resources_downloaded.keys())
    df = df.sort_values(
        ["dataset_total_resource_visits", "monthly_download_resourced"], ascending=False
    )
    df.to_csv(
        TMP_FOLDER + "top50_resources_most_downloads_last_month.csv",
        float_format="%.0f",
        index=False,
    )
    GristTable(grist_edito, "Top50_ressources_telechargees_1mois").from_dataframe(df)


@task()
def get_top_reuses_visits():
    # Top 50 des réutilisations les plus visitées
    data = local_client.get_all_from_api_query(
        f"{api_metrics_url}api/reuses/data/?metric_month__exact={last_month}&monthly_visit__sort=desc",
        next_page="links.next",
        _ignore_base_url=True,
    )
    reuses_visited = {}
    while len(reuses_visited) < 50:
        d = next(data)
        if d["monthly_visit"]:
            reuses_visited.update(
                {d["reuse_id"]: {"monthly_visit": d["monthly_visit"]}}
            )
    reuses_visited = {
        k: v
        for k, v in sorted(reuses_visited.items(), key=lambda x: -x[1]["monthly_visit"])
    }
    for k in reuses_visited:
        r = RequestRetry.get("https://www.data.gouv.fr/api/1/reuses/" + k).json()
        reuses_visited[k].update(
            {
                "title": r.get("title", None),
                "url": r.get("page", None),
                "creator": (
                    r["organization"].get("name", None)
                    if r.get("organization", None)
                    else r["owner"].get("slug", None)
                    if r.get("owner", None)
                    else None
                ),
            }
        )
    df = pd.DataFrame(reuses_visited.values(), index=reuses_visited.keys())
    df.to_csv(TMP_FOLDER + "top50_reuses_most_visits_last_month.csv", index=False)
    GristTable(grist_edito, "Top50_reutilisations_visites_1mois").from_dataframe(df)


@task()
def get_top_datasets_discussions():
    # Top 50 des JDD les plus discutés
    discussions = pd.read_csv(
        "https://www.data.gouv.fr/api/1/datasets/r/d77705e1-4ecd-461c-8c24-662d47c4c2f9",
        sep=";",
        usecols=["created", "subject_class", "subject_id"],
    )
    threshold = (datetime.today() - timedelta(days=30)).strftime("%Y-%m-%d")
    discussions = discussions.loc[
        (discussions["created"] >= threshold)
        & (discussions["subject_class"] == "Dataset")
    ]
    discussions_of_interest = {}
    for dataset_id in discussions["subject_id"]:
        if dataset_id not in discussions_of_interest:
            tmp = RequestRetry.get(
                f"https://www.data.gouv.fr/api/1/datasets/{dataset_id}"
            ).json()
            organization_or_owner = (
                tmp.get("organization", {}).get("name", None)
                if tmp.get("organization", None)
                else tmp.get("owner", {}).get("slug", None)
            )
            discussions_of_interest[dataset_id] = {
                "discussions_created": 0,
                "dataset_id": dataset_id,
                "dataset_url": f"https://www.data.gouv.fr/datasets/{dataset_id}",
                "organization_or_owner": organization_or_owner,
            }
        discussions_of_interest[dataset_id]["discussions_created"] += 1
    discussions_of_interest = {
        k: v
        for k, v in sorted(
            discussions_of_interest.items(), key=lambda x: -x[1]["discussions_created"]
        )
    }
    df = pd.DataFrame(
        discussions_of_interest.values(), index=discussions_of_interest.keys()
    )
    df.to_csv(TMP_FOLDER + "top50_orgas_most_discussions_30_days.csv", index=False)
    GristTable(grist_edito, "Top50_orgas_discutees_30jours").from_dataframe(df)


edito_tasks = [
    get_top_datasets_discussions(),
    get_top_datasets_visits(),
    get_top_orgas_publish(),
    get_top_orgas_visits(),
    get_top_resources_downloads(),
    get_top_reuses_visits(),
]


# %%
@task(trigger_rule="none_skipped")
def send_tables_to_s3():
    print(os.listdir(TMP_FOLDER))
    print("Saving tops as millésimes")
    s3_open.send_files(
        list_files=[
            File(
                source_path=TMP_FOLDER,
                source_name=file,
                dest_path=f"bizdev/{today.strftime('%Y-%m-%d')}/",
                dest_name=file,
            )
            for file in os.listdir(TMP_FOLDER)
            if not any([k in file for k in ["spam", "KO"]])
        ],
    )
    print("Saving KO reuses and spams (erasing previous files)")
    s3_open.send_files(
        list_files=[
            File(
                source_path=TMP_FOLDER,
                source_name=file,
                dest_path="bizdev/",
                dest_name=file,
            )
            for file in os.listdir(TMP_FOLDER)
            if any([k in file for k in ["spam", "KO"]])
        ],
    )


@task(trigger_rule="none_skipped")
def publish_mattermost():
    print("Publishing on mattermost")
    list_curation = ["empty", "spam", "KO"]
    curation = [
        f for f in os.listdir(TMP_FOLDER) if any([k in f for k in list_curation])
    ]
    if curation:
        print("   - Files for curation:")
        print(curation)
        message = ":zap: Les rapports bizdev curation sont disponibles "
        message += f"dans [grist]({GRIST_UI_URL + grist_curation}):"
        for file in curation:
            url = f"https://object.files.data.gouv.fr/{S3_BUCKET_DATA_PIPELINE_OPEN}/{AIRFLOW_ENV}"
            if any([k in file for k in ["spam", "KO"]]):
                url += f"/bizdev/{file}"
            else:
                url += f"/bizdev/{datetime.today().strftime('%Y-%m-%d')}/{file}"
            message += f"\n - [{file} ⬇️]({url})"
        send_message(message, MATTERMOST_MODERATION_NOUVEAUTES)

    edito = [f for f in os.listdir(TMP_FOLDER) if f not in curation]
    if edito:
        print("   - Files for édito:")
        print(edito)
        message = ":zap: Les rapports bizdev édito sont disponibles "
        message += f"dans [grist]({GRIST_UI_URL + grist_edito}):"
        for file in edito:
            url = f"https://object.files.data.gouv.fr/{S3_BUCKET_DATA_PIPELINE_OPEN}/{AIRFLOW_ENV}"
            if any([k in file for k in ["spam", "KO"]]):
                url += f"/bizdev/{file}"
            else:
                url += f"/bizdev/{datetime.today().strftime('%Y-%m-%d')}/{file}"
            message += f"\n - [{file} ⬇️]({url})"
        send_message(message, MATTERMOST_DATAGOUV_EDITO)
