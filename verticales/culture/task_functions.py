import json
import logging

import pandas as pd
import requests
from airflow.decorators import task
from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
    AIRFLOW_ENV,
    S3_BUCKET_DATA_PIPELINE_OPEN,
)
from datagouvfr_data_pipelines.utils.datagouv import (
    DATAGOUV_SECRET_API_KEY,
    local_client,
)
from datagouvfr_data_pipelines.utils.filesystem import File
from datagouvfr_data_pipelines.utils.grist import GristTable
from datagouvfr_data_pipelines.utils.mattermost import send_message
from datagouvfr_data_pipelines.utils.s3 import S3Client

TMP_FOLDER = f"{AIRFLOW_DAG_TMP}culture/data/"

topic_id = (
    "68889f00bd51536864e35316" if AIRFLOW_ENV == "prod" else "689604546058bf73a6c7a4eb"
)
metrics_api_url = "https://metric-api.data.gouv.fr/api/{}/data/?{}_id__exact={}"
s3_open = S3Client(bucket=S3_BUCKET_DATA_PIPELINE_OPEN)

objects = {
    "datasets": {
        "catalog_id": "f868cca6-8da1-4369-a78d-47463f19a9a3",
        "metrics_keys": {
            "monthly_visit": "visits",
            "monthly_download_resource": "downloads",
        },
    },
    "dataservices": {
        "catalog_id": "322d1475-f36a-472d-97ce-d218c8f79092",
        "metrics_keys": {"monthly_visit": "visits"},
    },
    "reuses": {
        "catalog_id": "970aafa0-3778-4d8b-b9d1-de937525e379",
        "metrics_keys": {"monthly_visit": "visits"},
    },
    "organizations": {
        # no need for the catalog, it's based on grist for the perimeter
        "metrics_keys": {"monthly_visit_dataset": "visits"}
    },
}


@task()
def get_perimeter_orgas(**context):
    table = GristTable("hrDZg8StuE1d", "Perimetre_Culture").to_dataframe()
    context["ti"].xcom_push(key="organizations", value=table["datagouv_id"].to_list())


@task()
def get_and_send_perimeter_objects(object_type: str, **context):
    orgas = context["ti"].xcom_pull(key="organizations", task_ids="get_perimeter_orgas")
    catalog_ids = pd.read_csv(
        f"https://www.data.gouv.fr/api/1/datasets/r/{objects[object_type]['catalog_id']}",
        usecols=["id", "organization_id"],
        sep=";",
        dtype=str,
    )
    catalog_ids = catalog_ids.loc[
        catalog_ids["organization_id"].isin(orgas), "id"
    ].to_list()
    # getting tags to put them back
    tags = requests.get(
        f"{local_client.base_url}/api/2/topics/{topic_id}/",
        headers={"X-API-KEY": DATAGOUV_SECRET_API_KEY, "X-fields": "tags"},
    ).json()["tags"]
    # replacing topic field with fresh data
    r = requests.put(
        f"{local_client.base_url}/api/2/topics/{topic_id}/",
        headers={"X-API-KEY": DATAGOUV_SECRET_API_KEY},
        json={object_type: catalog_ids, "tags": tags},
    )
    r.raise_for_status()
    context["ti"].xcom_push(key=object_type, value=catalog_ids)


@task()
def get_perimeter_stats(object_type: str, **context):
    ids = context["ti"].xcom_pull(
        key=object_type,
        task_ids=(
            "get_perimeter_orgas"
            if object_type == "organizations"
            else f"get_and_send_perimeter_{object_type}"
        ),
    )
    stats = []
    # we have to do a pirouette to end up with standardized labels
    total = {
        metric_label: 0
        for metric_label in objects[object_type]["metrics_keys"].values()
    } | {f"nb_{object_type}": 0}
    for idx, obj_id in enumerate(ids):
        if idx and idx % 20 == 0:
            logging.info(f"> Got stats for {idx}/{len(ids)} {object_type}")
        total[f"nb_{object_type}"] += 1
        obj_stats = {
            metric_label: 0
            for metric_label in objects[object_type]["metrics_keys"].values()
        }
        for monthly_stats in local_client.get_all_from_api_query(
            metrics_api_url.format(object_type, object_type[:-1], obj_id),
            next_page="links.next",
            _ignore_base_url=True,
        ):
            for metric_id in objects[object_type]["metrics_keys"].keys():
                obj_stats[objects[object_type]["metrics_keys"][metric_id]] += (
                    monthly_stats[metric_id] or 0
                )
                total[objects[object_type]["metrics_keys"][metric_id]] += (
                    monthly_stats[metric_id] or 0
                )
        stats.append({"id": obj_id} | obj_stats)

    context["ti"].xcom_push(key=f"detailed_{object_type}", value=stats)
    context["ti"].xcom_push(key=f"total_{object_type}", value=total)


@task()
def gather_stats(object_types: list[str], **context):
    detailed, total = {}, {}
    for object_type in object_types:
        detailed[object_type] = context["ti"].xcom_pull(
            key=f"detailed_{object_type}",
            task_ids=f"get_perimeter_stats_{object_type}",
        )
        total[object_type] = context["ti"].xcom_pull(
            key=f"total_{object_type}",
            task_ids=f"get_perimeter_stats_{object_type}",
        )

    with open(TMP_FOLDER + "detailed.json", "w") as f:
        json.dump(detailed, f)

    with open(TMP_FOLDER + "total.json", "w") as f:
        json.dump(total, f)


@task()
def send_stats_to_s3():
    s3_open.send_files(
        list_files=[
            File(
                source_path=TMP_FOLDER,
                source_name=f"{scope}.json",
                dest_path="verticale_culture/",
                dest_name=f"{scope}.json",
            )
            for scope in ["detailed", "total"]
        ],
        ignore_airflow_env=True,
        burn_after_sending=True,
    )


@task()
def refresh_datasets_tops(**context):
    orgas = context["ti"].xcom_pull(key="organizations", task_ids="get_perimeter_orgas")
    logging.info("Loading catalog...")
    datasets_catalog = pd.read_csv(
        f"https://www.data.gouv.fr/api/1/datasets/r/{objects['datasets']['catalog_id']}",
        sep=";",
        dtype=str,
        usecols=[
            "id",
            "title",
            "slug",
            "organization_id",
            "created_at",
            "metric.reuses",
            "metric.resources_downloads",
        ],
    )
    datasets_catalog = datasets_catalog.loc[
        datasets_catalog["organization_id"].isin(orgas)
    ]
    metrics = {
        "top-datasets": "metric.resources_downloads",
        "top-reuses": "metric.reuses",
        "new-datasets": "created_at",
    }
    logging.info("Refreshing table...")
    table = GristTable("hrDZg8StuE1d", "Tops")
    for top_type, column in metrics.items():
        top = datasets_catalog.sort_values(by=column, ascending=False)
        for idx, (_, row) in enumerate(top.iterrows()):
            if idx > 2:
                # only looking for top 3
                break
            table.update_records(
                conditions={"type": top_type, "ordre": idx + 1},
                new_values={
                    "titre": row["title"],
                    "slug": row["slug"],
                    "id2": row["id"],
                },
            )


@task()
def send_notification_mattermost():
    send_message(
        text=":performing_arts: Catalogue et stats de la verticale culture mis à jour."
    )
