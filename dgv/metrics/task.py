from collections import defaultdict
import glob
import logging
import os
import tarfile
from datetime import date, datetime, timedelta

import pandas as pd
from tqdm import tqdm

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
    MINIO_BUCKET_INFRA,
)
from datagouvfr_data_pipelines.dgv.metrics.task_functions import (
    get_catalog_id_mapping,
    parse_logs,
    sum_outlinks_by_orga,
    get_matomo_outlinks,
)
from datagouvfr_data_pipelines.utils.download import download_files
from datagouvfr_data_pipelines.utils.filesystem import remove_files_from_directory, File
from datagouvfr_data_pipelines.utils.minio import MinIOClient
from datagouvfr_data_pipelines.utils.postgres import PostgresClient
from datagouvfr_data_pipelines.dgv.metrics.config import MetricsConfig
from datagouvfr_data_pipelines.utils.utils import get_unique_list


tqdm.pandas(desc="pandas progress bar", mininterval=5)

minio_client = MinIOClient(bucket=MINIO_BUCKET_INFRA)
pgclient = PostgresClient(conn_name="POSTGRES_METRIC")
config = MetricsConfig()
TMP_FOLDER = f"{AIRFLOW_DAG_TMP}{config.tmp_folder}"
FOUND_FOLDER = f"{TMP_FOLDER}found/"
OUTPUT_FOLDER = f"{TMP_FOLDER}outputs/"


def create_metrics_tables() -> None:
    pgclient.execute_sql_file(
        file=File(
            source_path=f"{config.code_folder_full_path}/sql/",
            source_name="create_tables.sql",
            column_order=None,
        ),
    )


def get_new_logs(ti) -> bool:
    new_logs_path = minio_client.get_files_from_prefix(
        prefix="metrics-logs/new/",
        ignore_airflow_env=True,
    )
    if new_logs_path:
        ongoing_logs_path = minio_client.copy_many_objects(
            new_logs_path,
            "metrics-logs/ongoing/",
            remove_source_file=True,
        )
        ti.xcom_push(key="ongoing_logs_path", value=ongoing_logs_path)
        return True
    else:
        return False


def download_catalog() -> None:
    download_files(
        [
            File(
                url=obj_config.catalog_download_url,
                dest_path=TMP_FOLDER,
                dest_name=obj_config.catalog_destination_name,
            )
            for obj_config in config.logs_config
        ]
    )


def download_log(ti):
    ongoing_logs_path = ti.xcom_pull(key="ongoing_logs_path", task_ids="get_new_logs")

    logging.info("Downloading raw logs...")
    for path in ongoing_logs_path:
        minio_client.download_files(
            list_files=[
                File(
                    source_path="metrics-logs/ongoing/",
                    source_name=path.split("/")[-1],
                    dest_path=TMP_FOLDER,
                    dest_name=path.split("/")[-1],
                    remote_source=True,
                )
            ],
            ignore_airflow_env=True,
        )

    dates_to_process = set(d.split("/")[-1].split("-")[2] for d in ongoing_logs_path)
    ti.xcom_push(key="dates_to_process", value=dates_to_process)


def process_log(ti) -> None:
    """Aggregates log files by group of available dates and log types."""

    dates_to_process = ti.xcom_pull(key="dates_to_process", task_ids="download_log")

    remove_files_from_directory(OUTPUT_FOLDER)

    # analyser toutes les dates différentes
    for log_date in dates_to_process:
        remove_files_from_directory(FOUND_FOLDER)
        isoformat_log_date = datetime.strptime(log_date, "%d%m%Y").date().isoformat()
        logging.info(f"Processing {isoformat_log_date} date...")
        for file_name in glob.glob(f"{TMP_FOLDER}*{log_date}*.tar.gz"):
            with tarfile.open(file_name, "r:gz") as tar:
                for log_file in tar:
                    logging.info(f"> Parsing {file_name} content: {log_file.name}...")
                    log_data = tar.extractfile(log_file)
                    if not log_data:
                        logging.info("Empty file!")
                        break
                    batch_size = 300_000_000  # One log line is around 290 bytes
                    n_logs_found_total = 0
                    while True:
                        lines = log_data.readlines(batch_size)
                        if not lines:
                            break
                        n_logs_processed = len(lines)
                        n_logs_found = parse_logs(
                            lines, isoformat_log_date, config.logs_config, FOUND_FOLDER
                        )
                        n_logs_found_total += n_logs_found
                        logging.info(
                            f">> {n_logs_processed} log processed. {n_logs_found} ({n_logs_found / n_logs_processed * 100:.1f}%) relevant logs found."
                        )
                    logging.info(
                        f">> Total of {n_logs_found_total} relevant log found."
                    )


def aggregate_log(ti) -> None:
    dates_to_process = ti.xcom_pull(key="dates_to_process", task_ids="download_log")
    dates_processed: list[str] = []

    for log_date in dates_to_process:
        for obj_config in config.logs_config:
            logging.info(f"Aggregating {obj_config.type} objects...")
            df_catalog = pd.read_csv(
                f"{TMP_FOLDER}{obj_config.catalog_destination_name}",
                dtype="string",
                sep=";",
                usecols=list(obj_config.catalog_columns.keys()),
            )
            df = pd.read_csv(
                f"{FOUND_FOLDER}found_{obj_config.type}.csv",
                dtype="string",
                sep=";",
            )

            if obj_config.type in ["resources"]:
                catalog_dict: dict[str, str] = defaultdict()
                static_uri = "https://static.data.gouv.fr/resources/"

                # Resource catalog has no slug column but static
                # URLs starting with https://static.data.gouv.fr/resources/$SLUG
                # Using a resource ID will trigger a redirect to its static URL so we want:
                # 1. All the slugs from the static URLs
                df_slugs = (
                    df_catalog.loc[lambda df: df["url"].str.contains(static_uri)]
                    .assign(slug=lambda df: df["url"].str.replace(static_uri, ""))
                    .filter(items=["slug", "id"])
                )
                catalog_dict.update(df_slugs.set_index("slug")["id"].to_dict())

                # 2. All the IDs that don't have any static URL
                #  Note: a few resource_id are common to multiple datasets
                #  They need to be deduplicated with a priority to
                #  "dataset.archived" as False otherwise keep the last one created
                df_ids = (
                    df_catalog.loc[lambda df: ~df["url"].str.contains(static_uri)]
                    .sort_values(
                        by=["dataset.archived", "created_at"], ascending=[True, False]
                    )
                    .drop_duplicates(subset=["id"], keep="first")
                    .filter(items=["id"])
                )
                catalog_dict.update({id: id for id in df_ids["id"].to_list()})
            else:
                # Get all slugs and IDs
                catalog_dict = get_catalog_id_mapping(df_catalog, "slug")

            # Replace slugs by their ID and make sure all IDs do exist in the catalog
            df["id"] = df["id"].apply(
                lambda x: catalog_dict[x] if x in catalog_dict else None
            )

            df = df.groupby(["date_metric", "id"], as_index=False).aggregate(
                nb_visit_static=(
                    "segment",
                    lambda x: x.isin(
                        [
                            segment.replace("/", "")
                            for segment in config.all_static_segments
                        ]
                    ).sum(),
                ),
                nb_visit=(
                    "segment",
                    lambda x: x.isin(
                        [
                            segment.replace("/", "")
                            for segment in config.web_segments
                            + config.all_static_segments
                        ]
                    ).sum(),
                ),
                nb_visit_apis=(
                    "segment",
                    lambda x: x.isin(
                        [segment.replace("/", "") for segment in config.api_segments]
                    ).sum(),
                ),
                nb_visit_total=("segment", "count"),
                **{
                    f"nb_visit_{segment.replace('/', '')}": (
                        "segment",
                        lambda x, s=segment.replace("/", ""): (x == s).sum(),
                    )
                    for segment in config.all_segments
                },  # type: ignore
            )
            df.sort_values(by="nb_visit", ascending=False)
            df = pd.merge(
                df,
                df_catalog,
                on="id",
                how="left",
            )
            df = df.rename(columns=obj_config.catalog_columns)
            df[obj_config.output_columns].to_csv(
                path_or_buf=f"{OUTPUT_FOLDER}{obj_config.type}-{log_date}.csv",
                index=False,
                header=False,
            )
            logging.info(
                f"> Output saved in {obj_config.type}-{log_date}.csv ({df.shape[0]} rows)."
                f" With columns: {obj_config.output_columns}"
            )
            processed_dates = list(df["date_metric"].unique())
            dates_processed = get_unique_list(dates_processed, processed_dates)

    logging.info(f"Processed dates: {dates_processed}")
    ti.xcom_push(key="dates_processed", value=dates_processed)


def visit_postgres_duplication_safety(ti) -> None:
    """
    In case we have to process some logs again, this task is making
    sure we don't end up duplicating the metrics on postgres.
    """
    processed_dates = ti.xcom_pull(key="dates_processed", task_ids="aggregate_log")
    for log_date in processed_dates:
        logging.info(
            f"Deleting existing visit metrics from the {log_date} if they exists."
        )
        pgclient.execute_sql_file(
            File(
                source_name="remove_visit_metrics.sql",
                source_path=f"{config.code_folder_full_path}/sql/",
                column_order=None,
            ),
            replacements={"%%date%%": log_date},
        )


def save_metrics_to_postgres() -> None:
    for obj_config in config.logs_config:
        for lf in glob.glob(f"{OUTPUT_FOLDER}{obj_config.type}-*"):
            if "-id-" not in lf and "-static-" not in lf:
                pgclient.copy_file(
                    file=File(
                        source_path="/".join(lf.split("/")[:-1]) + "/",
                        source_name=lf.split("/")[-1],
                        column_order="(" + ", ".join(obj_config.output_columns) + ")",
                    ),
                    table=f"{config.database_schema}.visits_{obj_config.type}",
                    has_header=False,
                )


def copy_logs_to_processed_folder(ti) -> None:
    ongoing_logs_path = ti.xcom_pull(key="ongoing_logs_path", task_ids="get_new_logs")
    minio_client.copy_many_objects(
        ongoing_logs_path, "metrics-logs/processed/", remove_source_file=True
    )


def refresh_materialized_views() -> None:
    pgclient.execute_sql_file(
        file=File(
            source_path=f"{config.code_folder_full_path}/sql/",
            source_name="refresh_materialized_views.sql",
            column_order=None,
        ),
    )


def process_matomo(ti) -> None:
    """
    Fetch matomo metrics for external links for reuses and sum these by orga
    """
    if not os.path.exists(f"{TMP_FOLDER}matomo-outputs/"):
        os.makedirs(f"{TMP_FOLDER}matomo-outputs/")

    # Which timespan to target?
    yesterday = (date.today() - timedelta(days=1)).isoformat()
    logging.info("get matamo outlinks for reuses")
    reuses_obj = next((obj for obj in config.logs_config if obj.type == "reuses"))
    df_catalog = pd.read_csv(
        f"{TMP_FOLDER}{reuses_obj.catalog_destination_name}",
        dtype="string",
        sep=";",
        usecols=["id", "slug", "remote_url", "organization_id"],
    )
    df_catalog["outlinks"] = df_catalog.progress_apply(
        lambda x: get_matomo_outlinks(reuses_obj.type, x.slug, x.remote_url, yesterday),
        axis=1,
    )  # type: ignore
    df_catalog["date_metric"] = yesterday
    df_catalog.to_csv(
        f"{TMP_FOLDER}matomo-outputs/{reuses_obj.type}-outlinks.csv",
        columns=["date_metric", "id", "organization_id", "outlinks"],
        index=False,
        header=False,
    )
    logging.info("Matomo reuses outlinks processed!")

    organizations_obj = next(
        (obj for obj in config.logs_config if obj.type == "organizations")
    )
    df_orga = pd.read_csv(
        f"{TMP_FOLDER}{organizations_obj.catalog_destination_name}",
        dtype="string",
        sep=";",
        usecols=list(organizations_obj.catalog_columns.keys()),
    )
    df_orga = df_orga.rename(columns=organizations_obj.catalog_columns)

    df_orga = sum_outlinks_by_orga(df_orga, df_catalog, reuses_obj.type)

    df_orga["date_metric"] = yesterday
    df_orga.to_csv(
        f"{TMP_FOLDER}matomo-outputs/organizations-outlinks.csv",
        columns=["date_metric", "organization_id", "reuses_outlinks"],
        index=False,
        header=False,
    )
    ti.xcom_push(key="dates_processed", value=[yesterday])
    logging.info("Matomo organisations outlinks processed!")


def matomo_postgres_duplication_safety(ti) -> None:
    """
    In case we have to process some logs again, this task is making
    sure we don't end up duplicating the matomo metrics on postgres.
    """
    processed_dates = ti.xcom_pull(key="dates_processed", task_ids="process_matomo")
    for log_date in processed_dates:
        logging.info(
            f"Deleting existing matomo metrics from the {log_date} if they exists."
        )
        pgclient.execute_sql_file(
            File(
                source_name="remove_matomo_metrics.sql",
                source_path=f"{config.code_folder_full_path}/sql/",
                column_order=None,
            ),
            replacements={"%%date%%": log_date},
        )


def save_matomo_to_postgres() -> None:
    pg_config = [
        {
            "name": "reuses",
            "columns": "(date_metric, reuse_id, organization_id, nb_outlink)",
        },
        {
            "name": "organizations",
            "columns": "(date_metric, organization_id, nb_outlink)",
        },
    ]
    for obj in pg_config:
        for lf in glob.glob(f"{TMP_FOLDER}matomo-outputs/{obj['name']}-*"):
            pgclient.copy_file(
                file=File(
                    source_path="/".join(lf.split("/")[:-1]) + "/",
                    source_name=lf.split("/")[-1],
                    column_order=obj["columns"],
                ),
                table=f"{config.database_schema}.matomo_{obj['name']}",
                has_header=False,
            )
