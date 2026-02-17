from datetime import date, datetime, timedelta
import glob
import logging
import os
import tarfile

from airflow.decorators import task
from airflow.utils.trigger_rule import TriggerRule
import pandas as pd
from tqdm import tqdm

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
    AIRFLOW_ENV,
    # S3_BUCKET_INFRA,
    # S3_URL,
    # SECRET_S3_METRICS_PASSWORD,
    # SECRET_S3_METRICS_USER,
)
from datagouvfr_data_pipelines.dgv.metrics.task_functions import (
    aggregate_metrics,
    parse_logs,
    sum_outlinks_by_orga,
    get_matomo_outlinks,
)
from datagouvfr_data_pipelines.utils.download import download_files
from datagouvfr_data_pipelines.utils.filesystem import remove_files_from_directory, File
from datagouvfr_data_pipelines.utils.s3 import S3Client
from datagouvfr_data_pipelines.utils.postgres import PostgresClient
from datagouvfr_data_pipelines.dgv.metrics.config import MetricsConfig
from datagouvfr_data_pipelines.utils.utils import get_unique_list


tqdm.pandas(desc="pandas progress bar", mininterval=5)

s3_client = S3Client(
    bucket="infra",
    # bucket=S3_BUCKET_INFRA,
    # user=SECRET_S3_METRICS_USER,
    # pwd=SECRET_S3_METRICS_PASSWORD,
    # s3_url=S3_URL,
)
pgclient = PostgresClient(conn_name="POSTGRES_METRIC")
config = MetricsConfig()
TMP_FOLDER = f"{AIRFLOW_DAG_TMP}{config.tmp_folder}"
FOUND_FOLDER = f"{TMP_FOLDER}found/"
OUTPUT_FOLDER = f"{TMP_FOLDER}outputs/"


@task()
def create_metrics_tables() -> None:
    pgclient.execute_sql_file(
        file=File(
            source_path=f"{config.code_folder_full_path}/sql/",
            source_name="create_tables.sql",
            column_order=None,
        ),
    )


def get_new_logs(**context) -> bool:
    new_logs_path = list(s3_client.get_files_from_prefix(prefix="metrics-logs/new/"))
    if new_logs_path:
        ongoing_logs_path = s3_client.copy_many_objects(
            new_logs_path,
            f"{AIRFLOW_ENV}/metrics-logs/ongoing/",
            remove_source_file=True,
            client_side=True,
        )
        context["ti"].xcom_push(key="ongoing_logs_path", value=ongoing_logs_path)
        return True
    else:
        return False


@task()
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


@task()
def download_log(**context):
    ongoing_logs_path = context["ti"].xcom_pull(
        key="ongoing_logs_path", task_ids="get_new_logs"
    )

    logging.info("Downloading raw logs...")
    for path in ongoing_logs_path:
        s3_client.download_files(
            list_files=[
                File(
                    source_path="metrics-logs/ongoing/",
                    source_name=path.split("/")[-1],
                    dest_path=TMP_FOLDER,
                    dest_name=path.split("/")[-1],
                    remote_source=True,
                )
            ]
        )

    dates_to_process = set(d.split("/")[-1].split("-")[2] for d in ongoing_logs_path)
    context["ti"].xcom_push(key="dates_to_process", value=dates_to_process)


@task()
def process_log(**context) -> None:
    """Aggregates log files by group of available dates and log types."""

    dates_to_process = context["ti"].xcom_pull(
        key="dates_to_process", task_ids="download_log"
    )

    remove_files_from_directory(FOUND_FOLDER)

    # analyser toutes les dates différentes
    for log_date in dates_to_process:
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


@task()
def aggregate_log(**context) -> None:
    dates_to_process = context["ti"].xcom_pull(
        key="dates_to_process", task_ids="download_log"
    )
    dates_processed: list[str] = []

    remove_files_from_directory(OUTPUT_FOLDER)

    for obj_config in config.logs_config:
        df_catalog = pd.read_csv(
            f"{TMP_FOLDER}{obj_config.catalog_destination_name}",
            dtype="string",
            sep=";",
            usecols=list(obj_config.catalog_columns.keys()),
        )
        for log_date in dates_to_process:
            isoformat_log_date = (
                datetime.strptime(log_date, "%d%m%Y").date().isoformat()
            )
            logging.info(f"Aggregating {obj_config.type} objects...")
            df = pd.read_csv(
                f"{FOUND_FOLDER}{isoformat_log_date}_{obj_config.type}_found.csv",
                dtype="string",
                sep=";",
            )

            processed_dates, n_rows = aggregate_metrics(
                df,
                df_catalog,
                obj_config,
                config,
                f"{OUTPUT_FOLDER}{isoformat_log_date}_{obj_config.type}.csv",
            )
            dates_processed = get_unique_list(dates_processed, processed_dates)
            logging.info(
                f"> Output saved in {isoformat_log_date}_{obj_config.type}.csv ({n_rows} rows)."
                f" With columns: {obj_config.output_columns}"
            )

    logging.info(f"Processed dates: {dates_processed}")
    context["ti"].xcom_push(key="dates_processed", value=dates_processed)


@task()
def visit_postgres_duplication_safety(**context) -> None:
    """
    In case we have to process some logs again, this task is making
    sure we don't end up duplicating the metrics on postgres.
    """
    processed_dates = context["ti"].xcom_pull(
        key="dates_processed", task_ids="aggregate_log"
    )
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


@task()
def save_metrics_to_postgres() -> None:
    for obj_config in config.logs_config:
        # Looking for files such as 2025-09-24_resources.csv
        for lf in glob.glob(f"{OUTPUT_FOLDER}????-??-??_{obj_config.type}.csv"):
            if "-id-" not in lf and "-static-" not in lf:
                pgclient.copy_file(
                    file=File(
                        source_path="/".join(lf.split("/")[:-1]) + "/",
                        source_name=lf.split("/")[-1],
                        column_order="(" + ", ".join(obj_config.output_columns) + ")",
                    ),
                    table=f"{config.database_schema}.visits_{obj_config.type}",
                    has_header=True,
                )


@task()
def copy_logs_to_processed_folder(**context) -> None:
    ongoing_logs_path = context["ti"].xcom_pull(
        key="ongoing_logs_path", task_ids="get_new_logs"
    )
    s3_client.copy_many_objects(
        ongoing_logs_path,
        f"{AIRFLOW_ENV}/metrics-logs/processed/",
        remove_source_file=True,
        client_side=True,
    )


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def refresh_materialized_views() -> None:
    pgclient.execute_sql_file(
        file=File(
            source_path=f"{config.code_folder_full_path}/sql/",
            source_name="refresh_materialized_views.sql",
            column_order=None,
        ),
    )


@task()
def process_matomo(**context) -> None:
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
    context["ti"].xcom_push(key="dates_processed", value=[yesterday])
    logging.info("Matomo organisations outlinks processed!")


@task()
def matomo_postgres_duplication_safety(**context) -> None:
    """
    In case we have to process some logs again, this task is making
    sure we don't end up duplicating the matomo metrics on postgres.
    """
    processed_dates = context["ti"].xcom_pull(
        key="dates_processed", task_ids="process_matomo"
    )
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


@task()
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
