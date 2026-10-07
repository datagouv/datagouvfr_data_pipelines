import json
import logging
import os
from datetime import datetime

import duckdb
import pandas as pd
import requests
import yaml
from airflow.sdk.exceptions import AirflowSkipException
from airflow.sdk import task
from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_HOME,
    AIRFLOW_DAG_TMP,
    AIRFLOW_ENV,
    S3_BUCKET_DATA_PIPELINE_OPEN,
)
from datagouvfr_data_pipelines.data_processing.elections.aggregation import (
    checks,
    description,
    table_passage,
)
from datagouvfr_data_pipelines.data_processing.elections.aggregation.tests import (
    check_values,
)
from datagouvfr_data_pipelines.data_processing.elections.aggregation.schema import (
    SCHEMAS_FOLDER,
    SCOPES,
    TABLES,
    dtypes,
    load_schema,
    schema_path,
)
from datagouvfr_data_pipelines.utils.conversions import csv_to_parquet
from datagouvfr_data_pipelines.utils.datagouv import local_client
from datagouvfr_data_pipelines.utils.filesystem import File
from datagouvfr_data_pipelines.utils.s3 import S3Client
from datagouvfr_data_pipelines.utils.tchap import send_message

DAG_FOLDER = "datagouvfr_data_pipelines/data_processing/"
TMP_FOLDER = f"{AIRFLOW_DAG_TMP}elections/"
# prod outputs are at the root of the bucket (published URLs), other envs are prefixed
OUTPUT_FOLDER = "elections/" if AIRFLOW_ENV == "prod" else f"{AIRFLOW_ENV}/elections/"
SOURCE_DATASETS_API_URL = "https://www.data.gouv.fr/api/1/datasets/"
CORRESPONDENCE_FILE = "table_passage_communes.csv"
NUANCES_FILE = "nuances_politiques.csv"
# table -> csv file produced in TMP_FOLDER (the results also have a parquet)
OUTPUT_FILES = {
    "general": "general_results.csv",
    "candidats": "candidats_results.csv",
    "nuances": NUANCES_FILE,
    "communes": CORRESPONDENCE_FILE,
}
# the steps that a manual run can select with the "steps" param, all by default
STEPS = [
    "check_sources_updates",
    "process_election_data",
    "process_nuances",
    "process_communes",
    "check_outputs",
    "send_results_to_s3",
    "publish_results_elections",
    "publish_description",
    "notification",
]


def load_sources() -> dict:
    with open(
        f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}elections/aggregation/sources.json"
    ) as fp:
        return json.load(fp)


def load_config() -> dict:
    with open(f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}elections/aggregation/config.json") as fp:
        return json.load(fp)


def skip_unless_selected(context, step: str) -> None:
    if step not in (context["params"].get("steps") or STEPS):
        raise AirflowSkipException(f"{step} not selected in the steps param")


def produced(name: str) -> bool:
    # whether the file was produced in this run (steps may have been skipped)
    return os.path.exists(TMP_FOLDER + name)


def published_files() -> list[tuple[str, str, str]]:
    """(table, format, file name) of each published data file."""
    return [
        (table, ext, name.replace(".csv", f".{ext}"))
        for table, name in OUTPUT_FILES.items()
        for ext in (["csv", "parquet"] if table in SCOPES else ["csv"])
    ]


@task(trigger_rule="none_failed")
def check_sources_updates(**context):
    skip_unless_selected(context, "check_sources_updates")
    # the source datasets are always on prod, whatever the environment
    alerts = []
    for key, source in sorted(load_sources().items()):
        for part in ("resultats", "nuances"):
            dataset_id = source.get(part, {}).get("source_dataset_id")
            if not dataset_id:
                # e.g. the nuance grids taken from the circulars, with no source dataset
                continue
            label = key if part == "resultats" else f"{key} (nuances)"
            link = f"https://www.data.gouv.fr/datasets/{dataset_id}"
            response = requests.get(
                f"{SOURCE_DATASETS_API_URL}{dataset_id}/", timeout=60
            )
            if not response.ok:
                alerts.append(
                    f"- [{label}]({link}) : jeu inaccessible (HTTP {response.status_code})"
                )
                continue
            dataset = response.json()
            if dataset.get("archived"):
                alerts.append(f"- [{label}]({link}) : jeu archivé")
            last_update = datetime.fromisoformat(dataset["last_update"])
            recorded = source[part]["source_last_update"]
            if last_update > datetime.fromisoformat(recorded):
                alerts.append(
                    f"- [{label}]({link}) : modifié le {last_update:%Y-%m-%d}"
                    f" (config : {recorded[:10]})"
                )
    if not alerts:
        logging.info("All source datasets are up to date")
        return
    logging.warning("\n".join(alerts))
    # we only warn, our copies on S3 remain the reference for the aggregation
    send_message(
        text=(
            "Élections : jeux sources du Ministère de l'Intérieur à vérifier\n\n"
            + "\n".join(alerts)
            + "\n\nSi une correction est reprise, mettre à jour `source_last_update`"
            " dans `sources.json`."
        )
    )


@task(trigger_rule="none_failed")
def process_election_data(**context):
    skip_unless_selected(context, "process_election_data")
    # the standardized files are built by the standalone scripts (see scripts/),
    # stored on our S3 and listed in sources.json, here we only concatenate them
    sources = load_sources()
    s3_client = S3Client(bucket=S3_BUCKET_DATA_PIPELINE_OPEN, conn_name="S3_OVH_SBG")
    for scope in SCOPES:
        logging.info(f"Processing {scope} resources")
        # the entries without "resultats" are elections listed in the description only
        integrated = sorted(key for key in sources if "resultats" in sources[key])
        for idx, id_election in enumerate(integrated):
            key = sources[id_election]["resultats"]["files"][scope]
            file = File(
                source_path=os.path.dirname(key),
                source_name=os.path.basename(key),
                dest_path=TMP_FOLDER,
                dest_name=f"{id_election}_{scope}.csv",
                remote_source=True,
            )
            s3_client.download_files([file], ignore_airflow_env=True)
            df = pd.read_csv(file.full_dest_path, sep=";", dtype=str)
            os.remove(file.full_dest_path)
            assert all(col in dtypes[scope].keys() for col in df.columns)
            # add missing columns and reorder for concatenation
            for col in dtypes[scope].keys():
                if col not in df.columns:
                    df[col] = ""
            df = df[dtypes[scope].keys()]
            # concatenating all files (first one has header)
            df.to_csv(
                TMP_FOLDER + f"{scope}_results.csv",
                sep=";",
                index=False,
                mode="w" if idx == 0 else "a",
                header=idx == 0,
            )
            del df
        # hydra is not (yet) able to ingest the big csv, maybe soon? :eyes:
        logging.info("Export en parquet...")
        csv_to_parquet(
            csv_file_path=TMP_FOLDER + f"{scope}_results.csv",
            dtype=dtypes[scope],
            # pandas quotes and escapes with ", don't let duckdb sniff it on the first rows
            quotechar='"',
            escapechar='"',
        )


@task(trigger_rule="none_failed")
def process_nuances(**context):
    skip_unless_selected(context, "process_nuances")
    # the nuance grids of the elections that have one ("nuances" part of sources.json)
    sources = load_sources()
    s3_client = S3Client(bucket=S3_BUCKET_DATA_PIPELINE_OPEN, conn_name="S3_OVH_SBG")
    frames = []
    for key in sorted(sources):
        if "nuances" not in sources[key]:
            continue
        path = sources[key]["nuances"]["files"]["nuances"]
        file = File(
            source_path=os.path.dirname(path),
            source_name=os.path.basename(path),
            dest_path=TMP_FOLDER,
            dest_name=f"{key}_nuances.csv",
            remote_source=True,
        )
        s3_client.download_files([file], ignore_airflow_env=True)
        df = pd.read_csv(file.full_dest_path, sep=";", dtype=str, keep_default_na=False)
        os.remove(file.full_dest_path)
        assert list(df.columns) == list(dtypes["nuances"]), f"{key}: {list(df.columns)}"
        frames.append(df)
    pd.concat(frames, ignore_index=True).to_csv(
        TMP_FOLDER + NUANCES_FILE, sep=";", index=False
    )


@task(trigger_rule="none_failed")
def process_communes(**context):
    skip_unless_selected(context, "process_communes")
    if not produced(OUTPUT_FILES["general"]):
        # process_election_data was skipped: use the last published general results
        logging.info(f"Getting {OUTPUT_FILES['general']} from {OUTPUT_FOLDER}")
        S3Client(
            bucket=S3_BUCKET_DATA_PIPELINE_OPEN, conn_name="S3_OVH_SBG"
        ).download_files(
            [
                File(
                    source_path=OUTPUT_FOLDER,
                    source_name=OUTPUT_FILES["general"],
                    dest_path=TMP_FOLDER,
                    dest_name=f"published_{OUTPUT_FILES['general']}",
                    remote_source=True,
                )
            ],
            ignore_airflow_env=True,
        )
        general = f"{TMP_FOLDER}published_{OUTPUT_FILES['general']}"
    else:
        general = TMP_FOLDER + OUTPUT_FILES["general"]
    # the communes of each election, as published in the aggregated general results
    communes = duckdb.sql(
        f"""
        select id_election, code_departement, code_commune,
            any_value(libelle_commune) as libelle_commune
        from read_csv('{general}', delim=';', all_varchar=true,
            quote='"', escape='"')
        where coalesce(code_commune, '') <> ''
        group by all
        """
    ).df()
    url, latest_year = table_passage.get_latest_annual_table()
    logging.info(f"INSEE correspondence table: {url}")
    insee, published = table_passage.load_annual_table(url)
    # useful to keep the resource description (managed in the UI) up to date
    logging.info(f"INSEE table {latest_year}, published on {published}")
    result = table_passage.build_table_passage(communes, insee, latest_year)
    result.to_csv(TMP_FOLDER + CORRESPONDENCE_FILE, sep=";", index=False)
    logging.info(
        f"{len(result)} rows, methods: "
        f"{result['methode_rapprochement'].value_counts(dropna=False).to_dict()}"
    )
    alerts = table_passage.check_millesimes(
        communes, insee, latest_year
    ) + table_passage.check_unmapped(result)
    if alerts:
        logging.warning("\n".join(alerts))
        # we only warn, the table is published anyway
        send_message(
            text="Élections : table de passage vers la géographie communale à vérifier\n\n"
            + "\n".join(f"- {alert}" for alert in alerts)
        )


@task(trigger_rule="none_failed")
def check_outputs(**context):
    skip_unless_selected(context, "check_outputs")
    errors = []
    for table, name in OUTPUT_FILES.items():
        if not produced(name):
            logging.info(f"{name} not produced in this run, not checked")
            continue
        errors += [
            f"{name}: {error}"
            for error in checks.check_file(TMP_FOLDER + name, load_schema(table))
        ]
        logging.info(f"{name} checked against {schema_path(table).name}")
        cases = check_values.load_cases(table) if table in check_values.TABLES else []
        errors += [
            f"{name}: {error}"
            for error in check_values.check_values(TMP_FOLDER + name, cases)
        ]
        logging.info(f"{name}: {len(cases)} expected values checked")
    if errors:
        # nothing is sent nor published with files that don't match their schema
        # or the values read by hand in the sources
        raise ValueError(
            "Files not matching their schema or the expected values:\n"
            + "\n".join(errors)
        )


@task(trigger_rule="none_failed")
def send_results_to_s3(**context):
    skip_unless_selected(context, "send_results_to_s3")
    files = [
        File(
            source_path=TMP_FOLDER,
            source_name=name,
            dest_path=OUTPUT_FOLDER,
            dest_name=name,
            content_type=(
                "application/vnd.apache.parquet" if ext == "parquet" else "text/csv"
            ),
        )
        for _, ext, name in published_files()
        if produced(name)
    ]
    # the schema of each table produced in this run
    files += [
        File(
            source_path=str(SCHEMAS_FOLDER),
            source_name=schema_path(table).name,
            dest_path=f"{OUTPUT_FOLDER}schemas/",
            dest_name=schema_path(table).name,
            content_type="application/json",
        )
        for table in TABLES
        if produced(OUTPUT_FILES[table])
    ]
    S3Client(bucket=S3_BUCKET_DATA_PIPELINE_OPEN, conn_name="S3_OVH_SBG").send_files(
        list_files=files,
        # the environment prefix is already handled by OUTPUT_FOLDER
        ignore_airflow_env=True,
        is_public=True,
    )


@task(trigger_rule="none_failed")
def publish_results_elections(**context):
    skip_unless_selected(context, "publish_results_elections")
    s3_client = S3Client(bucket=S3_BUCKET_DATA_PIPELINE_OPEN, conn_name="S3_OVH_SBG")
    config = load_config()
    titles = {"general": "Résultats généraux", "candidats": "Résultats par candidat"}
    descriptions = {
        "general": "Résultats généraux des élections agrégés au niveau des bureaux de votes",
        "candidats": "Résultats des élections par candidat agrégés au niveau des bureaux de votes",
    }
    for table, ext, name in published_files():
        if not produced(name):
            continue
        payload = {
            "url": s3_client.get_file_url(f"{OUTPUT_FOLDER}{name}"),
            "filesize": os.path.getsize(TMP_FOLDER + name),
            "format": ext,
        }
        if table in SCOPES:
            payload |= {
                "title": titles[table],
                "description": (
                    f"{descriptions[table]},"
                    " créés à partir des données du Ministère de l'Intérieur"
                    f", au format {ext}"
                    f" (dernière modification : {datetime.today().strftime('%Y-%m-%d')})"
                ),
            }
        # for the other tables, title and description are managed in the UI
        local_client.resource(
            id=config[table][ext][AIRFLOW_ENV]["resource_id"],
            dataset_id=config["dataset_id"][AIRFLOW_ENV],
            fetch=False,
        ).update(payload=payload)
        logging.info(f"Done with {name}")
    for table in TABLES:
        if not produced(OUTPUT_FILES[table]):
            continue
        # title and description of the schema resources are managed in the UI
        local_client.resource(
            id=config[table]["schema"][AIRFLOW_ENV]["resource_id"],
            dataset_id=config["dataset_id"][AIRFLOW_ENV],
            fetch=False,
        ).update(
            payload={
                "url": s3_client.get_file_url(
                    f"{OUTPUT_FOLDER}schemas/{schema_path(table).name}"
                ),
                "filesize": os.path.getsize(schema_path(table)),
                "format": "json",
            },
        )
        logging.info(f"Done with {schema_path(table).name}")


@task(trigger_rule="none_failed")
def publish_description(**context):
    skip_unless_selected(context, "publish_description")
    # the manual text of description.yaml, then the sources listed in sources.json
    with open(
        f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}elections/aggregation/description.yaml"
    ) as fp:
        texts = yaml.safe_load(fp)
    new = description.build_description(texts, load_sources())
    dataset = local_client.dataset(load_config()["dataset_id"][AIRFLOW_ENV])
    if dataset.description == new:
        logging.info("Dataset description already up to date")
        return
    logging.info(f"New dataset description:\n{new}")
    dataset.update(payload={"description": new})


@task(trigger_rule="none_failed")
def notification(**context):
    skip_unless_selected(context, "notification")
    config = load_config()
    send_message(
        text=(
            "📣 Données élections mises à jour.\n\n"
            f"- Données stockées sur S3 - Bucket {S3_BUCKET_DATA_PIPELINE_OPEN}\n"
            f"- Données référencées [sur data.gouv.fr]({local_client.base_url}/datasets/"
            f"{config['dataset_id'][AIRFLOW_ENV]})"
        )
    )
