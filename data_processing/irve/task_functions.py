import logging
import os
import re
from ast import literal_eval
from datetime import datetime
from pathlib import Path
from typing import Union

import pandas as pd
from airflow.models import TaskInstance

from datagouvfr_data_pipelines.config import AIRFLOW_ENV
from datagouvfr_data_pipelines.data_processing.irve.geo_utils.geo import (
    improve_geo_data_quality,
)
from datagouvfr_data_pipelines.utils.schema import (
    append_stats_list,
    build_reference_table,
    comparer_versions,
    consolidate_data,
    create_detailed_report,
    download_schema_files,
    final_directory_clean_up,
    get_schema_report,
    load_config,
    notification_synthese,
    remove_old_schemas,
    update_consolidation_documentation_report,
    update_reference_table,
    update_resource_send_mail_producer,
    upload_consolidated,
    upload_minio,
)

schema_name = "etalab/schema-irve-statique"


def get_all_irve_resources(
    ti: TaskInstance,
    tmp_path: Path,
    schemas_catalogue_url: str,
    config_path: Path,
) -> None:
    consolidation_date_str = datetime.today().strftime("%Y%m%d")
    logging.info(consolidation_date_str)

    data_path = tmp_path / "data"
    data_path.mkdir(parents=True, exist_ok=True)

    consolidated_data_path = tmp_path / "consolidated_data"
    consolidated_data_path.mkdir(parents=True, exist_ok=True)

    ref_tables_path = tmp_path / "ref_tables"
    ref_tables_path.mkdir(parents=True, exist_ok=True)

    report_tables_path = tmp_path / "report_tables"
    report_tables_path.mkdir(parents=True, exist_ok=True)

    output_path = tmp_path / "output"
    output_path.mkdir(parents=True, exist_ok=True)

    validata_reports_path = tmp_path / "validata_reports"
    validata_reports_path.mkdir(parents=True, exist_ok=True)

    schemas_report_dict, schemas_catalogue_list = get_schema_report(
        schemas_catalogue_url=schemas_catalogue_url,
        config_path=config_path,
        schema_name=schema_name,
    )

    config_dict = load_config(config_path)
    config_dict = remove_old_schemas(config_dict, schemas_catalogue_list, single_schema=True)

    if AIRFLOW_ENV == "dev":  # to consolidate on demo
        schemas_catalogue_list = [
            {
                "name": "etalab/schema-irve-statique",
                "title": "IRVE statique",
                "description": "Spécification du fichier d'échange relatif aux données concernant la localisation géographique et les caractéristiques techniques des stations et des points de recharge pour véhicules électriques",
                "schema_url": "https://schema.data.gouv.fr/schemas/etalab/schema-irve-statique/latest/schema-statique.json",
                "schema_type": "tableschema",
                "contact": "contact@transport.beta.gouv.fr",
                "examples": [
                    {
                        "title": "Exemple de fichier IRVE valide",
                        "path": "https://raw.githubusercontent.com/etalab/schema-irve/v2.3.0/statique/exemple-valide-statique.csv",
                    }
                ],
                "labels": [
                    "Socle Commun des Données Locales",
                    "transport.data.gouv.fr",
                ],
                "consolidation_dataset_id": "64b521568ecbee60f15aa241",
                "versions": [
                    {
                        "version_name": "2.3.0",
                        "schema_url": "https://schema.data.gouv.fr/schemas/etalab/schema-irve-statique/2.3.0/schema-statique.json",
                    },
                    {
                        "version_name": "2.3.1",
                        "schema_url": "https://schema.data.gouv.fr/schemas/etalab/schema-irve-statique/2.3.1/schema-statique.json",
                    },
                ],
                "external_doc": "https://doc.transport.data.gouv.fr/producteurs/infrastructures-de-recharge-de-vehicules-electriques-irve",
                "external_tool": None,
                "homepage": "https://github.com/etalab/schema-irve.git",
                "datapackage_title": "Infrastructures de recharges pour véhicules électriques",
                "datapackage_name": "etalab/schema-irve",
                "datapackage_description": "data package contenant 2 schémas : IRVE statique et IRVE dynamique",
            }
        ]
        config_dict = {
            "etalab/schema-irve-statique": {
                "consolidate": True,
                "consolidated_dataset_id": "64b521568ecbee60f15aa241",
                "documentation_resource_id": "66f90dcf-caa3-43ad-9aeb-0f504f503104",
                "drop_versions": [
                    "1.0.0",
                    "1.0.1",
                    "1.0.2",
                    "1.0.3",
                    "2.0.0",
                    "2.0.1",
                    "2.0.2",
                    "2.0.3",
                    "2.1.0",
                    "2.2.0",
                    "2.2.1",
                    "2.3.0",
                ],
                "exclude_dataset_ids": [
                    "54231d4a88ee38334b5b9e1d",
                    "601d660f2be2c8896f86e18d",
                ],
                "geojson_resource_id": "489c3d81-4312-4506-8242-44a674b0bb55",
                "latest_resource_ids": {
                    "2.3.0": "18ac7b73-5781-4493-b98a-d624f9f9ab27",
                    "2.3.1": "9a300b97-d0c4-411b-9830-b2465624cf22",
                    "latest": "f8fdc246-e67b-4006-8f65-6db0f2f9b57b",
                },
                "publication": True,
                "search_words": [
                    "infrastructures de recharge pour véhicules électriques",
                    "bornes de recharge pour véhicules électriques",
                    "IRVE",
                ],
                "tags": ["irve"],
            }
        }

    success = build_reference_table(
        config_dict,
        schema_name,
        schemas_catalogue_list,
        schemas_report_dict,
        validata_reports_path,
        ref_tables_path,
        should_succeed=True,
    )
    assert success

    ti.xcom_push(key="consolidation_date_str", value=consolidation_date_str)
    ti.xcom_push(key="data_path", value=data_path.as_posix())
    ti.xcom_push(key="consolidated_data_path", value=consolidated_data_path.as_posix())
    ti.xcom_push(key="ref_tables_path", value=ref_tables_path.as_posix())
    ti.xcom_push(key="report_tables_path", value=report_tables_path.as_posix())
    ti.xcom_push(key="validata_reports_path", value=validata_reports_path.as_posix())
    ti.xcom_push(key="schemas_report_dict", value=str(schemas_report_dict))
    ti.xcom_push(key="schemas_catalogue_list", value=schemas_catalogue_list)
    ti.xcom_push(key="config_dict", value=str(config_dict))


def download_irve_resources(
    ti: TaskInstance,
) -> None:
    ref_tables_path = ti.xcom_pull(key="ref_tables_path", task_ids="get_all_irve_resources")
    data_path = ti.xcom_pull(key="data_path", task_ids="get_all_irve_resources")
    logging.debug(f"ref_tables_pat={ref_tables_path} -- {type(ref_tables_path)}")
    logging.debug(f"data_path={data_path} -- {type(data_path)}")
    success = download_schema_files(schema_name, ref_tables_path, data_path, should_succeed=True)
    assert success


def consolidate_irve(
    ti: TaskInstance,
    tmp_path: Path,
) -> None:
    ref_tables_path = ti.xcom_pull(key="ref_tables_path", task_ids="get_all_irve_resources")
    data_path = ti.xcom_pull(key="data_path", task_ids="get_all_irve_resources")
    consolidation_date_str = ti.xcom_pull(key="consolidation_date_str", task_ids="get_all_irve_resources")
    consolidated_data_path = ti.xcom_pull(key="consolidated_data_path", task_ids="get_all_irve_resources")
    schemas_report_dict = literal_eval(ti.xcom_pull(key="schemas_report_dict", task_ids="get_all_irve_resources"))
    schemas_catalogue_list = ti.xcom_pull(key="schemas_catalogue_list", task_ids="get_all_irve_resources")
    success = consolidate_data(
        data_path,
        schema_name,
        consolidated_data_path,
        ref_tables_path,
        schemas_catalogue_list,
        consolidation_date_str,
        tmp_path,
        schemas_report_dict,
        should_succeed=True,
    )
    assert success


def custom_filters_irve(
    ti: TaskInstance,
) -> None:
    consolidated_data_path = ti.xcom_pull(key="consolidated_data_path", task_ids="get_all_irve_resources")
    schema_consolidated_data_path = Path(consolidated_data_path) / schema_name.replace("/", "_")
    consolidated_file = [
        f for f in os.listdir(schema_consolidated_data_path) if f.startswith("consolidation") and f.endswith(".csv")
    ][0]
    logging.info(f"Consolidated IRVE file is here: {os.path.join(schema_consolidated_data_path, consolidated_file)}")
    df_conso = pd.read_csv(os.path.join(schema_consolidated_data_path, consolidated_file), dtype=str)
    df_filtered = df_conso.copy()
    # on enlève les lignes publiées par des utilisateurs (aka pas par des organisations)
    df_filtered = df_filtered.loc[df_filtered["is_orga"] == "True"].drop("is_orga", axis=1)

    # pour un id_pdc_itinerance publié plusieurs fois par le même producteur
    # on garde la ligne du fichier le plus récent (si un id_pdc est en double
    # MAIS dans des fichiers de producteurs différents on garde les deux)
    # récent par rapport au champ date_maj (en cas d'égalité, on regarde la date de création de la ressource)
    df_filtered = df_filtered.sort_values(
        [
            "id_pdc_itinerance",
            "datagouv_organization_or_owner",
            "date_maj",
            "created_at",
        ],
        ascending=[True, True, False, False],
    )
    df_filtered = df_filtered.drop_duplicates(
        subset=["id_pdc_itinerance", "datagouv_organization_or_owner"], keep="first"
    )
    logging.info("Consolidated file has", len(df_filtered), "rows")
    df_filtered.to_csv(
        os.path.join(schema_consolidated_data_path, consolidated_file),
        index=False,
        encoding="utf-8",
    )


def improve_irve_geo_data_quality(
    tmp_path: Path,
) -> None:
    def sort_consolidated_from_version(file_name: str) -> list[Union[int, float]]:
        version_lookup = re.search(r"\d+.\d+.\d+", file_name)
        version = str(version_lookup[0]) if version_lookup else "inf"
        return comparer_versions(version)

    schema_irve_cols = {
        "xy_coords": "coordonneesXY",
        "code_insee": "code_insee_commune",
        "adress": "adresse_station",
        "longitude": "consolidated_longitude",
        "latitude": "consolidated_latitude",
    }
    schema_irve_path = os.path.join(tmp_path, "consolidated_data", "etalab_schema-irve-statique")

    latest_version_consolidation = sorted(
        [file for file in os.listdir(schema_irve_path) if file.endswith(".csv")],
        key=sort_consolidated_from_version,
    )[-1]

    logging.info(f"Only processing the latest version (too time-consuming): {latest_version_consolidation}")
    improve_geo_data_quality({os.path.join(schema_irve_path, latest_version_consolidation): schema_irve_cols})


def upload_consolidated_irve(
    ti: TaskInstance,
    config_path: Path,
) -> None:
    consolidation_date_str = ti.xcom_pull(key="consolidation_date_str", task_ids="get_all_irve_resources")
    consolidated_data_path = ti.xcom_pull(key="consolidated_data_path", task_ids="get_all_irve_resources")
    schemas_report_dict = literal_eval(ti.xcom_pull(key="schemas_report_dict", task_ids="get_all_irve_resources"))
    config_dict = literal_eval(ti.xcom_pull(key="config_dict", task_ids="get_all_irve_resources"))
    schemas_catalogue_list = ti.xcom_pull(key="schemas_catalogue_list", task_ids="get_all_irve_resources")

    success = upload_consolidated(
        schema_name,
        consolidated_data_path,
        config_dict,
        schemas_catalogue_list,
        config_path,
        schemas_report_dict,
        consolidation_date_str,
        bool_upload_geojson=True,
        should_succeed=True,
    )
    assert success


def update_reference_table_irve(
    ti: TaskInstance,
) -> None:
    ref_tables_path = ti.xcom_pull(key="ref_tables_path", task_ids="get_all_irve_resources")
    success = update_reference_table(ref_tables_path, schema_name, should_succeed=True)
    assert success


def update_resource_send_mail_producer_irve(
    ti: TaskInstance,
) -> None:
    ref_tables_path = ti.xcom_pull(key="ref_tables_path", task_ids="get_all_irve_resources")
    validata_reports_path = ti.xcom_pull(key="validata_reports_path", task_ids="get_all_irve_resources")
    success = update_resource_send_mail_producer(
        ref_tables_path, schema_name, validata_reports_path, should_succeed=True
    )
    assert success


def update_consolidation_documentation_report_irve(
    ti: TaskInstance,
    config_path: Path,
) -> None:
    ref_tables_path = ti.xcom_pull(key="ref_tables_path", task_ids="get_all_irve_resources")
    consolidation_date_str = ti.xcom_pull(key="consolidation_date_str", task_ids="get_all_irve_resources")
    config_dict = literal_eval(ti.xcom_pull(key="config_dict", task_ids="get_all_irve_resources"))
    success = update_consolidation_documentation_report(
        schema_name,
        ref_tables_path,
        config_path,
        consolidation_date_str,
        config_dict,
        should_succeed=True,
    )
    assert success


def create_consolidation_reports_irve(
    ti: TaskInstance,
) -> None:
    ref_tables_path = ti.xcom_pull(key="ref_tables_path", task_ids="get_all_irve_resources")
    report_tables_path = ti.xcom_pull(key="report_tables_path", task_ids="get_all_irve_resources")
    consolidation_date_str = ti.xcom_pull(key="consolidation_date_str", task_ids="get_all_irve_resources")
    schemas_report_dict = literal_eval(ti.xcom_pull(key="schemas_report_dict", task_ids="get_all_irve_resources"))

    reports_list = []

    for schema_name in schemas_report_dict.keys():
        schema_report_dict = schemas_report_dict[schema_name]
        schema_report_dict["schema_name"] = schema_name
        reports_list += [schema_report_dict]

    reports_df = pd.DataFrame(reports_list)

    reports_df = reports_df[["schema_name"] + [col for col in reports_df.columns if col != "schema_name"]].rename(
        columns={"config_created": "new_config_created"}
    )  # rename to drop at next launch

    stats_df_list: list[pd.DataFrame] = []
    append_stats_list(ref_tables_path, schema_name, stats_df_list)

    stats_df = pd.concat(stats_df_list).reset_index(drop=True)

    reports_df = reports_df.merge(stats_df, on="schema_name", how="left")

    reports_df.head()

    reports_df.to_excel(
        os.path.join(
            report_tables_path,
            "report_by_schema_{}.xlsx".format(consolidation_date_str),
        ),
        index=False,
    )
    reports_df.to_csv(
        os.path.join(
            report_tables_path,
            "report_by_schema_{}.csv".format(consolidation_date_str),
        ),
        index=False,
    )


def create_detailed_report_irve(
    ti: TaskInstance,
) -> None:
    ref_tables_path = ti.xcom_pull(key="ref_tables_path", task_ids="get_all_irve_resources")
    report_tables_path = ti.xcom_pull(key="report_tables_path", task_ids="get_all_irve_resources")
    success = create_detailed_report(ref_tables_path, schema_name, report_tables_path, should_succeed=True)
    assert success


def final_directory_clean_up_irve(
    tmp_path: Path,
    output_data_folder: str,
) -> None:
    tmp_folder = tmp_path.as_posix() + "/"
    final_directory_clean_up(tmp_folder, output_data_folder)


def upload_minio_irve(
    tmp_folder: Path,
    minio_bucket_data_pipeline_open: str,
    minio_output_filepath: str,
) -> None:
    upload_minio(
        tmp_folder.as_posix(),
        minio_bucket_data_pipeline_open,
        minio_output_filepath,
    )


def notification_synthese_irve(
    minio_url: str,
    minio_bucket_data_pipeline_open: str,
    tmp_folder: Path,
    mattermost_datagouv_schema_activite: str,
) -> None:
    notification_synthese(
        minio_url,
        minio_bucket_data_pipeline_open,
        tmp_folder,
        mattermost_datagouv_schema_activite,
        schema_name,
    )
