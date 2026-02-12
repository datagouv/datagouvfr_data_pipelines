from ast import literal_eval
from datetime import datetime
import os
from pathlib import Path

from airflow.decorators import task
import pandas as pd

from datagouvfr_data_pipelines.utils.schema import (
    remove_old_schemas,
    get_schema_report,
    build_reference_table,
    download_schema_files,
    consolidate_data,
    load_config,
    upload_consolidated,
    update_reference_table,
    update_resource_send_mail_producer,
    update_consolidation_documentation_report,
    append_stats_list,
    create_detailed_report,
    final_directory_clean_up,
)

pd.set_option("display.max_columns", None)


@task()
def get_resources(
    tmp_path: Path,
    schema_catalog_url: str,
    config_path: Path,
    **context,
):
    consolidation_date_str = datetime.today().strftime("%Y%m%d")
    print(consolidation_date_str)

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

    print("______________________")
    print("Building catalogue")
    schemas_report_dict, schemas_catalogue_list = get_schema_report(
        schemas_catalogue_url=schema_catalog_url,
        config_path=config_path,
        list_schema_skip=["etalab/schema-irve-statique"],
    )

    print("______________________")
    print("Loading config dict")
    config_dict = load_config(config_path)
    if config_dict:
        config_dict = remove_old_schemas(config_dict, schemas_catalogue_list)
    print(config_dict)

    # ## Building reference tables (parsing and listing resources + Validata check)
    print("______________________")
    print("Building reference tables")
    for schema_name in config_dict.keys():
        build_reference_table(
            config_dict,
            schema_name,
            schemas_catalogue_list,
            schemas_report_dict,
            validata_reports_path,
            ref_tables_path,
        )

    context["ti"].xcom_push(key="consolidation_date_str", value=consolidation_date_str)
    context["ti"].xcom_push(key="data_path", value=data_path.as_posix())
    context["ti"].xcom_push(
        key="consolidated_data_path", value=consolidated_data_path.as_posix()
    )
    context["ti"].xcom_push(key="ref_tables_path", value=ref_tables_path.as_posix())
    context["ti"].xcom_push(
        key="report_tables_path", value=report_tables_path.as_posix()
    )
    context["ti"].xcom_push(
        key="validata_reports_path", value=validata_reports_path.as_posix()
    )
    context["ti"].xcom_push(key="schemas_report_dict", value=str(schemas_report_dict))
    context["ti"].xcom_push(key="schemas_catalogue_list", value=schemas_catalogue_list)
    context["ti"].xcom_push(key="config_dict", value=str(config_dict))


@task()
def download_resources(**context):
    # ## Downloading valid data
    # We download only data that is valid for at least one version of the schema.
    config_dict = literal_eval(
        context["ti"].xcom_pull(key="config_dict", task_ids="get_resources")
    )
    ref_tables_path = context["ti"].xcom_pull(
        key="ref_tables_path", task_ids="get_resources"
    )
    data_path = context["ti"].xcom_pull(key="data_path", task_ids="get_resources")
    print("Downloading valid data")
    for schema_name in config_dict.keys():
        if config_dict[schema_name]["consolidate"]:
            download_schema_files(
                schema_name,
                ref_tables_path,
                data_path,
            )


@task()
def consolidate_resources(tmp_path, **context):
    config_dict = literal_eval(
        context["ti"].xcom_pull(key="config_dict", task_ids="get_resources")
    )
    ref_tables_path = context["ti"].xcom_pull(
        key="ref_tables_path", task_ids="get_resources"
    )
    data_path = context["ti"].xcom_pull(key="data_path", task_ids="get_resources")
    consolidated_data_path = context["ti"].xcom_pull(
        key="consolidated_data_path", task_ids="get_resources"
    )
    schemas_catalogue_list = context["ti"].xcom_pull(
        key="schemas_catalogue_list", task_ids="get_resources"
    )
    consolidation_date_str = context["ti"].xcom_pull(
        key="consolidation_date_str", task_ids="get_resources"
    )
    schemas_report_dict = literal_eval(
        context["ti"].xcom_pull(key="schemas_report_dict", task_ids="get_resources")
    )
    print("Consolidating data")
    for schema_name in config_dict.keys():
        if config_dict[schema_name]["consolidate"]:
            consolidate_data(
                data_path,
                schema_name,
                consolidated_data_path,
                ref_tables_path,
                schemas_catalogue_list,
                consolidation_date_str,
                tmp_path,
                schemas_report_dict,
            )


@task()
def upload_consolidated_data(config_path, **context):
    config_dict = literal_eval(
        context["ti"].xcom_pull(key="config_dict", task_ids="get_resources")
    )
    consolidated_data_path = context["ti"].xcom_pull(
        key="consolidated_data_path", task_ids="get_resources"
    )
    schemas_catalogue_list = context["ti"].xcom_pull(
        key="schemas_catalogue_list", task_ids="get_resources"
    )
    consolidation_date_str = context["ti"].xcom_pull(
        key="consolidation_date_str", task_ids="get_resources"
    )
    schemas_report_dict = literal_eval(
        context["ti"].xcom_pull(key="schemas_report_dict", task_ids="get_resources")
    )
    print("Uploading consolidated data")
    for schema_name in config_dict.keys():
        if config_dict[schema_name]["consolidate"]:
            upload_consolidated(
                schema_name,
                consolidated_data_path,
                config_dict,
                schemas_catalogue_list,
                config_path,
                schemas_report_dict,
                consolidation_date_str,
                bool_upload_geojson=False,
            )


@task()
def update_reference_tables(**context):
    config_dict = literal_eval(
        context["ti"].xcom_pull(key="config_dict", task_ids="get_resources")
    )
    ref_tables_path = context["ti"].xcom_pull(
        key="ref_tables_path", task_ids="get_resources"
    )
    print("Adding infos to resources in reference table")
    for schema_name in config_dict.keys():
        update_reference_table(
            ref_tables_path,
            schema_name,
        )


@task()
def update_resources(**context):
    config_dict = literal_eval(
        context["ti"].xcom_pull(key="config_dict", task_ids="get_resources")
    )
    ref_tables_path = context["ti"].xcom_pull(
        key="ref_tables_path", task_ids="get_resources"
    )
    validata_reports_path = context["ti"].xcom_pull(
        key="validata_reports_path", task_ids="get_resources"
    )
    print("Adding schema to resources and emailing producers")
    for schema_name in config_dict.keys():
        update_resource_send_mail_producer(
            ref_tables_path,
            schema_name,
            validata_reports_path,
        )


@task()
def update_consolidation_documentation(config_path, **context):
    config_dict = literal_eval(
        context["ti"].xcom_pull(key="config_dict", task_ids="get_resources")
    )
    ref_tables_path = context["ti"].xcom_pull(
        key="ref_tables_path", task_ids="get_resources"
    )
    consolidation_date_str = context["ti"].xcom_pull(
        key="consolidation_date_str", task_ids="get_resources"
    )
    print("Updating consolidated data documentation")
    for schema_name in config_dict.keys():
        if config_dict[schema_name]["consolidate"]:
            update_consolidation_documentation_report(
                schema_name,
                ref_tables_path,
                config_path,
                consolidation_date_str,
                config_dict,
            )


@task()
def create_consolidation_reports(**context):
    schemas_report_dict = literal_eval(
        context["ti"].xcom_pull(key="schemas_report_dict", task_ids="get_resources")
    )
    ref_tables_path = context["ti"].xcom_pull(
        key="ref_tables_path", task_ids="get_resources"
    )
    consolidation_date_str = context["ti"].xcom_pull(
        key="consolidation_date_str", task_ids="get_resources"
    )
    report_tables_path = context["ti"].xcom_pull(
        key="report_tables_path", task_ids="get_resources"
    )
    print("Building consolidation reports")
    reports_list = []

    for schema_name in schemas_report_dict.keys():
        schema_report_dict = schemas_report_dict[schema_name]
        schema_report_dict["schema_name"] = schema_name
        reports_list += [schema_report_dict]

    reports_df = pd.DataFrame(reports_list)

    reports_df = reports_df[
        ["schema_name"] + [col for col in reports_df.columns if col != "schema_name"]
    ].rename(
        columns={"config_created": "new_config_created"}
    )  # rename to drop at next launch

    stats_df_list = []
    for schema_name in schemas_report_dict.keys():
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


@task()
def create_detailed_reports(**context):
    config_dict = literal_eval(
        context["ti"].xcom_pull(key="config_dict", task_ids="get_resources")
    )
    ref_tables_path = context["ti"].xcom_pull(
        key="ref_tables_path", task_ids="get_resources"
    )
    report_tables_path = context["ti"].xcom_pull(
        key="report_tables_path", task_ids="get_resources"
    )
    print("Creating detailed reports")
    for schema_name in config_dict.keys():
        create_detailed_report(
            ref_tables_path,
            schema_name,
            report_tables_path,
        )


@task()
def final_clean_up(tmp_path, output_data_folder):
    print("Final cleanup")
    tmp_folder = tmp_path.as_posix() + "/"
    final_directory_clean_up(tmp_folder, output_data_folder)
