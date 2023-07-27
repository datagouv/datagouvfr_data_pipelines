from datagouvfr_data_pipelines.utils.schema import (
    upload_consolidated,
    update_reference_table,
    update_resource_send_mail_producer,
    add_validata_report,
    update_consolidation_documentation_report,
    append_stats_list,
    create_detailed_report,
    final_directory_clean_up,
)
import os
import pandas as pd
import pickle
import requests
from ast import literal_eval


def run_consolidation_upload(
    ti,
    api_url: str,
    api_key: str,
    tmp_path: str,
    date_airflow: str,
    schema_catalog: str,
    output_data_folder: str,
    config_path: str,
) -> None:

    consolidation_date_str = ti.xcom_pull(key='consolidation_date_str', task_ids='run_schemas_consolidation')
    consolidated_data_path = ti.xcom_pull(key='consolidated_data_path', task_ids='run_schemas_consolidation')
    ref_tables_path = ti.xcom_pull(key='ref_tables_path', task_ids='run_schemas_consolidation')
    validata_reports_path = ti.xcom_pull(key='validata_reports_path', task_ids='run_schemas_consolidation')
    report_tables_path = ti.xcom_pull(key='report_tables_path', task_ids='run_schemas_consolidation')
    schemas_report_dict = literal_eval(
        ti.xcom_pull(key='schemas_report_dict', task_ids='run_schemas_consolidation')
    )
    config_dict = literal_eval(
        ti.xcom_pull(key='config_dict', task_ids='run_schemas_consolidation')
    )
    schemas_catalogue_list = ti.xcom_pull(key='schemas_catalogue_list', task_ids='run_schemas_consolidation')

    with open(tmp_path / "schemas_report_dict.pickle", "rb") as f:
        schemas_report_dict = pickle.load(f)

    consolidation_date_str = date_airflow.replace("-", "")

    schemas_list_url = schema_catalog
    schemas_catalogue_dict = requests.get(schemas_list_url).json()
    schemas_catalogue_list = [
        schema
        for schema in schemas_catalogue_dict["schemas"]
        if schema["schema_type"] == "tableschema"
    ]

    # ## Upload
    for schema_name in config_dict.keys():
        upload_consolidated(
            schema_name,
            consolidated_data_path,
            config_dict,
            schemas_catalogue_list,
            config_path,
            schemas_report_dict,
            consolidation_date_str,
            api_key,
            bool_upload_geojson=False,
        )

    # ## Schemas (versions) feedback loop on resources
    # ### Adding needed infos for each resource in reference tables
    for schema_name in config_dict.keys():
        update_reference_table(
            ref_tables_path,
            schema_name,
        )

    # ### Updating resources schemas and sending comments/mails to notify producers
    for schema_name in config_dict.keys():
        update_resource_send_mail_producer(
            ref_tables_path,
            schema_name,
            api_key,
        )

    # ### Add validata report to extras for each resource
    for schema_name in config_dict.keys():
        add_validata_report(
            ref_tables_path,
            validata_reports_path,
            schema_name,
            api_key,
        )

    # ## Updating consolidation documentation resource
    for schema_name in config_dict.keys():
        update_consolidation_documentation_report(
            schema_name,
            ref_tables_path,
            config_path,
            consolidation_date_str,
            config_dict,
            api_key,
        )

    # ## Consolidation Reports
    # ### Report by schema
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
        append_stats_list(
            ref_tables_path,
            schema_name,
            stats_df_list
        )

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

    # ## Detailed reports (by schema and resource source)
    for schema_name in config_dict.keys():
        create_detailed_report(
            ref_tables_path,
            schema_name,
            report_tables_path,
        )

    # %%
    tmp_folder = tmp_path.as_posix() + "/"
    final_directory_clean_up(
        tmp_folder,
        output_data_folder
    )
