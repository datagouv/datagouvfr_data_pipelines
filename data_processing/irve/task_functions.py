from datagouvfr_data_pipelines.utils.schema import (
    remove_old_schemas,
    get_schema_report,
    build_reference_table,
    download_schema_files,
    consolidate_data,
    upload_consolidated,
    update_reference_table,
    update_resource_send_mail_producer,
    add_validata_report,
    update_consolidation_documentation_report,
    append_stats_list,
    create_detailed_report,
    final_directory_clean_up,
    notification_synthese
)
import yaml
from ast import literal_eval
import pandas as pd
import os

schema_name = "etalab/schema-irve-statique"


def get_all_irve_resources(
    ti,
    date_airflow,
    tmp_path,
    schemas_catalogue_url,
    config_path
):
    consolidation_date_str = date_airflow.replace("-", "")
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

    schemas_report_dict, schemas_catalogue_list = get_schema_report(
        schemas_catalogue_url=schemas_catalogue_url,
        config_path=config_path,
        schema_name=schema_name
    )

    with open(config_path, "r") as f:
        config_dict = yaml.safe_load(f)
        config_dict = remove_old_schemas(config_dict, schemas_catalogue_list, single_schema=True)

    success = build_reference_table(
        config_dict,
        schema_name,
        schemas_catalogue_list,
        schemas_report_dict,
        validata_reports_path,
        ref_tables_path,
        should_succeed=True
    )
    assert success

    ti.xcom_push(key='consolidation_date_str', value=consolidation_date_str)
    ti.xcom_push(key='data_path', value=data_path.as_posix())
    ti.xcom_push(key='consolidated_data_path', value=consolidated_data_path.as_posix())
    ti.xcom_push(key='ref_tables_path', value=ref_tables_path.as_posix())
    ti.xcom_push(key='report_tables_path', value=report_tables_path.as_posix())
    ti.xcom_push(key='validata_reports_path', value=validata_reports_path.as_posix())
    ti.xcom_push(key='schemas_report_dict', value=str(schemas_report_dict))
    ti.xcom_push(key='schemas_catalogue_list', value=schemas_catalogue_list)
    return


def download_irve_resources(ti):
    ref_tables_path = ti.xcom_pull(key='ref_tables_path', task_ids='get_all_irve_resources')
    data_path = ti.xcom_pull(key='data_path', task_ids='get_all_irve_resources')
    success = download_schema_files(
        schema_name,
        ref_tables_path,
        data_path,
        should_succeed=True
    )
    assert success
    return


def consolidate_irve(
    ti,
    tmp_path
):
    ref_tables_path = ti.xcom_pull(key='ref_tables_path', task_ids='get_all_irve_resources')
    data_path = ti.xcom_pull(key='data_path', task_ids='get_all_irve_resources')
    consolidation_date_str = ti.xcom_pull(key='consolidation_date_str', task_ids='get_all_irve_resources')
    consolidated_data_path = ti.xcom_pull(key='consolidated_data_path', task_ids='get_all_irve_resources')
    schemas_report_dict = literal_eval(
        ti.xcom_pull(key='schemas_report_dict', task_ids='get_all_irve_resources')
    )
    schemas_catalogue_list = ti.xcom_pull(key='schemas_catalogue_list', task_ids='get_all_irve_resources')
    success = consolidate_data(
        data_path,
        schema_name,
        consolidated_data_path,
        ref_tables_path,
        schemas_catalogue_list,
        consolidation_date_str,
        tmp_path,
        schemas_report_dict,
        should_succeed=True
    )
    assert success
    return


def upload_consolidated_irve(
    ti,
    api_key,
    config_path,
    single_schema
):
    consolidation_date_str = ti.xcom_pull(key='consolidation_date_str', task_ids='get_all_irve_resources')
    consolidated_data_path = ti.xcom_pull(key='consolidated_data_path', task_ids='get_all_irve_resources')
    schemas_report_dict = literal_eval(
        ti.xcom_pull(key='schemas_report_dict', task_ids='get_all_irve_resources')
    )
    schemas_catalogue_list = ti.xcom_pull(key='schemas_catalogue_list', task_ids='get_all_irve_resources')

    with open(config_path, "r") as f:
        config_dict = yaml.safe_load(f)
    config_dict = remove_old_schemas(config_dict, schemas_catalogue_list, single_schema)

    success = upload_consolidated(
        schema_name,
        consolidated_data_path,
        config_dict,
        schemas_catalogue_list,
        config_path,
        schemas_report_dict,
        consolidation_date_str,
        single_schema,
        api_key,
        bool_upload_geojson=True,
        should_succeed=True
    )
    assert success
    return


def update_reference_table_irve(
    ti
):
    ref_tables_path = ti.xcom_pull(key='ref_tables_path', task_ids='get_all_irve_resources')
    success = update_reference_table(
        ref_tables_path,
        schema_name,
        should_succeed=True
    )
    assert success
    return


def update_resource_send_mail_producer_irve(
    ti,
    api_key
):
    ref_tables_path = ti.xcom_pull(key='ref_tables_path', task_ids='get_all_irve_resources')
    success = update_resource_send_mail_producer(
        ref_tables_path,
        schema_name,
        api_key,
        should_succeed=False
    )
    assert success
    return


def add_validata_report_irve(
    ti,
    api_key
):
    ref_tables_path = ti.xcom_pull(key='ref_tables_path', task_ids='get_all_irve_resources')
    validata_reports_path = ti.xcom_pull(key='validata_reports_path', task_ids='get_all_irve_resources')
    success = add_validata_report(
        ref_tables_path,
        validata_reports_path,
        schema_name,
        api_key,
        should_succeed=True
    )
    assert success
    return


def update_consolidation_documentation_report_irve(
    ti,
    api_key,
    config_path,
    single_schema
):
    ref_tables_path = ti.xcom_pull(key='ref_tables_path', task_ids='get_all_irve_resources')
    schemas_catalogue_list = ti.xcom_pull(key='schemas_catalogue_list', task_ids='get_all_irve_resources')
    consolidation_date_str = ti.xcom_pull(key='consolidation_date_str', task_ids='get_all_irve_resources')
    success = update_consolidation_documentation_report(
        schema_name,
        ref_tables_path,
        config_path,
        schemas_catalogue_list,
        consolidation_date_str,
        single_schema,
        api_key,
        should_succeed=True
    )
    assert success
    return


def create_consolidation_reports_irve(
    ti,
    config_path,
):
    ref_tables_path = ti.xcom_pull(key='ref_tables_path', task_ids='get_all_irve_resources')
    schemas_catalogue_list = ti.xcom_pull(key='schemas_catalogue_list', task_ids='get_all_irve_resources')
    report_tables_path = ti.xcom_pull(key='report_tables_path', task_ids='get_all_irve_resources')
    consolidation_date_str = ti.xcom_pull(key='consolidation_date_str', task_ids='get_all_irve_resources')
    schemas_report_dict = literal_eval(
        ti.xcom_pull(key='schemas_report_dict', task_ids='get_all_irve_resources')
    )

    with open(config_path, "r") as f:
        config_dict = yaml.safe_load(f)
        config_dict = remove_old_schemas(config_dict, schemas_catalogue_list, single_schema=True)

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


def create_detailed_report_irve(
    ti
):
    ref_tables_path = ti.xcom_pull(key='ref_tables_path', task_ids='get_all_irve_resources')
    report_tables_path = ti.xcom_pull(key='report_tables_path', task_ids='get_all_irve_resources')
    success = create_detailed_report(
        ref_tables_path,
        schema_name,
        report_tables_path,
        should_succeed=True
    )
    assert success
    return


def final_directory_clean_up_irve(
    tmp_path,
    output_data_folder
):
    tmp_folder = tmp_path.as_posix() + "/"
    final_directory_clean_up(
        tmp_folder,
        output_data_folder
    )


def notification_synthese_irve(
    MINIO_URL,
    MINIO_BUCKET_DATA_PIPELINE_OPEN,
    TMP_FOLDER,
    SECRET_MINIO_DATA_PIPELINE_USER,
    SECRET_MINIO_DATA_PIPELINE_PASSWORD,
    MATTERMOST_DATAGOUV_SCHEMA_ACTIVITE,
    **kwargs
):
    notification_synthese(
        MINIO_URL,
        MINIO_BUCKET_DATA_PIPELINE_OPEN,
        TMP_FOLDER,
        SECRET_MINIO_DATA_PIPELINE_USER,
        SECRET_MINIO_DATA_PIPELINE_PASSWORD,
        MATTERMOST_DATAGOUV_SCHEMA_ACTIVITE,
        schema_name,
        **kwargs
    )
