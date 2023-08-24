from datagouvfr_data_pipelines.utils.schema import (
    remove_old_schemas,
    get_schema_report,
    build_reference_table,
    download_schema_files,
    consolidate_data,
)
import pandas as pd
import yaml
pd.set_option('display.max_columns', None)


VALIDATA_BASE_URL = (
    "https://validata-api.app.etalab.studio/validate?schema={schema_url}&url={rurl}"
)


def run_schemas_consolidation(
    ti,
    tmp_path: str,
    date_airflow: str,
    schema_catalog_url: str,
    config_path: str,
) -> None:

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

    print('Building catalogue')
    schemas_report_dict, schemas_catalogue_list = get_schema_report(
        schemas_catalogue_url=schema_catalog_url,
        config_path=config_path,
        list_schema_skip=['etalab/schema-irve-statique']
    )

    print('Loading config dict')
    with open(config_path, "r") as f:
        config_dict = yaml.safe_load(f)
        config_dict = remove_old_schemas(config_dict, schemas_catalogue_list)
    print(config_dict)

    # ## Building reference tables (parsing and listing resources + Validata check)
    print('Building reference tables')
    for schema_name in config_dict.keys():
        build_reference_table(
            config_dict,
            schema_name,
            schemas_catalogue_list,
            schemas_report_dict,
            validata_reports_path,
            ref_tables_path,
        )

    # ## Downloading valid data
    # We download only data that is valid for at least one version of the schema.
    print('Downloading valid data')
    for schema_name in config_dict.keys():
        if config_dict[schema_name]["consolidate"]:
            download_schema_files(
                schema_name,
                ref_tables_path,
                data_path,
            )

    # ## Consolidation
    print('Consolidating data')
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

    ti.xcom_push(key='consolidation_date_str', value=consolidation_date_str)
    ti.xcom_push(key='data_path', value=data_path.as_posix())
    ti.xcom_push(key='consolidated_data_path', value=consolidated_data_path.as_posix())
    ti.xcom_push(key='ref_tables_path', value=ref_tables_path.as_posix())
    ti.xcom_push(key='report_tables_path', value=report_tables_path.as_posix())
    ti.xcom_push(key='validata_reports_path', value=validata_reports_path.as_posix())
    ti.xcom_push(key='schemas_report_dict', value=str(schemas_report_dict))
    ti.xcom_push(key='schemas_catalogue_list', value=schemas_catalogue_list)
    ti.xcom_push(key='config_dict', value=str(config_dict))
