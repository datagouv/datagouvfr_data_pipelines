from datetime import datetime, timedelta

from airflow.providers.standard.operators.python import ShortCircuitOperator
from airflow.sdk import DAG, Param
from airflow.sdk.definitions.param import ParamsDict
from datagouvfr_data_pipelines.data_processing.dvf.geoloc.task_functions import (
    TMP_FOLDER,
    build_full_file,
    check_if_modif,
    download_cadastre_source_data,
    download_dvf_source_data,
    download_published_yearly_files,
    enrich_years,
    process_cadastre_cols,
    publish_concat,
    publish_yearly,
)
from datagouvfr_data_pipelines.utils.tasks import clean_up_folder

with DAG(
    dag_id="data_processing_dvf_geoloc",
    schedule=None,
    start_date=datetime(2026, 4, 15),
    catchup=False,
    dagrun_timeout=timedelta(minutes=1200),
    tags=["data_processing", "dvf"],
    params=ParamsDict(
        {
            "year_to_run": Param(
                None,
                type=["null", "integer"],
                description="Only rebuild the selected year if set, for faster debug.",
            ),
            "delivery": Param(
                None,
                type=["null", "string"],
                enum=[None, "april", "october"],
                description=(
                    "Force the cadastre millesime of the latest April (YYYY-03-01) "
                    "or October (YYYY-06-01) delivery, for debug. "
                    "Defaults to the delivery matching the current date."
                ),
            ),
            "only": Param(
                "none",
                type="string",
                enum=["none", "only-concatenated", "only-yearly"],
                description=(
                    "Restrict the run to a subset of outputs: 'none' (default) publishes "
                    "both the yearly full-*.csv.gz files and the concat file dvf.csv.gz; "
                    "'only-concatenated' runs only the concat (skipping the heavy "
                    "enrichment, reusing the yearly files already published on the "
                    "destination dataset); 'only-yearly' runs only the yearly files "
                    "(skipping the concat file)."
                ),
            ),
        },
    ),
):
    source_download = download_dvf_source_data()
    yearly_files = enrich_years(unzipped_file_names=source_download)
    cadastre = download_cadastre_source_data()
    cols = process_cadastre_cols()
    # the concat file is produced either by the heavy enrichment (default / only-yearly)
    # or by reusing the published yearly files (only-concatenated); build_full_file has
    # trigger_rule="none_failed" so it runs whichever branch produced them.
    concat_file = build_full_file()

    ShortCircuitOperator(
        task_id="check_if_modif",
        python_callable=check_if_modif,
    ) >> (
        clean_up_folder(TMP_FOLDER, recreate=True)
        >> source_download
        >> yearly_files
        >> cadastre
        >> cols
    )
    # yearly files for the concat come from the heavy run, or from the already-published
    # files in only-concatenated mode (heavy tasks are skipped then)
    download_published_yearly_files() >> concat_file
    cols >> concat_file
    concat_file >> publish_concat()
    cols >> publish_yearly()
    # # >> notification()
    # # >> TriggerDagRunOperator(
    # #     task_id="trigger_dvf_explore",
    # #     trigger_dag_id="data_processing_dvf_explore",
    # # )
