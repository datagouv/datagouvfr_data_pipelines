from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_HOME,
    AIRFLOW_DAG_TMP,
)
from datagouvfr_data_pipelines.data_processing.dvf.task_functions import (
    get_year_interval,
    alter_dvf_table,
    create_copro_table,
    populate_copro_table,
    process_dpe,
    create_dpe_table,
    populate_dpe_table,
    index_dpe_table,
    create_dvf_table,
    populate_dvf_table,
    index_dvf_table,
    create_stats_dvf_table,
    get_epci,
    populate_stats_dvf_table,
    process_dvf_stats,
    publish_stats_dvf,
    send_stats_to_s3,
    notification_mattermost,
    create_distribution_and_stats_whole_period,
    create_distribution_table,
    populate_distribution_table,
    create_whole_period_table,
    populate_whole_period_table,
    send_distribution_to_s3,
    concat_and_publish_whole,
)
from datagouvfr_data_pipelines.utils.tasks import clean_up_folder

TMP_FOLDER = f"{AIRFLOW_DAG_TMP}dvf/"
DAG_FOLDER = "datagouvfr_data_pipelines/data_processing/"
DAG_NAME = "data_processing_dvf"
DATADIR = f"{AIRFLOW_DAG_TMP}dvf/data"
start, end = get_year_interval()

with DAG(
    dag_id=DAG_NAME,
    schedule=None,
    start_date=datetime(2024, 8, 10),
    catchup=False,
    dagrun_timeout=timedelta(minutes=300),
    tags=["data_processing", "dvf", "stats"],
):
    final_clean_up = clean_up_folder(TMP_FOLDER)

    download_dvf_data = BashOperator(
        task_id="download_dvf_data",
        bash_command=(
            f"sh {AIRFLOW_DAG_HOME}{DAG_FOLDER}"
            f"dvf/scripts/script_dl_dvf.sh {DATADIR} "
            f"{start} {end} "
        ),
    )

    with TaskGroup("copro") as copro:
        (
            BashOperator(
                task_id="download_copro",
                bash_command=(
                    f"sh {AIRFLOW_DAG_HOME}{DAG_FOLDER}"
                    f"dvf/scripts/script_dl_copro.sh {DATADIR} "
                ),
            )
            >> create_copro_table()
            >> populate_copro_table()
        )

    with TaskGroup("dpe") as dpe:
        (
            BashOperator(
                task_id="download_dpe",
                bash_command=(
                    f"sh {AIRFLOW_DAG_HOME}{DAG_FOLDER}dvf/scripts/script_dl_dpe.sh {DATADIR} "
                )
            )
            >> process_dpe()
            >> create_dpe_table()
            >> populate_dpe_table()
            >> index_dpe_table()
        )

    with TaskGroup("raw_dvf") as raw_dvf:
        create_dvf_table() >> populate_dvf_table() >> alter_dvf_table() >> index_dvf_table()

    (
        clean_up_folder(TMP_FOLDER, recreate=True)
        >> download_dvf_data
        >> [
            dpe,
            copro,
            raw_dvf,
            concat_and_publish_whole(),
        ]
        >> final_clean_up
        >> notification_mattermost()
    )
    
    _process_stats = process_dvf_stats()
    _create_dist = create_distribution_and_stats_whole_period()

    download_dvf_data >> get_epci() >> _process_stats >> _create_dist

    _process_stats >> create_stats_dvf_table() >> populate_stats_dvf_table() >> final_clean_up

    _create_dist >> send_distribution_to_s3() >> final_clean_up
    _create_dist >> create_distribution_table() >> populate_distribution_table() >> final_clean_up
    _create_dist >> create_whole_period_table() >> populate_whole_period_table() >> final_clean_up
    _create_dist >> send_stats_to_s3() >> publish_stats_dvf() >> final_clean_up
    _create_dist >> create_stats_dvf_table() >> populate_stats_dvf_table() >> final_clean_up
