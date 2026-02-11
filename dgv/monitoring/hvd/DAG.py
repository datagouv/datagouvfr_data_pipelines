from datetime import timedelta, datetime
from airflow.models import DAG
from airflow.operators.python import ShortCircuitOperator

from datagouvfr_data_pipelines.utils.utils import check_if_monday
from datagouvfr_data_pipelines.dgv.monitoring.hvd.task_functions import (
    DAG_NAME,
    DATADIR,
    get_hvd,
    send_to_s3,
    publish_mattermost,
    build_df_for_grist,
    update_grist,
    publish_mattermost_grist,
)
from datagouvfr_data_pipelines.utils.tasks import clean_up_folder

default_args = {
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_NAME,
    schedule="0 6 * * *",
    start_date=datetime(2024, 6, 1),
    dagrun_timeout=timedelta(minutes=60),
    tags=["hvd", "datagouv"],
    default_args=default_args,
    catchup=False,
):

    clean_up_recreate = clean_up_folder(DATADIR, recreate=True)
    
    # Recap HVD mattermost
    (
        clean_up_recreate
        >> ShortCircuitOperator(
            task_id="check_if_monday",
            python_callable=check_if_monday,
        )
        >> get_hvd()
        >> send_to_s3()
        >> publish_mattermost()
    )

    # Grist
    (
        clean_up_recreate
        >> build_df_for_grist()
        >> ShortCircuitOperator(
            task_id="update_grist",
            python_callable=update_grist,
        )
        >> publish_mattermost_grist()
    )
