from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.python import ShortCircuitOperator

from datagouvfr_data_pipelines.dgv.monitoring.bizdev.task_functions import (
    DAG_NAME,
    TMP_FOLDER,
    curation_tasks,
    edito_tasks,
    send_tables_to_s3,
    publish_mattermost,
)
from datagouvfr_data_pipelines.utils.tasks import clean_up_folder
from datagouvfr_data_pipelines.utils.utils import (
    check_if_monday,
    check_if_first_day_of_month,
)


default_args = {
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_NAME,
    # every Monday (for spam, empty JDD and KO reuses) and every 1st day of month for tops
    schedule="0 4 1 * 1",
    start_date=datetime(2023, 10, 15),
    dagrun_timeout=timedelta(minutes=120),
    tags=["curation", "bizdev", "monthly", "datagouv"],
    default_args=default_args,
    catchup=False,
):
    clean_up_create = clean_up_folder(TMP_FOLDER, recreate=True)
    _send_tables_to_s3 = send_tables_to_s3()

    (
        clean_up_create
        >> ShortCircuitOperator(
            task_id="check_if_monday",
            python_callable=check_if_monday,
        )
        >> curation_tasks
        >> _send_tables_to_s3
    )

    (
        clean_up_create
        >> ShortCircuitOperator(
            task_id="check_if_first_day_of_month",
            python_callable=check_if_first_day_of_month,
        )
        >> edito_tasks
        >> _send_tables_to_s3
    )

    _send_tables_to_s3 >> publish_mattermost() >> clean_up_folder(TMP_FOLDER)
