from datetime import timedelta, datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.bash import BashOperator

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
)
from datagouvfr_data_pipelines.data_processing.dfi.task_functions import (
    check_if_modif,
    gather_data,
    send_to_minio,
    publish_on_datagouv,
    notification_mattermost,
)

TMP_FOLDER = f"{AIRFLOW_DAG_TMP}dfi/"
DAG_NAME = "data_processing_dfi_consolidation"

default_args = {
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
    "provide_context": True,
}


with DAG(
    dag_id=DAG_NAME,
    schedule_interval="0 */4 * * *",
    start_date=datetime(2024, 8, 10),
    dagrun_timeout=timedelta(minutes=240),
    tags=["dfi", "consolidation", "datagouv"],
    catchup=False,
    default_args=default_args,
) as dag:
    clean_previous_outputs = BashOperator(
        task_id="clean_previous_outputs",
        bash_command=f"rm -rf {TMP_FOLDER} && mkdir -p {TMP_FOLDER}",
    )

    check_if_modif = ShortCircuitOperator(
        task_id="check_if_modif",
        python_callable=check_if_modif,
    )

    gather_data = PythonOperator(
        task_id="gather_data",
        python_callable=gather_data,
    )

    send_to_minio = PythonOperator(
        task_id="send_to_minio",
        python_callable=send_to_minio,
    )

    publish_on_datagouv = PythonOperator(
        task_id="publish_on_datagouv",
        python_callable=publish_on_datagouv,
    )

    clean_up = BashOperator(
        task_id="clean_up",
        bash_command=f"rm -rf {TMP_FOLDER}",
    )

    notification_mattermost = PythonOperator(
        task_id="notification_mattermost",
        python_callable=notification_mattermost,
    )

    check_if_modif.set_upstream(clean_previous_outputs)
    gather_data.set_upstream(check_if_modif)
    send_to_minio.set_upstream(gather_data)
    publish_on_datagouv.set_upstream(send_to_minio)
    clean_up.set_upstream(publish_on_datagouv)
    notification_mattermost.set_upstream(clean_up)
