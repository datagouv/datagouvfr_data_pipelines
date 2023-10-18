from airflow.models import DAG
from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
)
from dags.datagouvfr_data_pipelines.data_processing.rne.flux.task_functions import (
    TMP_FOLDER,
    get_every_day_flux,
    send_rne_flux_to_minio,
    send_notification_mattermost,
)


with DAG(
    dag_id="data_processing_flux_rne",
    start_date=datetime(2023, 10, 5),
    # schedule_interval="0 0 * * FRI",  # every Friday at midnight
    catchup=False,
    dagrun_timeout=timedelta(minutes=(60 * 8)),
    tags=["data_processing", "rne", "flux"],
    params={},
) as dag:
    clean_previous_outputs = BashOperator(
        task_id="clean_previous_outputs",
        bash_command=f"rm -rf {TMP_FOLDER} && mkdir -p {TMP_FOLDER}",
    )

    get_daily_flux_rne = PythonOperator(
        task_id="get_every_day_flux", python_callable=get_every_day_flux
    )

    upload_rne_flux_to_minio = PythonOperator(
        task_id="upload_rne_flux_to_minio", python_callable=send_rne_flux_to_minio
    )

    send_notification_mattermost = PythonOperator(
        task_id="send_notification_mattermost",
        python_callable=send_notification_mattermost,
    )

    get_daily_flux_rne.set_upstream(clean_previous_outputs)
    upload_rne_flux_to_minio.set_upstream(get_daily_flux_rne)
    send_notification_mattermost.set_upstream(upload_rne_flux_to_minio)
