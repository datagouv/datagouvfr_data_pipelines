from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_HOME,
    AIRFLOW_DAG_TMP,
)
from datagouvfr_data_pipelines.dgv.impact.task_functions import (
    calculate_metrics,
    send_stats_to_minio,
    publish_datagouv,
    send_notification_mattermost
)

TMP_FOLDER = f"{AIRFLOW_DAG_TMP}dgv_impact/"
DAG_FOLDER = 'datagouvfr_data_pipelines/dgv/impact/'
DAG_NAME = 'dgv_impact'
DATADIR = f"{TMP_FOLDER}data"

default_args = {
    'email': [
        'pierlou.ramade@data.gouv.fr',
        'geoffrey.aldebert@data.gouv.fr'
    ],
    'email_on_failure': False
}

with DAG(
    dag_id=DAG_NAME,
    schedule_interval='0 5 1 * *',
    start_date=days_ago(1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=120),
    tags=["datagouv", "impact", "metrics"],
    default_args=default_args,
) as dag:

    clean_previous_outputs = BashOperator(
        task_id="clean_previous_outputs",
        bash_command=f"rm -rf {TMP_FOLDER} && mkdir -p {TMP_FOLDER}",
    )

    download_history = BashOperator(
        task_id='download_history',
        bash_command=(
            f"bash {AIRFLOW_DAG_HOME}{DAG_FOLDER}"
            f"scripts/script_dl_history.sh {DATADIR} "
        )
    )

    calculate_metrics = PythonOperator(
        task_id='calculate_metrics',
        python_callable=calculate_metrics,
    )

    send_stats_to_minio = PythonOperator(
        task_id='send_stats_to_minio',
        python_callable=send_stats_to_minio,
    )

    publish_datagouv = PythonOperator(
        task_id='publish_datagouv',
        python_callable=publish_datagouv,
        op_kwargs={
            "DAG_FOLDER": DAG_FOLDER,
        },
    )

    send_notification_mattermost = PythonOperator(
        task_id='send_notification_mattermost',
        python_callable=send_notification_mattermost,
        op_kwargs={
            "DAG_FOLDER": DAG_FOLDER,
        },
    )

    download_history.set_upstream(clean_previous_outputs)
    calculate_metrics.set_upstream(download_history)
    send_stats_to_minio.set_upstream(calculate_metrics)
    publish_datagouv.set_upstream(send_stats_to_minio)
    send_notification_mattermost.set_upstream(publish_datagouv)
