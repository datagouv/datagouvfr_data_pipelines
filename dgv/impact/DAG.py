from datetime import timedelta, datetime
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
)
from datagouvfr_data_pipelines.dgv.impact.task_functions import (
    calculate_quality_score,
    calculate_time_for_legitimate_answer,
    get_quality_reuses,
    get_discoverability,
    gather_kpis,
    send_stats_to_minio,
    publish_datagouv,
    send_notification_mattermost
)

TMP_FOLDER = f"{AIRFLOW_DAG_TMP}dgv_impact/"
DAG_FOLDER = 'datagouvfr_data_pipelines/dgv/impact/'
DAG_NAME = 'dgv_impact'
DATADIR = f"{TMP_FOLDER}data"

default_args = {
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_NAME,
    schedule_interval='0 5 1 * *',
    start_date=datetime(2023, 10, 15),
    catchup=False,
    dagrun_timeout=timedelta(minutes=120),
    tags=["datagouv", "impact", "metrics"],
    default_args=default_args,
) as dag:

    clean_previous_outputs = BashOperator(
        task_id="clean_previous_outputs",
        bash_command=(
            f"rm -rf {TMP_FOLDER} && "
            f"mkdir -p {TMP_FOLDER} && "
            f"mkdir -p {DATADIR}"
        ),
    )

    calculate_quality_score = PythonOperator(
        task_id='calculate_quality_score',
        python_callable=calculate_quality_score,
    )

    calculate_time_for_legitimate_answer = PythonOperator(
        task_id='calculate_time_for_legitimate_answer',
        python_callable=calculate_time_for_legitimate_answer,
    )

    get_quality_reuses = PythonOperator(
        task_id='get_quality_reuses',
        python_callable=get_quality_reuses,
    )

    get_discoverability = PythonOperator(
        task_id='get_discoverability',
        python_callable=get_discoverability,
    )

    gather_kpis = PythonOperator(
        task_id='gather_kpis',
        python_callable=gather_kpis,
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

    clean_up = BashOperator(
        task_id="clean_up",
        bash_command=f"rm -rf {TMP_FOLDER}",
    )

    send_notification_mattermost = PythonOperator(
        task_id='send_notification_mattermost',
        python_callable=send_notification_mattermost,
        op_kwargs={
            "DAG_FOLDER": DAG_FOLDER,
        },
    )

    calculate_quality_score.set_upstream(clean_previous_outputs)
    calculate_time_for_legitimate_answer.set_upstream(clean_previous_outputs)
    get_quality_reuses.set_upstream(clean_previous_outputs)
    get_discoverability.set_upstream(clean_previous_outputs)

    gather_kpis.set_upstream(calculate_quality_score)
    gather_kpis.set_upstream(calculate_time_for_legitimate_answer)
    gather_kpis.set_upstream(get_quality_reuses)
    gather_kpis.set_upstream(get_discoverability)

    send_stats_to_minio.set_upstream(gather_kpis)
    publish_datagouv.set_upstream(send_stats_to_minio)
    clean_up.set_upstream(publish_datagouv)
    send_notification_mattermost.set_upstream(clean_up)
