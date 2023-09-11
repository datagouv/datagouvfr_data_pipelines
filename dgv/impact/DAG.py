from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_HOME,
    AIRFLOW_DAG_TMP,
)
from datagouvfr_data_pipelines.data_processing.rna.task_functions import (
    process_rna,
    send_rna_to_minio,
    publish_rna_communautaire,
    send_notification_mattermost,
)

TMP_FOLDER = f"{AIRFLOW_DAG_TMP}dgv_impact/"
DAG_FOLDER = 'datagouvfr_data_pipelines/data_processing/'
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

    send_rna_to_minio = PythonOperator(
        task_id='send_rna_to_minio',
        python_callable=send_rna_to_minio,
    )

    publish_rna_communautaire = PythonOperator(
        task_id='publish_rna_communautaire',
        python_callable=publish_rna_communautaire,
    )

    send_notification_mattermost = PythonOperator(
        task_id='send_notification_mattermost',
        python_callable=send_notification_mattermost,
    )

    send_rna_to_minio.set_upstream(process_rna)
    publish_rna_communautaire.set_upstream(send_rna_to_minio)
    send_notification_mattermost.set_upstream(publish_rna_communautaire)