from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from operators.clean_folder import CleanFolderOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_HOME,
    AIRFLOW_DAG_TMP,
)
from datagouvfr_data_pipelines.data_processing.rna.task_functions import (
    process_rna,
    create_rna_table,
    populate_rna_table,
    index_rna_table,
    send_rna_to_minio,
    publish_rna_communautaire
)
import requests

TMP_FOLDER = f"{AIRFLOW_DAG_TMP}rna/"
DAG_FOLDER = 'datagouvfr_data_pipelines/data_processing/'
DAG_NAME = 'data_processing_rna'
DATADIR = f"{AIRFLOW_DAG_TMP}rna/data"
SQLDIR = f"{AIRFLOW_DAG_TMP}rna/sql"

resources = requests.get(
    "https://www.data.gouv.fr/api/1/datasets/repertoire-national-des-associations"
).json()['resources']
resources = [r for r in resources if 'import' in r['title'].lower()]
url_rna = sorted(resources, key=lambda r: r['created_at'])[-1]['url']

default_args = {
    'email': [
        'pierlou.ramade@data.gouv.fr',
        'geoffrey.aldebert@data.gouv.fr'
    ],
    'email_on_failure': True
}

with DAG(
    dag_id=DAG_NAME,
    schedule_interval='15 7 1 * *',
    start_date=days_ago(1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=240),
    tags=["data_processing", "rna"],
    default_args=default_args,
) as dag:

    clean_previous_outputs = CleanFolderOperator(
        task_id="clean_previous_outputs",
        folder_path=TMP_FOLDER
    )

    download_rna_data = BashOperator(
        task_id='download_rna_data',
        bash_command=(
            f"sh {AIRFLOW_DAG_HOME}{DAG_FOLDER}"
            f"rna/scripts/script_dl_rna.sh {DATADIR} {url_rna} {SQLDIR}"
        )
    )

    process_rna = PythonOperator(
        task_id='process_rna',
        python_callable=process_rna,
    )

    create_rna_table = PythonOperator(
        task_id='create_rna_table',
        python_callable=create_rna_table,
    )

    populate_rna_table = PythonOperator(
        task_id='populate_rna_table',
        python_callable=populate_rna_table,
    )

    index_rna_table = PythonOperator(
        task_id='index_rna_table',
        python_callable=index_rna_table,
    )

    send_rna_to_minio = PythonOperator(
        task_id='send_rna_to_minio',
        python_callable=send_rna_to_minio,
    )

    publish_rna_communautaire = PythonOperator(
        task_id='publish_rna_communautaire',
        python_callable=publish_rna_communautaire,
    )

    download_rna_data.set_upstream(clean_previous_outputs)
    process_rna.set_upstream(download_rna_data)
    create_rna_table.set_upstream(process_rna)
    populate_rna_table.set_upstream(create_rna_table)
    index_rna_table.set_upstream(populate_rna_table)
    send_rna_to_minio.set_upstream(process_rna)
    publish_rna_communautaire.set_upstream(send_rna_to_minio)
