from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datagouvfr_data_pipelines.utils.mattermost import send_message
from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
)
from datagouvfr_data_pipelines.dgv.monitoring.dashboard.task_functions import (
    gather_and_upload,
    get_and_upload_certification,
    get_and_upload_reuses_down,
    get_catalog_stats,
    get_hvd_dataservices_stats,
    get_visits,
    # get_zammad_tickets,
)
from datagouvfr_data_pipelines.utils.minio import MinIOClient

DAG_NAME = "dgv_dashboard"
DATADIR = f"{AIRFLOW_DAG_TMP}{DAG_NAME}/data/"
one_year_ago = datetime.today() - timedelta(days=365)
groups = [
    k + "@" + ".".join(["data", "gouv", "fr"])
    for k in ["support", "ouverture", "moissonnage", "certification"]
]
entreprises_api_url = "https://recherche-entreprises.api.gouv.fr/search?q="

minio_open = MinIOClient(bucket="dataeng-open")
minio_destination_folder = "dashboard/"

default_args = {
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_NAME,
    schedule_interval="0 4 1 * *",
    start_date=datetime(2023, 10, 15),
    dagrun_timeout=timedelta(minutes=120),
    tags=["dashboard", "support", "certification"],
    default_args=default_args,
    catchup=False,
) as dag:
    clean_previous_outputs = BashOperator(
        task_id="clean_previous_outputs",
        bash_command=f"rm -rf {DATADIR} && mkdir -p {DATADIR}",
    )

    # get_zammad_tickets = PythonOperator(
    #     task_id="get_zammad_tickets",
    #     python_callable=get_zammad_tickets,
    #     op_kwargs={"start_date": one_year_ago},
    # )

    get_visits = PythonOperator(
        task_id="get_visits",
        python_callable=get_visits,
        op_kwargs={"start_date": one_year_ago},
    )

    get_catalog_stats = PythonOperator(
        task_id="get_catalog_stats",
        python_callable=get_catalog_stats,
    )

    get_hvd_dataservices_stats = PythonOperator(
        task_id="get_hvd_dataservices_stats",
        python_callable=get_hvd_dataservices_stats,
    )

    get_and_upload_certification = PythonOperator(
        task_id="get_and_upload_certification",
        python_callable=get_and_upload_certification,
    )

    get_and_upload_reuses_down = PythonOperator(
        task_id="get_and_upload_reuses_down",
        python_callable=get_and_upload_reuses_down,
    )

    gather_and_upload = PythonOperator(
        task_id="gather_and_upload",
        python_callable=gather_and_upload,
    )

    publish_mattermost = PythonOperator(
        task_id="publish_mattermost",
        python_callable=send_message,
        op_kwargs={
            "text": ":bar_chart: Données du dashboard de suivi des indicateurs mises à jour."
        },
    )

    # get_zammad_tickets.set_upstream(clean_previous_outputs)
    get_visits.set_upstream(clean_previous_outputs)
    get_and_upload_certification.set_upstream(clean_previous_outputs)
    get_and_upload_reuses_down.set_upstream(clean_previous_outputs)
    get_catalog_stats.set_upstream(clean_previous_outputs)
    get_hvd_dataservices_stats.set_upstream(clean_previous_outputs)

    # gather_and_upload.set_upstream(get_zammad_tickets)
    gather_and_upload.set_upstream(get_visits)

    publish_mattermost.set_upstream(gather_and_upload)
    publish_mattermost.set_upstream(get_and_upload_certification)
    publish_mattermost.set_upstream(get_and_upload_reuses_down)
    publish_mattermost.set_upstream(get_catalog_stats)
    publish_mattermost.set_upstream(get_hvd_dataservices_stats)
