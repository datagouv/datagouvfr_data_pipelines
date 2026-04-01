from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from datagouvfr_data_pipelines.dgv.monitoring.dashboard.task_functions import (
    DAG_NAME,
    TMP_FOLDER,
    gather_and_upload,
    get_and_upload_certification,
    get_and_upload_reuses_down,
    get_catalog_stats,
    get_hvd_dataservices_stats,
    get_support_tickets,
    get_visits,
)
from datagouvfr_data_pipelines.utils.tchap import send_message
from datagouvfr_data_pipelines.utils.tasks import clean_up_folder

one_year_ago = datetime.today() - timedelta(days=365)
groups = [
    k + "@" + ".".join(["data", "gouv", "fr"])
    for k in ["support", "ouverture", "moissonnage", "certification"]
]
entreprises_api_url = "https://recherche-entreprises.api.gouv.fr/search?q="

default_args = {
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_NAME,
    schedule="0 4 1 * *",
    start_date=datetime(2023, 10, 15),
    dagrun_timeout=timedelta(minutes=120),
    tags=["dashboard", "support", "certification"],
    default_args=default_args,
    catchup=False,
):

    @task()
    def notification():
        return send_message(
            "📊 Données du dashboard de suivi des indicateurs mises à jour."
        )

    clean_up_recreate = clean_up_folder(TMP_FOLDER, recreate=True)
    _publish = notification()
    _gather_and_upload = gather_and_upload()

    (
        clean_up_recreate
        >> [
            get_and_upload_certification(),
            get_and_upload_reuses_down(),
            get_catalog_stats(),
            get_hvd_dataservices_stats(),
        ]
        >> _publish
    )

    (clean_up_recreate >> get_support_tickets(one_year_ago) >> _gather_and_upload,)
    (clean_up_recreate >> get_visits(one_year_ago) >> _gather_and_upload,)

    _gather_and_upload >> _publish
