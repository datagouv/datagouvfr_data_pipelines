from datetime import datetime, timedelta

from airflow import DAG

from datagouvfr_data_pipelines.utils.tasks import clean_up_folder
from datagouvfr_data_pipelines.verticales.culture.task_functions import (
    TMP_FOLDER,
    gather_stats,
    get_and_send_perimeter_objects,
    get_perimeter_orgas,
    get_perimeter_stats,
    refresh_datasets_tops,
    send_stats_to_s3,
    send_notification_mattermost,
)

with DAG(
    dag_id="verticale_culture",
    # every monday morning
    schedule="0 1 * * *",
    start_date=datetime(2025, 8, 1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=240),
    tags=["verticale", "culture"],
):
    object_types = [
        "datasets",
        "dataservices",
        "reuses",
    ]

    _get_perimeter_orgas = get_perimeter_orgas()
    _gather_stats = gather_stats(object_types + ["organizations"])

    clean_up_folder(TMP_FOLDER, recreate=True) >> _get_perimeter_orgas

    for obj in object_types:
        (
            _get_perimeter_orgas
            >> get_and_send_perimeter_objects.override(
                task_id=f"get_and_send_perimeter_{obj}"
            )(
                object_type=obj,
            )
            >> get_perimeter_stats.override(task_id=f"get_perimeter_stats_{obj}")(
                object_type=obj,
            )
            >> _gather_stats
        )

    (_gather_stats >> send_stats_to_s3() >> send_notification_mattermost())

    _get_perimeter_orgas >> refresh_datasets_tops()
