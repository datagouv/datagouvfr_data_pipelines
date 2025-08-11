from collections import defaultdict
from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datagouvfr_data_pipelines.verticales.culture.task_functions import (
    DATADIR,
    gather_stats,
    get_and_send_perimeter_objects,
    get_perimeter_orgas,
    get_perimeter_stats,
    refresh_datasets_tops,
    send_stats_to_minio,
    send_notification_mattermost,
)

DAG_NAME = "verticale_culture"

with DAG(
    dag_id=DAG_NAME,
    # every monday morning
    schedule_interval="0 1 * * 1",
    start_date=datetime(2025, 8, 1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=240),
    tags=["verticale", "culture"],
) as dag:
    clean_previous_outputs = BashOperator(
        task_id="clean_previous_outputs",
        bash_command=f"rm -rf {DATADIR} && mkdir -p {DATADIR}",
    )

    get_perimeter_orgas = PythonOperator(
        task_id="get_perimeter_orgas",
        python_callable=get_perimeter_orgas,
    )

    refresh_datasets_tops = PythonOperator(
        task_id="refresh_datasets_tops",
        python_callable=refresh_datasets_tops,
    )

    get_perimeter_stats_organizations = PythonOperator(
        task_id="get_perimeter_stats_organizations",
        python_callable=get_perimeter_stats,
        op_kwargs={"object_type": "organizations"},
    )

    object_types = [
        "datasets",
        "dataservices",
        "reuses",
    ]
    tasks = defaultdict(list)
    for obj in object_types:
        tasks[obj].append(
            PythonOperator(
                task_id=f"get_and_send_perimeter_{obj}",
                python_callable=get_and_send_perimeter_objects,
                op_kwargs={"object_type": obj},
            )
        )
        tasks[obj].append(
            PythonOperator(
                task_id=f"get_perimeter_stats_{obj}",
                python_callable=get_perimeter_stats,
                op_kwargs={"object_type": obj},
            )
        )

    gather_stats = PythonOperator(
        task_id="gather_stats",
        python_callable=gather_stats,
        op_kwargs={"object_types": object_types + ["organizations"]},
    )

    send_stats_to_minio = PythonOperator(
        task_id="send_stats_to_minio",
        python_callable=send_stats_to_minio,
    )

    send_notification_mattermost = PythonOperator(
        task_id="send_notification_mattermost",
        python_callable=send_notification_mattermost,
    )

    get_perimeter_orgas.set_upstream(clean_previous_outputs)
    refresh_datasets_tops.set_upstream(get_perimeter_orgas)
    get_perimeter_stats_organizations.set_upstream(get_perimeter_orgas)
    for obj in object_types:
        tasks[obj][0].set_upstream(get_perimeter_orgas)
        tasks[obj][1].set_upstream(tasks[obj][0])
        gather_stats.set_upstream(tasks[obj][1])
    gather_stats.set_upstream(get_perimeter_stats_organizations)
    send_stats_to_minio.set_upstream(gather_stats)
    send_notification_mattermost.set_upstream(send_stats_to_minio)
