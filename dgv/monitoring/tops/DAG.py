from datetime import timedelta, datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator

from datagouvfr_data_pipelines.config import (
    AIRFLOW_ENV,
)
from datagouvfr_data_pipelines.dgv.monitoring.tops.task_functions import (
    get_top,
    send_tops_to_minio,
    send_stats_to_minio,
    publish_top_mattermost,
)
from datagouvfr_data_pipelines.utils.utils import (
    check_if_first_day_of_month,
    check_if_monday,
)

DAG_NAME = "dgv_tops"
MINIO_PATH = "dgv/"


default_args = {
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id=DAG_NAME,
    schedule_interval="15 6 * * *",
    start_date=datetime(2024, 8, 10),
    dagrun_timeout=timedelta(minutes=60),
    tags=["tops", "datagouv", "piwik"],
    default_args=default_args,
    catchup=False,
) as dag:
    get_top_datasets_day = PythonOperator(
        task_id="get_top_datasets_day",
        python_callable=get_top,
        templates_dict={
            "type": "datasets",
            "date": "{{ ds }}",
            "period": "day",
            "title": "Top 10 des jeux de données",
        },
    )

    get_top_reuses_day = PythonOperator(
        task_id="get_top_reuses_day",
        python_callable=get_top,
        templates_dict={
            "type": "reuses",
            "date": "{{ ds }}",
            "period": "day",
            "title": "Top 10 des réutilisations",
        },
    )

    publish_top_day_mattermost = PythonOperator(
        task_id="publish_top_day_mattermost",
        python_callable=publish_top_mattermost,
        templates_dict={"period": "day", "periode": "Journée d'hier"},
    )

    send_top_day_to_minio = PythonOperator(
        task_id="send_top_day_to_minio",
        python_callable=send_tops_to_minio,
        templates_dict={
            "period": "day",
            "minio": f"{AIRFLOW_ENV}/{MINIO_PATH}piwik_tops_daily/{{ ds }}/",
        },
    )

    send_stats_day_to_minio = PythonOperator(
        task_id="send_stats_day_to_minio",
        python_callable=send_stats_to_minio,
        templates_dict={
            "period": "day",
            "minio": f"{AIRFLOW_ENV}/{MINIO_PATH}piwik_stats_daily/{{ ds }}/",
            "date": "{{ ds }}",
            "title_end": "sur les trois derniers mois",
        },
    )

    check_if_monday = ShortCircuitOperator(
        task_id="check_if_monday", python_callable=check_if_monday
    )

    get_top_datasets_week = PythonOperator(
        task_id="get_top_datasets_week",
        python_callable=get_top,
        templates_dict={
            "type": "datasets",
            "period": "week",
            "date": "{{ ds }}",
            "title": "Top 10 des jeux de données",
        },
    )

    get_top_reuses_week = PythonOperator(
        task_id="get_top_reuses_week",
        python_callable=get_top,
        templates_dict={
            "type": "reuses",
            "period": "week",
            "date": "{{ ds }}",
            "title": "Top 10 des réutilisations",
        },
    )

    publish_top_week_mattermost = PythonOperator(
        task_id="publish_top_week_mattermost",
        python_callable=publish_top_mattermost,
        templates_dict={"period": "week", "periode": "Semaine dernière"},
    )

    send_top_week_to_minio = PythonOperator(
        task_id="send_top_week_to_minio",
        python_callable=send_tops_to_minio,
        templates_dict={
            "period": "week",
            "minio": f"{AIRFLOW_ENV}/{MINIO_PATH}piwik_tops_weekly/{{ ds }}/",
        },
    )

    send_stats_week_to_minio = PythonOperator(
        task_id="send_stats_week_to_minio",
        python_callable=send_stats_to_minio,
        templates_dict={
            "period": "week",
            "minio": f"{AIRFLOW_ENV}/{MINIO_PATH}piwik_stats_weekly/{{ ds }}/",
            "date": "{{ ds }}",
            "title_end": "sur les six derniers mois",
        },
    )

    check_if_first_day_of_month = ShortCircuitOperator(
        task_id="check_if_first_day_of_month",
        python_callable=check_if_first_day_of_month,
    )

    get_top_datasets_month = PythonOperator(
        task_id="get_top_datasets_month",
        python_callable=get_top,
        templates_dict={
            "type": "datasets",
            "period": "month",
            "date": "{{ ds }}",
            "title": "Top 10 des jeux de données",
        },
    )

    get_top_reuses_month = PythonOperator(
        task_id="get_top_reuses_month",
        python_callable=get_top,
        templates_dict={
            "type": "reuses",
            "period": "month",
            "date": "{{ ds }}",
            "title": "Top 10 des réutilisations",
        },
    )

    publish_top_month_mattermost = PythonOperator(
        task_id="publish_top_month_mattermost",
        python_callable=publish_top_mattermost,
        templates_dict={"period": "month", "periode": "Mois dernier"},
    )
    send_top_month_to_minio = PythonOperator(
        task_id="send_top_month_to_minio",
        python_callable=send_tops_to_minio,
        templates_dict={
            "period": "month",
            "minio": f"{AIRFLOW_ENV}/{MINIO_PATH}piwik_tops_monthly/{{ ds }}/",
        },
    )

    send_stats_month_to_minio = PythonOperator(
        task_id="send_stats_month_to_minio",
        python_callable=send_stats_to_minio,
        templates_dict={
            "period": "month",
            "minio": f"{AIRFLOW_ENV}/{MINIO_PATH}piwik_stats_monthly/{{ ds }}/",
            "date": "{{ ds }}",
            "title_end": "sur les deux dernières années",
        },
    )

    publish_top_day_mattermost.set_upstream(get_top_datasets_day)
    publish_top_day_mattermost.set_upstream(get_top_reuses_day)

    send_top_day_to_minio.set_upstream(publish_top_day_mattermost)
    send_stats_day_to_minio.set_upstream(publish_top_day_mattermost)

    check_if_monday.set_upstream(send_top_day_to_minio)
    get_top_datasets_week.set_upstream(check_if_monday)
    get_top_reuses_week.set_upstream(check_if_monday)

    publish_top_week_mattermost.set_upstream(get_top_datasets_week)
    publish_top_week_mattermost.set_upstream(get_top_reuses_week)
    send_top_week_to_minio.set_upstream(publish_top_week_mattermost)
    send_stats_week_to_minio.set_upstream(publish_top_week_mattermost)

    check_if_first_day_of_month.set_upstream(send_top_day_to_minio)
    get_top_datasets_month.set_upstream(check_if_first_day_of_month)
    get_top_reuses_month.set_upstream(check_if_first_day_of_month)

    publish_top_month_mattermost.set_upstream(get_top_datasets_month)
    publish_top_month_mattermost.set_upstream(get_top_reuses_month)
    send_top_month_to_minio.set_upstream(publish_top_month_mattermost)
    send_stats_month_to_minio.set_upstream(publish_top_month_mattermost)
