from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.python import ShortCircuitOperator
from datagouvfr_data_pipelines.dgv.monitoring.tops.task_functions import (
    get_top,
    publish_top_mattermost,
    send_stats_to_s3,
    send_tops_to_s3,
)
from datagouvfr_data_pipelines.utils.utils import (
    check_if_first_day_of_month,
    check_if_first_day_of_year,
    check_if_monday,
)
from dateutil.relativedelta import relativedelta

DAG_NAME = "dgv_tops"
S3_PATH = "tops/"
# we base ourselves on completed days
yesterday = (datetime.today() + relativedelta(days=-1)).strftime("%Y-%m-%d")

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id=DAG_NAME,
    schedule="15 6 * * *",
    start_date=datetime(2024, 8, 10),
    dagrun_timeout=timedelta(minutes=60),
    tags=["tops", "datagouv", "piwik"],
    default_args=default_args,
    catchup=False,
):
    short_circuits = {
        "week": ShortCircuitOperator(
            task_id="check_if_monday",
            python_callable=check_if_monday,
        ),
        "month": ShortCircuitOperator(
            task_id="check_if_first_day_of_month",
            python_callable=check_if_first_day_of_month,
        ),
        "year": ShortCircuitOperator(
            task_id="check_if_first_day_of_year",
            python_callable=check_if_first_day_of_year,
        ),
    }

    freqs = {
        "day": "Journée d'hier",
        "week": "Semaine dernière",
        "month": "Mois dernier",
        "year": "Année dernière",
    }

    classes = {
        "datasets": "jeux de données",
        "reuses": "réutilisations",
    }

    tasks = {}
    for freq, freq_label in freqs.items():
        prefix = "dai" if freq == "day" else freq
        tasks[freq] = []
        if freq in short_circuits:
            tasks[freq].append(short_circuits[freq])
        tasks[freq] += [
            [
                get_top.override(task_id=f"get_top_{_class}_{freq}")(
                    _type=_class,
                    date=yesterday,
                    period=freq,
                    title=f"Top 10 des {class_label}",
                )
                for _class, class_label in classes.items()
            ],
            publish_top_mattermost.override(task_id=f"publish_top_{freq}_mattermost")(
                period=freq,
                label=freq_label,
            ),
            [
                send_tops_to_s3.override(task_id=f"send_top_{freq}_to_s3")(
                    period=freq,
                    s3=f"{S3_PATH}piwik_tops_{prefix}ly/{yesterday}/",
                ),
                send_stats_to_s3.override(task_id=f"send_stats_{freq}_to_s3")(
                    period=freq,
                    s3=f"{S3_PATH}piwik_stats_{prefix}ly/{yesterday}/",
                    date=yesterday,
                ),
            ],
        ]
        chain(*tasks[freq])
