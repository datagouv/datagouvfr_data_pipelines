from collections import defaultdict
from curses import raw
import json
from datetime import datetime, timedelta
import numpy as np
import pytz
from airflow.models import DagRun
from airflow.utils.state import State
from airflow.providers.http.hooks.http import HttpHook

from datagouvfr_data_pipelines.config import AIRFLOW_DAG_HOME, AIRFLOW_ENV, AIRFLOW_URL
from datagouvfr_data_pipelines.utils.mattermost import send_message

local_timezone = pytz.timezone("Europe/Paris")
http_hook = HttpHook(http_conn_id="HTTP_WORKFLOWS_INFRA_DATA_GOUV_FR", method="GET")

DEFAULT_DAG_OWNERS = [
    "geoffrey.aldebert",
    "pierlou_ramade",
    "hadrien_bossard",
]
with open(
    f"{AIRFLOW_DAG_HOME}datagouvfr_data_pipelines/meta/config/config.json", "r"
) as f:
    config = json.load(f)


def get_ids(config: dict) -> dict[str, str]:
    dags = http_hook.run("api/v1/dags").json()["dags"]
    ids = {}
    for raw_id, included in config.items():
        if not included:
            continue
        if raw_id.endswith("*"):
            ids |= {
                d["dag_id"] : raw_id
                for d in dags if d["dag_id"].startswith(raw_id[:-1])
            }
        else:
            ids[raw_id] = raw_id
    return ids


def monitor_dags(
    ti,
    date=(datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S"),
):
    dag_ids_to_monitor = get_ids(config)
    print("DAG list:", dag_ids_to_monitor)

    print("Start date considered:", date)
    todays_runs = defaultdict(dict)
    for dag_id in dag_ids_to_monitor:
        dag_runs = DagRun.find(
            dag_id=dag_id,
            # /!\ this filters based on execution date and not start date, so we filter manually afterwards
            # execution_start_date=start_date
        )

        for dag_run in dag_runs:
            status = dag_run.get_state()
            end_date = (
                dag_run.end_date.strftime("%Y-%m-%d %H:%M:%S")
                if dag_run.end_date
                else None
            )
            if end_date and end_date >= date:
                if status != State.SUCCESS:
                    failed_task_instances = dag_run.get_task_instances(
                        state=State.FAILED
                    )
                    todays_runs[dag_id][end_date] = {
                        "success": False,
                        "status": status,
                        "failed_tasks": {
                            task.task_id: task.log_url for task in failed_task_instances
                        },
                    }
                else:
                    duration = dag_run.end_date - dag_run.start_date
                    todays_runs[dag_id][end_date] = {
                        "success": True,
                        "duration": duration.total_seconds(),
                    }
    ti.xcom_push(key="todays_runs", value=todays_runs)
    # sending the mapping between dag ids and prefixes in the config
    ti.xcom_push(key="dag_ids", value=dag_ids_to_monitor)
    return todays_runs


def notification_mattermost(ti):
    todays_runs = ti.xcom_pull(key="todays_runs", task_ids="monitor_dags")
    dag_ids = ti.xcom_pull(key="dag_ids", task_ids="monitor_dags")
    message = "# Récap quotidien DAGs :"
    print(todays_runs)
    for dag, attempts in dict(sorted(todays_runs.items())).items():
        message += f"\n- **{dag}** :"
        successes = {
            atp_id: attempts[atp_id]
            for atp_id in attempts
            if attempts[atp_id]["success"]
        }
        if successes:
            average_duration = np.mean([i["duration"] for i in successes.values()])
            hours = int(average_duration // 3600)
            minutes = int((average_duration % 3600) // 60)
            start_time = datetime.fromisoformat(sorted(successes.keys())[-1]).replace(
                tzinfo=pytz.UTC
            )
            # setting time to UTC+2
            start_time = start_time.astimezone(local_timezone)
            message += (
                f"\n - ✅ {len(successes)} run{'s' if len(successes) > 1 else ''} OK"
            )
            message += (
                f" (en {f'{hours}h{minutes}min' if hours > 0 else f'{minutes}min'}"
            )
            message += f"{' en moyenne' if len(successes) > 1 else ''})."
            message += f" Dernier passage terminé à {start_time.strftime('%H:%M')}."

        failures = {
            atp_id: attempts[atp_id]
            for atp_id in attempts
            if not attempts[atp_id]["success"]
        }
        if failures:
            last_failure = failures[sorted(failures.keys())[-1]]
            start_time = datetime.fromisoformat(sorted(failures.keys())[-1]).replace(
                tzinfo=pytz.UTC
            )
            # setting time to UTC+2
            start_time = start_time.astimezone(local_timezone)
            message += (
                f"\n - ❌ {len(failures)} run{'s' if len(failures) > 1 else ''} KO."
            )
            message += (
                f" La dernière tentative a échoué à {start_time.strftime('%H:%M')} "
            )
            message += f"(status : {last_failure['status']}), "
            if not last_failure["failed_tasks"]:
                message += "timeout :hourglass:"
            else:
                message += "tâches en échec :"
                for ft in last_failure["failed_tasks"]:
                    url_log = last_failure["failed_tasks"][ft]
                    if AIRFLOW_ENV == "prod":
                        url_log = url_log.replace("http://localhost:8080", AIRFLOW_URL)
                    message += f"\n   - {ft} ([voir log]({url_log}))"
            # ping only if more than 10 failures or more than 2% failures
            if (
                len(failures) > 10
                or len(failures) / (len(failures) + len(successes)) > 0.02
            ):
                message += "\n" + (
                    " ".join(
                        [
                            "@" + owner
                            for owner in (
                                config[dag_ids[dag]]
                                if isinstance(config[dag_ids[dag]], list)
                                else DEFAULT_DAG_OWNERS
                            )
                        ]
                    )
                )
    send_message(message)
