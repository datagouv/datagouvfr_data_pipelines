import json
import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import numpy as np
import pytz
from airflow.decorators import task
from airflow_client.client.models.task_instance_response import TaskInstanceResponse
from airflow_client.client.api import dag_api, dag_run_api, task_instance_api
from airflow_client.client.exceptions import NotFoundException

# from airflow.providers.http.hooks.http import HttpHook
from airflow.utils.state import State
from datagouvfr_data_pipelines.config import AIRFLOW_DAG_HOME, AIRFLOW_ENV, AIRFLOW_URL
from datagouvfr_data_pipelines.utils.tchap import send_message
from datagouvfr_data_pipelines.utils.airflow import AirflowAPI

CONN_NAME = "HTTP_WORKFLOWS_INFRA_DATA_GOUV_FR"
local_timezone = pytz.timezone("Europe/Paris")

DEFAULT_DAG_OWNERS = [
    "geoffrey",
    "pierlou",
    "hadrien",
]
with open(f"{AIRFLOW_DAG_HOME}datagouvfr_data_pipelines/meta/config.json", "r") as f:
    config = json.load(f)


def get_ids(config: dict, api: dag_api.DAGApi) -> dict[str, str]:
    dags = AirflowAPI.paginate(api.get_dags, "dags")
    ids = {}
    for raw_id, included in config.items():
        if not included:
            continue
        if raw_id.endswith("*"):
            ids |= {
                dag.dag_id: raw_id for dag in dags if dag.dag_id.startswith(raw_id[:-1])
            }
        else:
            ids[raw_id] = raw_id
    return ids


def generate_task_log_url(task: TaskInstanceResponse):
    return (
        f"{AIRFLOW_URL}/dags/{task.dag_id}/runs/{task.dag_run_id}/tasks/{task.task_id}"
    )


@task()
def monitor_dags(
    date=(datetime.now() - timedelta(days=1)).replace(microsecond=0),
    **context,
):
    dags_not_found = []
    with AirflowAPI(conn_name=CONN_NAME).client as client:
        dag_api_client = dag_api.DAGApi(client)
        dag_ids_to_monitor = get_ids(config, dag_api_client)
        logging.info("DAG list:", dag_ids_to_monitor)
        logging.info(f"Start date considered: {date.strftime('%Y-%m-%d %H:%M:%S')}")
        todays_runs = defaultdict(dict)
        for dag_id in dag_ids_to_monitor:
            dag_run_api_client = dag_run_api.DagRunApi(client)
            try:
                date_dt = date.replace(
                    tzinfo=timezone.utc
                )  # The server needs a timezone-aware datetime or it throws a 500 error
                dag_runs = AirflowAPI.paginate(
                    dag_run_api_client.get_dag_runs,
                    "dag_runs",
                    dag_id=dag_id,
                    end_date_gte=date_dt,
                    state=["success", "failed"],  # exclude queued/running (no end_date)
                )
            except NotFoundException:
                dags_not_found.append(dag_id)
                continue  # Raise an error only at the end to avoid skipping the rest of DAGs
            for dag_run in dag_runs:
                start_date, end_date = dag_run.start_date, dag_run.end_date
                status = dag_run.state
                if status != State.SUCCESS:
                    task_instance_api_client = task_instance_api.TaskInstanceApi(client)
                    failed_task_instances = AirflowAPI.paginate(
                        task_instance_api_client.get_task_instances,
                        "task_instances",
                        dag_id=dag_run.dag_id,
                        dag_run_id=dag_run.dag_run_id,
                        state=[State.FAILED],
                    )
                    todays_runs[dag_id][end_date] = {
                        "success": False,
                        "status": status,
                        "failed_tasks": {
                            task.task_id: generate_task_log_url(task)
                            for task in failed_task_instances
                        },
                    }
                else:
                    duration = end_date - start_date  # type: ignore : start_date should not be None
                    todays_runs[dag_id][end_date] = {
                        "success": True,
                        "duration": duration.total_seconds(),
                    }
        context["ti"].xcom_push(key="dags_not_found", value=dags_not_found)
        context["ti"].xcom_push(key="todays_runs", value=todays_runs)
        # sending the mapping between dag ids and prefixes in the config
        context["ti"].xcom_push(key="dag_ids", value=dag_ids_to_monitor)
        return todays_runs


@task()
def notification(**context):
    todays_runs = context["ti"].xcom_pull(key="todays_runs", task_ids="monitor_dags")
    dag_ids = context["ti"].xcom_pull(key="dag_ids", task_ids="monitor_dags")
    message = f"# Récap quotidien [DAGs]({AIRFLOW_URL}):\n"
    ping = set()
    logging.info(todays_runs)
    for dag, attempts in dict(sorted(todays_runs.items())).items():
        message += f"\n- **{dag}** :\n"
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
                f"\n    - ✅ {len(successes)} run{'s' if len(successes) > 1 else ''} OK"
                f" (en {f'{hours}h{minutes}min' if hours > 0 else f'{minutes}min'}"
            )
            message += f"{' en moyenne' if len(successes) > 1 else ''})."
            message += f" Dernier passage terminé à {start_time.strftime('%H:%M')}.\n"

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
                f"\n    - ❌ {len(failures)} run{'s' if len(failures) > 1 else ''} KO."
                f" La dernière tentative a échoué à {start_time.strftime('%H:%M')} "
            )
            message += f"(status : {last_failure['status']}), "
            if not last_failure["failed_tasks"]:
                message += "timeout ⌛️"
            else:
                message += "tâches en échec :"
                for ft in last_failure["failed_tasks"]:
                    url_log = last_failure["failed_tasks"][ft]
                    if AIRFLOW_ENV == "prod":
                        url_log = url_log.replace("http://localhost:8080", AIRFLOW_URL)
                    message += f"\n        - {ft} ([voir log]({url_log}))"
            # ping only if more than 10 failures or more than 2% failures
            if (
                len(failures) > 10
                or len(failures) / (len(failures) + len(successes)) > 0.02
            ):
                ping |= set(
                    config[dag_ids[dag]]
                    if isinstance(config[dag_ids[dag]], list)
                    else DEFAULT_DAG_OWNERS
                )
                message += " \n " + (
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
                    + " \n "
                )
    send_message(message, ping=list(ping))
    # Notify in case of DAGs on the config
    dags_not_found = context["ti"].xcom_pull(
        key="dags_not_found", task_ids="monitor_dags"
    )
    if len(dags_not_found) > 0:
        message = "**❓️ Les DAGs suivants n'ont pas pu être trouvés:** " + "\n\n- "
        missing_dags_list = ", \n\n - ".join(dags_not_found)
        message += missing_dags_list
        message += (
            "\n\nSolutions:\n\n"
            f"1. Vérifiez de possibles erreurs d'import sur l'instance {AIRFLOW_URL}.\n\n"
            "2. Si ces DAGs sont obsolètes ou à renommer, consultez le fichier `/meta/config.json`"
        )
        send_message(message, ping=list(ping))
