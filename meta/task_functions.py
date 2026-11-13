import json
import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Literal, TypedDict

import numpy as np
import pytz
from airflow.sdk import task

# from airflow.providers.http.hooks.http import HttpHook
from airflow.utils.state import State
from airflow_client.client.api import dag_api, dag_run_api, task_instance_api
from airflow_client.client.exceptions import NotFoundException
from airflow_client.client.models.task_instance_response import TaskInstanceResponse
from datagouvfr_data_pipelines.config import AIRFLOW_DAG_HOME, AIRFLOW_ENV, AIRFLOW_URL
from datagouvfr_data_pipelines.utils.airflow import AirflowAPI
from datagouvfr_data_pipelines.utils.tchap import send_message

CONN_NAME = "HTTP_WORKFLOWS_INFRA_DATA_GOUV_FR"
local_timezone = pytz.timezone("Europe/Paris")

DEFAULT_DAG_OWNERS = [
    "estelle",
    "geoffrey",
    "hadrien",
    "sylvain",
]
with open(f"{AIRFLOW_DAG_HOME}datagouvfr_data_pipelines/meta/config.json", "r") as f:
    config = json.load(f)


def get_owners(config_value: bool | dict) -> list[str]:
    """Owners to ping for a DAG, depending on its value in config.json:
    - true: default owners
    - dict with any of these keys:
        - "override": these owners instead of the default ones
        - "include": owners added on top of the default ones
        - "exclude": owners removed from the default ones
    """
    if not isinstance(config_value, dict):
        return DEFAULT_DAG_OWNERS
    owners = config_value.get("override", DEFAULT_DAG_OWNERS)
    owners = owners + [o for o in config_value.get("include", []) if o not in owners]
    excluded = config_value.get("exclude", [])
    return [owner for owner in owners if owner not in excluded]


def get_ids(config: dict, api: dag_api.DAGApi) -> dict[str, str]:
    dags = AirflowAPI.paginate(api.get_dags, "dags")
    ids: dict[str, str] = {}
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


class SuccessfulRun(TypedDict):
    success: Literal[True]
    duration: float


class FailedRun(TypedDict):
    success: Literal[False]
    status: str
    failed_tasks: dict[str, str]


RunState = SuccessfulRun | FailedRun

# dag id -> (run id -> RunState): the `todays_runs` structure
TodaysRuns = dict[str, dict[str, RunState]]


def failed_runs_ratio(attempts: dict[str, RunState]) -> float:
    """Ratio of failed runs over all runs for a DAG, in [0, 1].
    Returns 0 when the DAG has no run at all.
    """
    total = len(attempts)
    if total == 0:
        return 0.0
    failures = sum(1 for atp in attempts.values() if not atp["success"])
    return failures / total


def sort_dags_by_failure_ratio(
    todays_runs: TodaysRuns,
) -> list[tuple[str, dict[str, RunState]]]:
    """Order DAGs for the daily summary.

    Alphabetical order, but DAGs with at least one failed run are listed last,
    sorted by their failure ratio (best on top, worst at the bottom, so that
    478 successes / 1 error appears above 0 success / 1 error).
    DAGs without any failed run have a ratio of 0.0 (< any failing DAG), so
    they come first and are ordered alphabetically among themselves.
    """
    return sorted(
        todays_runs.items(),
        key=lambda item: (failed_runs_ratio(item[1]), item[0]),
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
        todays_runs: TodaysRuns = defaultdict(dict)
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
                    duration = end_date - start_date  # type: ignore  # start_date should not be None
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
    todays_runs: TodaysRuns = context["ti"].xcom_pull(
        key="todays_runs", task_ids="monitor_dags"
    )
    dag_ids = context["ti"].xcom_pull(key="dag_ids", task_ids="monitor_dags")
    message = f"# Récap quotidien [DAGs]({AIRFLOW_URL}):\n"
    ping = set()
    logging.info(todays_runs)
    ordered_dags = sort_dags_by_failure_ratio(todays_runs)
    for dag, attempts in ordered_dags:
        message += f"\n- **{dag}** :\n"
        successes: dict[str, SuccessfulRun] = {
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

        failures: dict[str, FailedRun] = {
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
            if len(failures) > 10 or failed_runs_ratio(attempts) > 0.02:
                owners = get_owners(config[dag_ids[dag]])
                ping |= set(owners)
                message += " \n " + " ".join("@" + owner for owner in owners) + " \n "
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
