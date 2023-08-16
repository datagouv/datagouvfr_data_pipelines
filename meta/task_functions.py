from airflow.models import DagRun
from airflow.utils.state import State
import json
from datetime import datetime
import os

from datagouvfr_data_pipelines.config import AIRFLOW_DAG_HOME
from datagouvfr_data_pipelines.utils.mattermost import send_message


def monitor_dags(
    ti,
    date=datetime.today().strftime("%Y-%m-%d"),
):
    with open(f"{AIRFLOW_DAG_HOME}datagouvfr_data_pipelines/meta/config/config.json", 'r') as f:
        config = json.load(f)
    dag_ids_to_monitor = config['dag_list']

    todays_runs = {}
    for dag_id in dag_ids_to_monitor:
        dag_runs = DagRun.find(dag_id=dag_id)

        for dag_run in dag_runs:
            status = dag_run.get_state()
            execution_date = dag_run.execution_date.strftime("%Y-%m-%d %H:%M:%S")
            if execution_date >= date:
                if dag_id not in todays_runs.keys():
                    todays_runs[dag_id] = {}

                if status != State.SUCCESS:
                    failed_task_instances = dag_run.get_task_instances(state=State.FAILED)
                    todays_runs[dag_id][execution_date] = {
                        'success': False,
                        'status': status,
                        'failed_tasks': {
                            task.task_id: task.log_url for task in failed_task_instances
                        }
                    }
                else:
                    todays_runs[dag_id][execution_date] = {'success': True}
    ti.xcom_push(key="todays_runs", value=todays_runs)
    return todays_runs


def notification_mattermost(ti):
    todays_runs = ti.xcom_pull(key="todays_runs", task_ids="monitor_dags")
    message = '# Récap quotidien DAGs :'
    for dag in todays_runs:
        message += f'\n- **{dag}** :'
        for attempt in todays_runs[dag]:
            run_time = attempt.split(' ')[1][:-3]
            if todays_runs[dag][attempt]['success']:
                message += f"\n - ✅ Run de {run_time} OK !"
            else:
                message += f"\n - ❌ Run de {run_time} a échoué (status : {todays_runs[dag][attempt]['status']}). Les tâches en échec sont :"
                for ft in todays_runs[dag][attempt]['failed_tasks']:
                    message += f"\n   - {ft} ([voir log]({todays_runs[dag][attempt]['failed_tasks'][ft]}))"
    send_message(message)
