from airflow.models import DagRun
from airflow.utils.state import State
import json
from datetime import datetime, timedelta

from datagouvfr_data_pipelines.config import AIRFLOW_DAG_HOME
from datagouvfr_data_pipelines.utils.mattermost import send_message


def monitor_dags(
    ti,
    date=(datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S"),
):
    with open(f"{AIRFLOW_DAG_HOME}datagouvfr_data_pipelines/meta/config/config.json", 'r') as f:
        config = json.load(f)
    dag_ids_to_monitor = config['dag_list']
    print("DAG list:", dag_ids_to_monitor)

    print("Start date considered:", date)
    todays_runs = {}
    for dag_id in dag_ids_to_monitor:
        dag_runs = DagRun.find(
            dag_id=dag_id,
            # /!\ this filters based on execution date and not start date, so we filter manually afterwards
            # execution_start_date=start_date
        )

        for dag_run in dag_runs:
            status = dag_run.get_state()
            start_date = dag_run.start_date.strftime("%Y-%m-%d %H:%M:%S")
            if start_date >= date:
                if dag_id not in todays_runs.keys():
                    todays_runs[dag_id] = {}

                if status != State.SUCCESS:
                    failed_task_instances = dag_run.get_task_instances(state=State.FAILED)
                    todays_runs[dag_id][start_date] = {
                        'success': False,
                        'status': status,
                        'failed_tasks': {
                            task.task_id: task.log_url for task in failed_task_instances
                        }
                    }
                else:
                    duration = dag_run.end_date - dag_run.start_date
                    todays_runs[dag_id][start_date] = {
                        'success': True,
                        'duration': duration.total_seconds()
                    }
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
                hours = int(todays_runs[dag][attempt]['duration'] // 3600)
                minutes = int((todays_runs[dag][attempt]['duration'] % 3600) // 60)
                if hours > 0:
                    message += f"\n - ✅ Run de {run_time} OK ! (en {hours}h{minutes}min)"
                else:
                    message += f"\n - ✅ Run de {run_time} OK ! (en {minutes}min)"
            else:
                message += f"\n - ❌ Run de {run_time} a échoué (status : {todays_runs[dag][attempt]['status']}). Les tâches en échec sont :"
                for ft in todays_runs[dag][attempt]['failed_tasks']:
                    message += f"\n   - {ft} ([voir log]({todays_runs[dag][attempt]['failed_tasks'][ft]}))"
    send_message(message)
