from airflow.models import DagRun
from airflow.utils.state import State
import json
from datetime import datetime
import os

from datagouvfr_data_pipelines.config import AIRFLOW_DAG_HOME
from datagouvfr_data_pipelines.utils.mattermost import send_message


def monitor_dags(
    ti,
    # date=datetime.today().strftime("%Y-%m-%d"),
    date='2023-08-15',
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

####################################################################################
# attempt to use log folders because integrated airflow tools
# have weird behaviours with dates and manuals runs

# def get_subfolders_last_modified(root_folder):
#     subfolder_dict = {}
#     for root, subfolders, files in os.walk(root_folder):
#         for subfolder in subfolders:
#             subfolder_path = os.path.join(root, subfolder)
#             last_modified = os.path.getmtime(subfolder_path)
#             subfolder_dict[subfolder] = last_modified
#     return subfolder_dict


# # issue: attempts are not consistents from one task to another
# # e.g: task1 can pass at 3rd try, then task2 1st try, then task3 not pass after 2 tries
# # how do we know which task was the last one to run if DAG crashes?
# def monitor_dags_from_folders(date=datetime.today().strftime("%Y-%m-%d")):
#     with open(f"{AIRFLOW_DAG_HOME}datagouvfr_data_pipelines/meta/config/config.json", 'r') as f:
#         config = json.load(f)
#     dag_ids_to_monitor = config['dag_list']

#     todays_runs = {}
#     for dag_id in dag_ids_to_monitor:
#         if f'dag_id={dag_id}' in os.listdir(AIRFLOW_DAG_LOGS):
#             this_dag_today = [
#                 run for run in os.listdir(AIRFLOW_DAG_LOGS + f'/dag_id={dag_id}')
#                 if date in run
#             ]

#             if len(this_dag_today) > 0:
#                 todays_runs[dag_id] = {}

#             for run_folder in this_dag_today:
#                 path = AIRFLOW_DAG_LOGS + f'/dag_id={dag_id}/' + run_folder
#                 run_id = run_folder.replace('run_id=', '')

#                 task_folders = os.listdir(path)
#                 nb_attempts = max([
#                     len(os.listdir(path + '/' + t)) for t in task_folders
#                 ])
#                 # for task in task_folders:



#                 todays_runs[dag_id][run_id] = {
#                     'type': run_id.split('__')[0],
#                     'nb_attempts': nb_attempts,
#                 }
#     print(todays_runs)
