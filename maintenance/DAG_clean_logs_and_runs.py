from datetime import datetime, timedelta, timezone
import os
import logging
import shutil

from airflow.decorators import task
from airflow import DAG
from airflow.models import DagRun
from airflow.settings import Session

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_HOME,
    AIRFLOW_ENV,
)
from datagouvfr_data_pipelines.utils.mattermost import send_message

nb_days_to_keep = 60


def get_directory_size(directory):
    total_size = 0
    with os.scandir(directory) as it:
        for entry in it:
            if entry.is_file():
                total_size += entry.stat().st_size
            elif entry.is_dir():
                total_size += get_directory_size(entry.path)
    return total_size


# Define the Python function to delete old logs and directories
@task()
def delete_old_logs_and_directories(**context):
    total_size_bytes = 0
    log_dir = (
        "/opt/airflow/logs"
        if AIRFLOW_ENV == "dev"
        else f"{'/'.join(AIRFLOW_DAG_HOME.split('/')[:-2])}/logs"
    )
    cutoff_date = datetime.now() - timedelta(days=nb_days_to_keep)

    # Check if the directory exists
    if os.path.exists(log_dir):
        for root, dirs, files in os.walk(log_dir, topdown=False):
            for dir in dirs:
                dir_path = os.path.join(root, dir)
                # Check if the directory modification time is older than the cutoff date
                if os.path.getmtime(dir_path) < cutoff_date.timestamp():
                    # Recursively remove directory and its contents
                    total_size_bytes += get_directory_size(dir_path)
                    shutil.rmtree(dir_path)
                    logging.info(f"Deleted directory: {dir_path}")
            for file in files:
                file_path = os.path.join(root, file)
                # Check if the file modification time is older than the cutoff date
                if os.path.getmtime(file_path) < cutoff_date.timestamp():
                    total_size_bytes += os.path.getsize(file_path)
                    os.remove(file_path)
                    logging.info(f"Deleted file: {file_path}")
    else:
        logging.error(f"Log directory not found: {log_dir}")
    context["ti"].xcom_push(key="total_size_bytes", value=total_size_bytes)


@task()
def delete_old_runs():
    """
    Query and delete runs older than the threshold date (2 months ago).
    """
    oldest_run_date = datetime.now(tz=timezone.utc) - timedelta(days=nb_days_to_keep)

    # Create a session to interact with the metadata database
    session = Session()

    try:
        all_runs = session.query(DagRun).all()
        idx = 0
        for run in all_runs:
            if run.end_date is None or run.end_date > oldest_run_date:
                continue
            idx += 1
            logging.info(f"Deleting run: dag_id={run.dag_id}, end_date={run.end_date}")
            session.delete(run)
            if idx % 50 == 0:
                session.commit()
        session.commit()
    except Exception as e:
        logging.error(f"Error deleting old runs: {str(e)}")
        session.rollback()
    finally:
        session.close()


@task()
def send_notification_mattermost(**context):
    total_size_bytes = context["ti"].xcom_pull(
        key="total_size_bytes", task_ids="delete_logs"
    )
    units = ["octets", "ko", "Mo", "Go", "To"]
    k = 0
    while total_size_bytes > 1e3 and k < len(units):
        k += 1
        total_size_bytes = total_size_bytes / 1e3
    total_size_bytes = round(total_size_bytes, 1)
    unit = units[k]
    send_message(
        f"Les logs Airflow d'il y a plus de {nb_days_to_keep} jours ont été supprimés"
        f" ({total_size_bytes}{' ' * (len(unit) > 2) + unit} nettoyés)"
    )


# Define default arguments for the DAG
default_args = {
    "depends_on_past": False,
    "retries": 4,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="maintenance_clean_logs_and_runs",
    default_args=default_args,
    description=f"Delete Airflow logs and runs older than {nb_days_to_keep} days",
    schedule="0 16 */2 * *",  # run every 2nd of month at 4:00 PM (UTC)
    dagrun_timeout=timedelta(minutes=1200),
    start_date=datetime(2024, 1, 25),
    catchup=False,  # False to ignore past runs
    max_active_runs=1,
):
    (
        [
            delete_old_logs_and_directories(),
            delete_old_runs(),
        ]
        >> send_notification_mattermost()
    )
