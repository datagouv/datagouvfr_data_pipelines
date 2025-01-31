from datetime import timedelta, datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.bash import BashOperator

# from datagouvfr_data_pipelines.utils.utils import check_if_monday
from datagouvfr_data_pipelines.dgv.monitoring.hvd.task_functions import (
    DAG_NAME,
    DATADIR,
    # get_hvd,
    # send_to_minio,
    # publish_mattermost,
    build_df_for_grist,
    update_grist,
    publish_mattermost_grist,
)

default_args = {
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_NAME,
    schedule_interval="0 6 * * *",
    start_date=datetime(2024, 6, 1),
    dagrun_timeout=timedelta(minutes=60),
    tags=["hvd", "datagouv"],
    default_args=default_args,
    catchup=False,
) as dag:

    clean_previous_outputs = BashOperator(
        task_id="clean_previous_outputs",
        bash_command=f"rm -rf {DATADIR} && mkdir -p {DATADIR}",
    )

    # OBSOLETE
    # Recap HVD mattermost
    # check_if_monday = ShortCircuitOperator(
    #     task_id="check_if_monday",
    #     python_callable=check_if_monday,
    # )

    # get_hvd = PythonOperator(
    #     task_id="get_hvd",
    #     python_callable=get_hvd,
    # )

    # send_to_minio = PythonOperator(
    #     task_id="send_to_minio",
    #     python_callable=send_to_minio,
    # )

    # publish_mattermost = PythonOperator(
    #     task_id="publish_mattermost",
    #     python_callable=publish_mattermost,
    # )

    # check_if_monday.set_upstream(clean_previous_outputs)
    # get_hvd.set_upstream(check_if_monday)
    # send_to_minio.set_upstream(get_hvd)
    # publish_mattermost.set_upstream(send_to_minio)

    # Grist
    build_df_for_grist = PythonOperator(
        task_id="build_df_for_grist",
        python_callable=build_df_for_grist,
    )

    update_grist = ShortCircuitOperator(
        task_id="update_grist",
        python_callable=update_grist,
    )

    publish_mattermost_grist = PythonOperator(
        task_id="publish_mattermost_grist",
        python_callable=publish_mattermost_grist,
    )

    build_df_for_grist.set_upstream(clean_previous_outputs)
    update_grist.set_upstream(build_df_for_grist)
    publish_mattermost_grist.set_upstream(update_grist)
