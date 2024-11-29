from datetime import timedelta
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_HOME,
    AIRFLOW_DAG_TMP,
)
from datagouvfr_data_pipelines.data_processing.dvf.task_functions import (
    get_year_interval,
    alter_dvf_table,
    create_copro_table,
    populate_copro_table,
    process_dpe,
    create_dpe_table,
    populate_dpe_table,
    index_dpe_table,
    create_dvf_table,
    populate_dvf_table,
    index_dvf_table,
    create_stats_dvf_table,
    get_epci,
    populate_stats_dvf_table,
    process_dvf_stats,
    publish_stats_dvf,
    send_stats_to_minio,
    notification_mattermost,
    create_distribution_and_stats_whole_period,
    create_distribution_table,
    populate_distribution_table,
    create_whole_period_table,
    populate_whole_period_table,
    send_distribution_to_minio
)

TMP_FOLDER = f"{AIRFLOW_DAG_TMP}dvf/"
DAG_FOLDER = 'datagouvfr_data_pipelines/data_processing/'
DAG_NAME = 'data_processing_dvf'
DATADIR = f"{AIRFLOW_DAG_TMP}dvf/data"
start, end = get_year_interval()

default_args = {
    'email': [
        'pierlou.ramade@data.gouv.fr',
        'geoffrey.aldebert@data.gouv.fr'
    ],
    'email_on_failure': False
}

with DAG(
    dag_id=DAG_NAME,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=300),
    tags=["data_processing", "dvf", "stats"],
    default_args=default_args,
) as dag:

    clean_previous_outputs = BashOperator(
        task_id="clean_previous_outputs",
        bash_command=f"rm -rf {TMP_FOLDER} && mkdir -p {TMP_FOLDER}",
    )

    download_dvf_data = BashOperator(
        task_id='download_dvf_data',
        bash_command=(
            f"sh {AIRFLOW_DAG_HOME}{DAG_FOLDER}"
            f"dvf/scripts/script_dl_dvf.sh {DATADIR} "
            f"{start} {end} "
        )
    )

    download_copro = BashOperator(
        task_id='download_copro',
        bash_command=(
            f"sh {AIRFLOW_DAG_HOME}{DAG_FOLDER}"
            f"dvf/scripts/script_dl_copro.sh {DATADIR} "
        )
    )

    create_copro_table = PythonOperator(
        task_id='create_copro_table',
        python_callable=create_copro_table,
    )

    populate_copro_table = PythonOperator(
        task_id='populate_copro_table',
        python_callable=populate_copro_table,
    )

    download_dpe = BashOperator(
        task_id='download_dpe',
        bash_command=(
            f"sh {AIRFLOW_DAG_HOME}{DAG_FOLDER}"
            f"dvf/scripts/script_dl_dpe.sh {DATADIR} "
        )
    )

    process_dpe = PythonOperator(
        task_id='process_dpe',
        python_callable=process_dpe,
    )

    create_dpe_table = PythonOperator(
        task_id='create_dpe_table',
        python_callable=create_dpe_table,
    )

    populate_dpe_table = PythonOperator(
        task_id='populate_dpe_table',
        python_callable=populate_dpe_table,
    )

    alter_dvf_table = PythonOperator(
        task_id='alter_dvf_table',
        python_callable=alter_dvf_table,
    )

    index_dpe_table = PythonOperator(
        task_id='index_dpe_table',
        python_callable=index_dpe_table,
    )

    create_dvf_table = PythonOperator(
        task_id='create_dvf_table',
        python_callable=create_dvf_table,
    )

    populate_dvf_table = PythonOperator(
        task_id='populate_dvf_table',
        python_callable=populate_dvf_table,
    )

    index_dvf_table = PythonOperator(
        task_id='index_dvf_table',
        python_callable=index_dvf_table,
    )

    get_epci = PythonOperator(
        task_id='get_epci',
        python_callable=get_epci,
    )

    process_dvf_stats = PythonOperator(
        task_id='process_dvf_stats',
        python_callable=process_dvf_stats,
    )

    create_distribution_and_stats_whole_period = PythonOperator(
        task_id='create_distribution_and_stats_whole_period',
        python_callable=create_distribution_and_stats_whole_period,
    )

    create_distribution_table = PythonOperator(
        task_id='create_distribution_table',
        python_callable=create_distribution_table,
    )

    populate_distribution_table = PythonOperator(
        task_id='populate_distribution_table',
        python_callable=populate_distribution_table,
    )

    send_distribution_to_minio = PythonOperator(
        task_id='send_distribution_to_minio',
        python_callable=send_distribution_to_minio,
    )

    create_stats_dvf_table = PythonOperator(
        task_id='create_stats_dvf_table',
        python_callable=create_stats_dvf_table,
    )

    populate_stats_dvf_table = PythonOperator(
        task_id='populate_stats_dvf_table',
        python_callable=populate_stats_dvf_table,
    )

    create_whole_period_table = PythonOperator(
        task_id='create_whole_period_table',
        python_callable=create_whole_period_table,
    )

    populate_whole_period_table = PythonOperator(
        task_id='populate_whole_period_table',
        python_callable=populate_whole_period_table,
    )

    send_stats_to_minio = PythonOperator(
        task_id='send_stats_to_minio',
        python_callable=send_stats_to_minio,
    )

    publish_stats_dvf = PythonOperator(
        task_id='publish_stats_dvf',
        python_callable=publish_stats_dvf,
    )

    clean_up = BashOperator(
        task_id="clean_up",
        bash_command=f"rm -rf {TMP_FOLDER}",
    )

    notification_mattermost = PythonOperator(
        task_id="notification_mattermost",
        python_callable=notification_mattermost,
    )

    download_dvf_data.set_upstream(clean_previous_outputs)

    download_copro.set_upstream(download_dvf_data)
    create_copro_table.set_upstream(download_copro)
    populate_copro_table.set_upstream(create_copro_table)

    download_dpe.set_upstream(download_dvf_data)
    process_dpe.set_upstream(download_dpe)
    create_dpe_table.set_upstream(process_dpe)
    populate_dpe_table.set_upstream(create_dpe_table)
    index_dpe_table.set_upstream(populate_dpe_table)

    create_dvf_table.set_upstream(download_dvf_data)
    populate_dvf_table.set_upstream(create_dvf_table)
    alter_dvf_table.set_upstream(populate_dvf_table)
    index_dvf_table.set_upstream(alter_dvf_table)

    get_epci.set_upstream(download_dvf_data)
    process_dvf_stats.set_upstream(get_epci)

    create_distribution_and_stats_whole_period.set_upstream(process_dvf_stats)

    create_distribution_table.set_upstream(create_distribution_and_stats_whole_period)
    populate_distribution_table.set_upstream(create_distribution_table)

    create_whole_period_table.set_upstream(create_distribution_and_stats_whole_period)
    populate_whole_period_table.set_upstream(create_whole_period_table)

    send_distribution_to_minio.set_upstream(create_distribution_and_stats_whole_period)

    send_stats_to_minio.set_upstream(create_distribution_and_stats_whole_period)
    publish_stats_dvf.set_upstream(send_stats_to_minio)

    create_stats_dvf_table.set_upstream(process_dvf_stats)
    populate_stats_dvf_table.set_upstream(create_stats_dvf_table)

    clean_up.set_upstream(publish_stats_dvf)
    clean_up.set_upstream(populate_copro_table)
    clean_up.set_upstream(index_dpe_table)
    clean_up.set_upstream(populate_stats_dvf_table)
    clean_up.set_upstream(index_dvf_table)
    clean_up.set_upstream(send_distribution_to_minio)
    clean_up.set_upstream(populate_distribution_table)
    clean_up.set_upstream(populate_whole_period_table)

    notification_mattermost.set_upstream(clean_up)
