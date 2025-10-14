from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from datagouvfr_data_pipelines.data_processing.sante.finess.task_functions import (
    DAG_NAME,
    TMP_FOLDER,
    DATADIR,
    check_if_modif,
    get_finess_columns,
    get_geoloc_columns,
    build_finess_table_entites_juridiques,
    build_finess_table_etablissements,
    send_to_minio,
    publish_on_datagouv,
    send_notification_mattermost,
)

DAG_FOLDER = "datagouvfr_data_pipelines/data_processing/"

default_args = {
    "retries": 5,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_NAME,
    schedule_interval="0 7 * * *",
    start_date=datetime(2024, 8, 10),
    catchup=False,
    dagrun_timeout=timedelta(minutes=240),
    tags=["data_processing", "sante", "finess"],
    default_args=default_args,
) as dag:
    clean_previous_outputs = BashOperator(
        task_id="clean_previous_outputs",
        bash_command=f"rm -rf {TMP_FOLDER} && mkdir -p {DATADIR}",
    )

    # pipeline établissements
    check_if_modif_etablissements = ShortCircuitOperator(
        task_id="check_if_modif_etablissements",
        python_callable=check_if_modif,
        op_kwargs={"scope": "etablissements"},
    )

    get_finess_columns_etablissements = PythonOperator(
        task_id="get_finess_columns_etablissements",
        python_callable=get_finess_columns,
        op_kwargs={"scope": "etablissements"},
    )

    get_geoloc_columns = PythonOperator(
        task_id="get_geoloc_columns",
        python_callable=get_geoloc_columns,
    )

    build_finess_table_etablissements = PythonOperator(
        task_id="build_finess_table_etablissements",
        python_callable=build_finess_table_etablissements,
    )

    send_to_minio_etablissements = PythonOperator(
        task_id="send_to_minio_etablissements",
        python_callable=send_to_minio,
        op_kwargs={"scope": "etablissements"},
    )

    publish_on_datagouv_etablissements = PythonOperator(
        task_id="publish_on_datagouv_etablissements",
        python_callable=publish_on_datagouv,
        op_kwargs={"scope": "etablissements"},
    )

    check_if_modif_etablissements.set_upstream(clean_previous_outputs)

    get_finess_columns_etablissements.set_upstream(check_if_modif_etablissements)
    get_geoloc_columns.set_upstream(check_if_modif_etablissements)

    build_finess_table_etablissements.set_upstream(get_finess_columns_etablissements)
    build_finess_table_etablissements.set_upstream(get_geoloc_columns)

    send_to_minio_etablissements.set_upstream(build_finess_table_etablissements)
    publish_on_datagouv_etablissements.set_upstream(send_to_minio_etablissements)

    # pipeline entités juridiques
    check_if_modif_entites_juridiques = ShortCircuitOperator(
        task_id="check_if_modif_entites_juridiques",
        python_callable=check_if_modif,
        op_kwargs={"scope": "entites_juridiques"},
    )

    get_finess_columns_entites_juridiques = PythonOperator(
        task_id="get_finess_columns_entites_juridiques",
        python_callable=get_finess_columns,
        op_kwargs={"scope": "entites_juridiques"},
    )

    build_finess_table_entites_juridiques = PythonOperator(
        task_id="build_finess_table_entites_juridiques",
        python_callable=build_finess_table_entites_juridiques,
    )

    send_to_minio_entites_juridiques = PythonOperator(
        task_id="send_to_minio_entites_juridiques",
        python_callable=send_to_minio,
        op_kwargs={"scope": "entites_juridiques"},
    )

    publish_on_datagouv_entites_juridiques = PythonOperator(
        task_id="publish_on_datagouv_entites_juridiques",
        python_callable=publish_on_datagouv,
        op_kwargs={"scope": "entites_juridiques"},
    )

    check_if_modif_entites_juridiques.set_upstream(clean_previous_outputs)
    get_finess_columns_entites_juridiques.set_upstream(
        check_if_modif_entites_juridiques
    )
    build_finess_table_entites_juridiques.set_upstream(
        get_finess_columns_entites_juridiques
    )
    send_to_minio_entites_juridiques.set_upstream(build_finess_table_entites_juridiques)
    publish_on_datagouv_entites_juridiques.set_upstream(
        send_to_minio_entites_juridiques
    )

    # final steps
    clean_up = BashOperator(
        task_id="clean_up",
        bash_command=f"rm -rf {TMP_FOLDER}",
        trigger_rule="none_failed",
    )

    send_notification_mattermost = PythonOperator(
        task_id="send_notification_mattermost",
        python_callable=send_notification_mattermost,
        trigger_rule="none_failed",
    )

    clean_up.set_upstream(publish_on_datagouv_etablissements)
    clean_up.set_upstream(publish_on_datagouv_entites_juridiques)

    send_notification_mattermost.set_upstream(clean_up)
