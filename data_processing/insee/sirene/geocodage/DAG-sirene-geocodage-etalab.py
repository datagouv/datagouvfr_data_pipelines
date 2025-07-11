from datetime import datetime

from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator

from datagouvfr_data_pipelines.config import AIRFLOW_ENV

SCRIPTS_PATH = "datagouvfr_data_pipelines/data_processing/insee/sirene/geocodage/scripts/"
DEV_GIT_BRANCH = "main"  # It is the remote branch that will be cloned when using local Airflow
DEV_GIT_ARGS = f"--single-branch --branch {DEV_GIT_BRANCH}" if AIRFLOW_ENV == "dev" else ""

with DAG(
    dag_id="data_processing_sirene_geocodage",
    schedule_interval=None,  # triggered by data_processing_sirene_publication
    start_date=datetime(2024, 8, 10),
    catchup=False,
    tags=["data_processing", "sirene", "geocodage"],
    params={},
) as dag:
    common_kwargs = {
        "ssh_conn_id": "SSH_DATAENG_ETALAB_STUDIO",
        "dag": dag,
        "conn_timeout": (3600 * 8),
        "cmd_timeout": (3600 * 8),
    }

    if not AIRFLOW_ENV:
        raise ValueError("AIRFLOW_ENV has to be set (e.g. prod, dev)")

    start_addok = SSHOperator(
        task_id="start_addok",
        command=(
            "cd /srv/sirene/addok-docker/ && docker-compose -f docker-compose-ban-poi.yml up"
            "--scale addok-ban=6 --scale addok-redis-ban=6 -d"
        ),
        **common_kwargs,
    )

    wait_addok_to_be_ready = SSHOperator(
        task_id="wait_addok_to_be_ready",
        command="sleep 120",
        **common_kwargs,
    )

    clone_repo_to_get_scripts = SSHOperator(
        task_id="clone_repo_to_get_scripts",
        command=(
            "cd /srv/sirene/geocodage-sirene "
            "&& rm -rf datagouvfr_data_pipelines "
            f"&& git clone {DEV_GIT_ARGS} https://github.com/datagouv/datagouvfr_data_pipelines.git "
            f"&& chmod +x /srv/sirene/geocodage-sirene/{SCRIPTS_PATH}*"
        ),
        **common_kwargs,
    )

    download_last_sirene_batch = SSHOperator(
        task_id="download_last_sirene_batch",
        command=f"/srv/sirene/geocodage-sirene/{SCRIPTS_PATH}1_download_last_sirene_batch.sh {AIRFLOW_ENV}",
        **common_kwargs,
    )

    split_departments_files = SSHOperator(
        task_id="split_departments_files",
        command=f"/srv/sirene/geocodage-sirene/{SCRIPTS_PATH}2_split_departments_files.sh {AIRFLOW_ENV}",
        **common_kwargs,
    )

    geocoding = SSHOperator(
        task_id="geocoding",
        command=(
            f"/srv/sirene/geocodage-sirene/{SCRIPTS_PATH}3_geocoding_by_increasing_size.sh {AIRFLOW_ENV} "
            f"/srv/sirene/geocodage-sirene/{SCRIPTS_PATH[:-1]}"
        ),
        **common_kwargs,
    )

    split_by_locality = SSHOperator(
        task_id="split_by_locality",
        command=(
            f"/srv/sirene/geocodage-sirene/{SCRIPTS_PATH}4a_split_by_locality.sh {AIRFLOW_ENV} "
            f"/srv/sirene/geocodage-sirene/{SCRIPTS_PATH[:-1]}"
        ),
        **common_kwargs,
    )

    get_geocode_stats = SSHOperator(
        task_id="get_geocode_stats",
        command=f"/srv/sirene/geocodage-sirene/{SCRIPTS_PATH}4b_geocode_stats.sh {AIRFLOW_ENV}",
        **common_kwargs,
    )

    shutdown_addok = SSHOperator(
        task_id="shutdown_addok",
        command="cd /srv/sirene/addok-docker/ && docker-compose down --remove-orphans",
        **common_kwargs,
    )

    check_stats_coherence = SSHOperator(
        task_id="check_stats_coherence",
        command=f"/srv/sirene/venv/bin/python /srv/sirene/geocodage-sirene/{SCRIPTS_PATH}4c_check_stats_coherence.py {AIRFLOW_ENV}",
        **common_kwargs,
    )

    national_files_agregation = SSHOperator(
        task_id="national_files_agregation",
        command=f"/srv/sirene/geocodage-sirene/{SCRIPTS_PATH}5_national_files_agregation.sh {AIRFLOW_ENV}",
        **common_kwargs,
    )

    wait_addok_to_be_ready.set_upstream(start_addok)
    download_last_sirene_batch.set_upstream(wait_addok_to_be_ready)
    download_last_sirene_batch.set_upstream(clone_repo_to_get_scripts)
    split_departments_files.set_upstream(download_last_sirene_batch)
    geocoding.set_upstream(split_departments_files)
    split_by_locality.set_upstream(geocoding)
    get_geocode_stats.set_upstream(geocoding)
    shutdown_addok.set_upstream(geocoding)
    national_files_agregation.set_upstream(geocoding)
    check_stats_coherence.set_upstream(get_geocode_stats)

    if AIRFLOW_ENV == "prod":
        prepare_to_rsync = SSHOperator(
            task_id="prepare_to_rsync",
            command=f"/srv/sirene/geocodage-sirene/{SCRIPTS_PATH}6_prepare_to_rsync.sh ",
            **common_kwargs,
        )

        rsync_to_files_data_gouv = SSHOperator(
            task_id="rsync_to_files_data_gouv",
            command=f"/srv/sirene/geocodage-sirene/{SCRIPTS_PATH}7_rsync_to_files_data_gouv.sh ",
            **common_kwargs,
        )

        mkdir_last_and_symbolic_links = SSHOperator(
            ssh_conn_id="SSH_FILES_DATA_GOUV_FR",
            task_id="mkdir_last_and_symbolic_links",
            command=(
                r"cd /srv/nfs/files.data.gouv.fr/geo-sirene/ && rm -rf last/* && rm -rf last && "
                r"mkdir last && cd $(date +%Y-%m)/ && find . \( ! -regex '\.' \) -type d -exec "
                r"mkdir /srv/nfs/files.data.gouv.fr/geo-sirene/last/{} \; && find * -type f -exec "
                r"ln -s `pwd`/{} /srv/nfs/files.data.gouv.fr/geo-sirene/last/{} \;"
            ),
            dag=dag,
            conn_timeout=(3600 * 8),
            cmd_timeout=(3600 * 8),
        )

        prepare_to_rsync.set_upstream(national_files_agregation)
        prepare_to_rsync.set_upstream(split_by_locality)
        prepare_to_rsync.set_upstream(check_stats_coherence)
        rsync_to_files_data_gouv.set_upstream(prepare_to_rsync)
        mkdir_last_and_symbolic_links.set_upstream(rsync_to_files_data_gouv)
