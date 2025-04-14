import os
import json
from datetime import datetime
import logging
from airflow.providers.sftp.operators.sftp import SFTPOperator

from datagouvfr_data_pipelines.utils.filesystem import File
from datagouvfr_data_pipelines.utils.download import download_files
from datagouvfr_data_pipelines.utils.minio import MinIOClient
from datagouvfr_data_pipelines.utils.datagouv import local_client
from datagouvfr_data_pipelines.utils.mattermost import send_message
from datagouvfr_data_pipelines.utils.utils import MOIS_FR
from datagouvfr_data_pipelines.config import (
    FILES_BASE_URL,
    INSEE_BASE_URL,
    MINIO_BUCKET_DATA_PIPELINE,
    SECRET_INSEE_LOGIN,
    SECRET_INSEE_PASSWORD,
)

minio_restricted = MinIOClient(bucket=MINIO_BUCKET_DATA_PIPELINE)


def get_files(**kwargs):
    templates_dict = kwargs.get("templates_dict")
    tmp_dir = templates_dict.get("tmp_dir")
    resource_file = templates_dict.get("resource_file")

    with open(f"{os.path.dirname(__file__)}/config/{resource_file}") as json_file:
        data = json.load(json_file)

    download_files(
        list_urls=[
            File(
                url=f"{INSEE_BASE_URL}{item['nameFTP']}",
                dest_path=tmp_dir,
                dest_name=item["nameFTP"],
            ) for item in data
        ],
        auth_user=SECRET_INSEE_LOGIN,
        auth_password=SECRET_INSEE_PASSWORD,
        timeout=1200,
    )


def upload_files_minio(**kwargs):
    templates_dict = kwargs.get("templates_dict")
    tmp_dir = templates_dict.get("tmp_dir")
    minio_path = templates_dict.get("minio_path")
    resource_file = templates_dict.get("resource_file")

    with open(f"{os.path.dirname(__file__)}/config/{resource_file}") as json_file:
        data = json.load(json_file)

    minio_restricted.send_files(
        list_files=[
            File(
                source_path=tmp_dir,
                source_name=item["nameFTP"],
                dest_path=minio_path,
                dest_name=item["nameFTP"],
            ) for item in data
        ],
    )


def move_new_files_to_latest(**kwargs):
    templates_dict = kwargs.get("templates_dict")
    minio_new_path = templates_dict.get("minio_new_path")
    minio_latest_path = templates_dict.get("minio_latest_path")
    resource_file = templates_dict.get("resource_file")

    with open(f"{os.path.dirname(__file__)}/config/{resource_file}") as json_file:
        data = json.load(json_file)
    minio_restricted.copy_many_objects(
        obj_source_paths=[minio_new_path + item["nameFTP"] for item in data],
        target_directory=minio_latest_path,
        remove_source_file=True,
    )


def compare_minio_files(**kwargs):
    templates_dict = kwargs.get("templates_dict")
    minio_path_new = templates_dict.get("minio_path_new")
    minio_path_latest = templates_dict.get("minio_path_latest")
    resource_file = templates_dict.get("resource_file")

    with open(f"{os.path.dirname(__file__)}/config/{resource_file}") as json_file:
        data = json.load(json_file)
    logging.info(data)

    nb_same = 0
    for item in data:
        isSame = minio_restricted.compare_files(
            file_path_1=minio_path_latest,
            file_path_2=minio_path_new,
            file_name_1=item["nameFTP"],
            file_name_2=item["nameFTP"],
        )
        if isSame:
            nb_same = nb_same + 1
    if nb_same == len(data):
        return False
    else:
        return True


def publish_file_files_data_gouv(**kwargs):
    templates_dict = kwargs.get("templates_dict")
    tmp_dir = templates_dict.get("tmp_dir")
    resource_file = templates_dict.get("resource_file")
    files_path = templates_dict.get("files_path")
    with open(f"{os.path.dirname(__file__)}/config/{resource_file}") as json_file:
        data = json.load(json_file)
    logging.info(data)

    for d in data:
        put_file = SFTPOperator(
            task_id="put-file",
            ssh_conn_id="SSH_FILES_DATA_GOUV_FR",
            local_filepath=f"{tmp_dir}{d['nameFTP']}",
            remote_filepath=f"{FILES_BASE_URL}{files_path}{d['nameFTP']}",
            operation="put",
        )
        put_file_with_date = SFTPOperator(
            task_id="put-file",
            ssh_conn_id="SSH_FILES_DATA_GOUV_FR",
            local_filepath=tmp_dir + d["nameFTP"],
            remote_filepath=(
                f"{FILES_BASE_URL}{files_path}"
                f"{datetime.today().strftime('%Y-%m')}-01-{d['nameFTP']}"
            ),
            operation="put",
        )
        put_file.execute(dict())
        put_file_with_date.execute(dict())


def update_dataset_data_gouv(**kwargs):
    templates_dict = kwargs.get("templates_dict")
    resource_file = templates_dict.get("resource_file")
    day_file = templates_dict.get("day_file")

    liste_mois = [
        m.title() for m in MOIS_FR.values()
    ]
    mois = datetime.now().date().month
    with open(f"{os.path.dirname(__file__)}/config/{resource_file}") as json_file:
        data = json.load(json_file)
    logging.info(data)
    for d in data:
        local_client.resource(
            dataset_id=d["dataset_id"],
            id=d["resource_id"],
            fetch=False,
        ).update(
            payload={
                "title": (
                    f"Sirene : Fichier {d['name'].replace('_utf8.zip', '')} du "
                    f"{day_file} {liste_mois[mois - 1]} {datetime.today().strftime('%Y')}"
                ),
            },
        )


def publish_mattermost(geoloc):
    if geoloc:
        text = (
            "Sirene géolocalisé INSEE mis à jour\n- [Stockage données]"
            "(https://files.data.gouv.fr/insee-sirene-geo)"
            "\n- [Jeu de données data.gouv.fr](https://www.data.gouv.fr/fr/datasets/geolocalisation-"
            "des-etablissements-du-repertoire-sirene-pour-les-etudes-statistiques/) "
        )
    else:
        text = (
            "Base Sirene mise à jour\n- [Stockage données](https://files.data.gouv.fr/insee-sirene)"
            "\n- [Jeu de données data.gouv.fr](https://www.data.gouv.fr/fr/datasets/base-sirene-des-"
            "entreprises-et-de-leurs-etablissements-siren-siret/) "
        )
    send_message(
        text=text,
        image_url="https://static.data.gouv.fr/avatars/db/fbfd745ae543f6856ed59e3d44eb71.jpg",
    )
