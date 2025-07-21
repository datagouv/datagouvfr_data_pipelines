import os
import json
from datetime import datetime
import logging
import zipfile

from datagouvfr_data_pipelines.utils.filesystem import File, compute_checksum_from_file
from datagouvfr_data_pipelines.utils.download import download_files
from datagouvfr_data_pipelines.utils.minio import MinIOClient
from datagouvfr_data_pipelines.utils.datagouv import local_client
from datagouvfr_data_pipelines.utils.mattermost import send_message
from datagouvfr_data_pipelines.utils.utils import MOIS_FR, csv_to_parquet
from datagouvfr_data_pipelines.config import (
    INSEE_BASE_URL,
    MINIO_BUCKET_DATA_PIPELINE,
    MINIO_BUCKET_DATA_PIPELINE_OPEN,
    SECRET_INSEE_LOGIN,
    SECRET_INSEE_PASSWORD,
)

minio_restricted = MinIOClient(bucket=MINIO_BUCKET_DATA_PIPELINE)
minio_open = MinIOClient(bucket=MINIO_BUCKET_DATA_PIPELINE_OPEN)


def check_if_already_processed(minio_path: str):
    files_in_folder = minio_open.get_files_from_prefix(
        prefix=minio_path,
        ignore_airflow_env=True,
    )
    this_month_prefix = datetime.today().strftime("%Y-%m")
    for file in reversed(sorted(files_in_folder)):
        if file.startswith(this_month_prefix):
            # early stop and don't trigger the following tasks
            return False
    return True


def get_files(ti, **kwargs):
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
            if item["nameFTP"] not in os.listdir(tmp_dir)
        ],
        auth_user=SECRET_INSEE_LOGIN,
        auth_password=SECRET_INSEE_PASSWORD,
        timeout=1200,
    )

    # parquet conversion (extract zip, convert, delete intermediary step)
    for item in data:
        csv_name = item["nameFTP"].replace(".zip", ".csv")
        logging.info(f"Unzipping {csv_name}")
        with zipfile.ZipFile(tmp_dir + item["nameFTP"], 'r') as zip_ref:
            zip_ref.extract(csv_name, tmp_dir)
        csv_to_parquet(
            tmp_dir + csv_name,
            sep=";" if "Geolocalisation" in csv_name else ",",
            dtype=item["dtype"],
            strict_mode=False,
            # the files may contain things like "\," which make the parquet export crash,
            # so using a very rare character as escape to make it work
            escapechar="\u0001",
        )
        os.remove(tmp_dir + csv_name)

    hashfiles = {}
    for item in data:
        hashfiles[item["nameFTP"]] = compute_checksum_from_file(f"{tmp_dir}{item['nameFTP']}")

    ti.xcom_push(key="hashes", value=hashfiles)


def publish_file_minio(**kwargs):
    templates_dict = kwargs.get("templates_dict")
    tmp_dir = templates_dict.get("tmp_dir")
    resource_file = templates_dict.get("resource_file")
    minio_path = templates_dict.get("minio_path")
    with open(f"{os.path.dirname(__file__)}/config/{resource_file}") as json_file:
        data = json.load(json_file)
    logging.info(data)

    minio_open.send_files(
        list_files=[
            File(
                source_path=tmp_dir,
                source_name=item["nameFTP"],
                dest_path=minio_path,
                dest_name=item["nameFTP"],
            ) for item in data
        ],
    )

    # sending parquet
    minio_open.send_files(
        list_files=[
            File(
                source_path=tmp_dir,
                source_name=item["nameFTP"].replace(".zip", ".parquet"),
                dest_path=minio_path,
                dest_name=item["nameFTP"].replace(".zip", ".parquet"),
            ) for item in data
        ],
    )

    # sending dated files
    minio_open.send_files(
        list_files=[
            File(
                source_path=tmp_dir,
                source_name=item["nameFTP"],
                dest_path=minio_path,
                dest_name=f"{datetime.today().strftime('%Y-%m')}-01-{item['nameFTP']}",
            ) for item in data
        ],
    )


def update_dataset_data_gouv(ti, **kwargs):
    hashes = ti.xcom_pull(key="hashes", task_ids="get_files")
    templates_dict = kwargs.get("templates_dict")
    resource_file = templates_dict.get("resource_file")
    day_file = templates_dict.get("day_file")
    tmp_dir = templates_dict.get("tmp_dir")

    liste_mois = [
        m.title() for m in MOIS_FR.values()
    ]
    mois = datetime.now().date().month
    with open(f"{os.path.dirname(__file__)}/config/{resource_file}") as json_file:
        data = json.load(json_file)
    logging.info(data)
    for d in data:
        obj = {
            "title": (
                f"Sirene : Fichier {d['name'].replace('_utf8.zip', '')} du "
                f"{day_file} {liste_mois[mois - 1]} {datetime.today().strftime('%Y')}"
            ),
            "filesize": os.path.getsize(tmp_dir + d["nameFTP"]),
        }
        if d["nameFTP"] in hashes:
            obj["checksum"] = {
                "type": "sha256",
                "value": hashes[d["nameFTP"]]
            }
        local_client.resource(
            dataset_id=d["dataset_id"],
            id=d["resource_id"],
            fetch=False,
        ).update(payload=obj)

        # updating parquet
        local_client.resource(
            dataset_id=d["dataset_id"],
            id=d["parquet_resource_id"],
            fetch=False,
        ).update(
            payload={
                "title": (
                    f"Sirene : Fichier {d['name'].replace('_utf8.zip', '')} du "
                    f"{day_file} {liste_mois[mois - 1]} {datetime.today().strftime('%Y')}"
                    " (format parquet)"
                ),
                "filesize": os.path.getsize(tmp_dir + d["nameFTP"].replace(".zip", ".parquet")),
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
