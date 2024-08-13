import pandas as pd
import re
import requests
import json
import os
from datetime import datetime, timedelta, timezone

from datagouvfr_data_pipelines.config import (
    AIRFLOW_ENV,
    AIRFLOW_DAG_HOME,
    AIRFLOW_DAG_TMP,
    MINIO_BUCKET_DATA_PIPELINE_OPEN,
)
from datagouvfr_data_pipelines.utils.utils import csv_to_parquet
from datagouvfr_data_pipelines.utils.minio import MinIOClient
from datagouvfr_data_pipelines.utils.datagouv import post_remote_resource

DAG_FOLDER = "datagouvfr_data_pipelines/data_processing/"
DATADIR = f"{AIRFLOW_DAG_TMP}deces"
minio_open = MinIOClient(bucket=MINIO_BUCKET_DATA_PIPELINE_OPEN)


def check_if_new_file():
    resources = requests.get(
        'https://www.data.gouv.fr/api/1/datasets/5de8f397634f4164071119c5/',
        headers={"X-fields": "resources{created_at}"}
    ).json()['resources']
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    return True
    return any(
        r["created_at"] >= yesterday for r in resources
    )


def clean_period(file_name):
    return file_name.replace('deces-', '').replace('.txt', '')


def get_fields(row):
    nom_prenom = row[:80].strip()
    d = {
        "nom": nom_prenom.split("*")[0],
        "prenoms": nom_prenom.split("*")[1].replace('/', '').replace(' ', ','),
        "sexe": row[80].replace('1', 'M').replace('2', 'F'),
        "date_naissance": row[81:89],
        "code_insee_naissance": row[89:94],
        "commune_naissance": row[94:124].strip(),
        "pays_naissance": row[124:154].strip() or 'FRANCE',
        "date_deces": row[154:162],
        "code_insee_deces": row[162:167],
        "numero_acte_deces": row[167:176].strip(),
    }
    return d


def gather_data(ti):
    print("Getting resources list")
    resources = requests.get(
        "https://www.data.gouv.fr/api/1/datasets/5de8f397634f4164071119c5/",
        headers={"X-fields": "resources{id,title}"},
    ).json()["resources"]
    year_regex = r'deces-\d{4}.txt'
    month_regex = r'deces-\d{4}-m\d{2}.txt'
    full_years = []
    ids = {}
    for r in resources:
        if re.match(year_regex, r['title']):
            ids[clean_period(r['title'])] = r['id']
            full_years.append(r['title'][6:10])
    print(full_years)
    for r in resources:
        if re.match(month_regex, r['title']) and r['title'][6:10] not in full_years:
            print(r['title'])
            ids[clean_period(r['title'])] = r['id']

    errors = []
    columns = {}
    for idx, (origin, rid) in enumerate(ids.items()):
        data = []
        print(f'Proccessing {origin}')
        rows = requests.get(f'https://www.data.gouv.fr/fr/datasets/r/{rid}').text.split('\n')
        for r in rows:
            if not r:
                continue
            try:
                fields = get_fields(r)
                data.append({**fields, "fichier_origine": origin})
            except:
                print(r)
                errors.append(r)
        # can't have the whole dataframe in RAM, so saving in batches
        df = pd.DataFrame(data)
        del data
        df.to_csv(
            DATADIR + '/deces.csv',
            index=False,
            mode="w" if idx == 0 else "a",
        )
        for c in df.columns:
            if c not in columns:
                columns[c] = max(df[c].str.len())
            else:
                columns[c] = max(max(df[c].str.len()), columns[c])
        del df
    print(f"> {len(errors)} erreur(s)")
    # conversion to parquet, all columns are considered strings by default which is fine
    csv_to_parquet(
        DATADIR + '/deces.csv',
        sep=',',
        # ICI à comparer avec notepad
        columns={c: f"VARCHAR({v})" for c, v in columns.items()}
    )

    ti.xcom_push(key="min_date", value=min(ids.keys()))
    ti.xcom_push(key="max_date", value=max(ids.keys()))


def send_to_minio():
    minio_open.send_files(
        list_files=[
            {
                "source_path": f"{DATADIR}/",
                "source_name": f"deces.{_ext}",
                "dest_path": "deces/",
                "dest_name": f"deces.{_ext}",
            }
            for _ext in ["csv", "parquet"]
        ],
        ignore_airflow_env=True,
    )


def publish_on_datagouv(ti):
    min_date = ti.xcom_pull(key="min_date", task_ids="gather_data")
    max_date = ti.xcom_pull(key="max_date", task_ids="gather_data")
    # ICI better names
    with open(f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}deces/config/dgv.json") as fp:
        data = json.load(fp)
    for _ext in ["csv", "parquet"]:
        post_remote_resource(
            dataset_id=data[f"deces_{_ext}"][AIRFLOW_ENV]["dataset_id"],
            resource_id=data[f"deces_{_ext}"][AIRFLOW_ENV]["resource_id"],
            payload={
                "url": (
                    f"https://object.files.data.gouv.fr/{MINIO_BUCKET_DATA_PIPELINE_OPEN}"
                    f"/{AIRFLOW_ENV}/deces/deces.{_ext}"
                ),
                "filesize": os.path.getsize(DATADIR + f"/deces.{_ext}"),
                "title": f"Décès de français.es entre {min_date} et {max_date} (format {_ext})",
                "format": _ext,
                "description": (
                    f"Décès de français.es entre {min_date} et {max_date} (format {_ext})"
                    " (créé à partir des [fichiers de l'INSEE]"
                    "(https://www.data.gouv.fr/fr/datasets/5de8f397634f4164071119c5/))"
                ),
            },
        )
