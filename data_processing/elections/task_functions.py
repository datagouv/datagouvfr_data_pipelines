from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_HOME,
    AIRFLOW_DAG_TMP,
    AIRFLOW_ENV,
    MINIO_BUCKET_DATA_PIPELINE_OPEN,
)
from datagouvfr_data_pipelines.utils.datagouv import (
    post_remote_resource,
    get_all_from_api_query,
    DATAGOUV_URL
)
from datagouvfr_data_pipelines.utils.mattermost import send_message
from datagouvfr_data_pipelines.utils.minio import MinIOClient
from datagouvfr_data_pipelines.utils.utils import csv_to_parquet
from datagouvfr_data_pipelines.utils.download import download_files

import numpy as np
import os
import pandas as pd
import json
from itertools import chain
from datetime import datetime
import math
import requests
from bs4 import BeautifulSoup

DAG_FOLDER = "datagouvfr_data_pipelines/data_processing/"
DATADIR = f"{AIRFLOW_DAG_TMP}elections/data"
minio_open = MinIOClient(bucket=MINIO_BUCKET_DATA_PIPELINE_OPEN)
int_cols = {
    'general': ['Inscrits', 'Abstentions', 'Votants', 'Blancs', 'Nuls', 'Exprimés'],
    'candidats': ['N°Panneau', 'Voix'],
}

ID_CURRENT_ELECTION = "LG2024"


def num_converter(value, _type):
    if not isinstance(value, str) and math.isnan(value):
        return value
    try:
        return _type(value.replace(',', '.'))
    except:
        # print("erreur:", value)
        return ''


def process(df, int_cols):
    for c in df.columns:
        if "%" in c:
            # these columns are percentages
            df[c] = df[c].apply(lambda x: num_converter(x, float))
        elif any(c == k for k in int_cols):
            # these are numbers
            df[c] = df[c].apply(lambda x: num_converter(x, int))


def format_election_files():
    files = [f for f in os.listdir(DATADIR) if '.txt' in f]
    for f in files:
        this_file = DATADIR + '/' + f
        print('Processing ' + this_file)
        for encoding in ['cp1252', 'utf-8']:
            try:
                with open(this_file, 'r', encoding=encoding) as file:
                    content = file.readlines()
                    file.close()
                break
            except:
                pass
        max_nb_columns = max([row.count(';') for row in content]) + 1
        header = content[0].replace('\n', '').split(';')
        # specific rule for 2019_euro
        if "2019_euro" in f:
            print("Special process:", f)
            header[header.index('N°Liste')] = 'N°Panneau'
        # specific rule for 2020_muni_t2
        if "2020_muni_t2" in f:
            print("Special process:", f)
            header[header.index('Code B.Vote')] = 'Code du b.vote'
            header[header.index('Code Nuance')] = 'Nuance'
            header[header.index('N.Pan.')] = 'N°Panneau'
        hook_candidat_columns = np.argwhere(['Panneau' in c for c in header])[0][0]
        candidat_columns = header[hook_candidat_columns:]
        first_columns = header[:hook_candidat_columns]
        if (max_nb_columns - len(first_columns)) % len(candidat_columns) != 0:
            print('Fichier non valide')
            continue
        nb_candidats = (max_nb_columns - len(first_columns)) // len(candidat_columns)
        columns = first_columns + list(chain.from_iterable(
            [[c + '_' + str(k) for c in candidat_columns] for k in range(1, nb_candidats + 1)]
        ))
        output = ';'.join(columns) + '\n'
        for idx, row in enumerate(content[1:]):
            output += row.replace('\n', '') + ';' * (max_nb_columns - row.count(';') - 1) + '\n'
        with open(this_file.replace('.txt', '.csv'), 'w', encoding='utf-8') as new_file:
            new_file.write(output)
        try:
            pd.read_csv(
                this_file.replace('.txt', '.csv'),
                sep=';',
                dtype=str,
            )
            os.remove(this_file)
        except:
            print('Error in the file')
            os.remove(this_file.replace('.txt', '.csv'))


def process_election_data():
    files = [
        f for f in os.listdir(DATADIR)
        if '.csv' in f and f not in ['general_results.csv', 'candidats_results.csv']
    ]
    print(files)
    results = {'general': [], 'candidats': []}
    for file in files:
        print('Starting', file.split('.')[0])
        df = pd.read_csv(
            DATADIR + '/' + file,
            sep=';',
            dtype=str,
        )
        df['id_election'] = file.replace('.csv', '')
        df['id_brut_miom'] = (
            df['Code du département'] + df['Code de la commune'] + '_' + df['Code du b.vote']
        )
        # getting where the candidates results start
        threshold = np.argwhere(['Panneau' in c for c in df.columns])[0][0]
        results['general'].append(df[['id_election', 'id_brut_miom'] + list(df.columns[:threshold])])
        print('- Done with general data')

        cols_ref = [
            'id_election',
            'id_brut_miom',
            'Code du département',
            'Code de la commune',
            'Code du b.vote'
        ]
        tmp_candidats = df[
            # -2 because we are adding 2 columns above
            cols_ref + list(df.columns[threshold:-2])
        ]
        nb_candidats = sum(['Panneau' in i for i in tmp_candidats.columns])
        tmp_cols = tmp_candidats.columns[len(cols_ref):]
        candidat_columns = [c.split('_')[0] for c in tmp_cols[:len(tmp_cols) // nb_candidats]]
        for cand in range(1, nb_candidats + 1):
            candidats_group = tmp_candidats[cols_ref + [c + f'_{cand}' for c in candidat_columns]]
            candidats_group.columns = cols_ref + candidat_columns
            results['candidats'].append(candidats_group)
        print('- Done with candidats data')
        del df
        del tmp_candidats

    results['general'] = pd.concat(results['general'], ignore_index=True)
    process(
        df=results['general'],
        int_cols=int_cols['general'],
    )
    results['general'].to_csv(
        DATADIR + '/general_results.csv',
        sep=';',
        index=False
    )
    # removing rows created from empty columns due to uneven canditates number
    results['candidats'] = pd.concat(results['candidats'], ignore_index=True).dropna(subset=['Voix'])
    process(
        df=results['candidats'],
        int_cols=int_cols['candidats']
    )
    results['candidats'].to_csv(
        DATADIR + '/candidats_results.csv',
        sep=';',
        index=False
    )
    columns = {
        'general': results['general'].columns.to_list(),
        'candidats': results['candidats'].columns.to_list()
    }
    del results

    # getting preprocessed resources
    resources_url = [r['url'] for r in get_all_from_api_query(
        # due to https://github.com/MongoEngine/mongoengine/issues/2748
        # we have to specify a sort parameter for now
        'https://www.data.gouv.fr/api/1/datasets/community_resources/'
        '?organization=646b7187b50b2a93b1ae3d45&sort=-created_at_internal'
    )]
    resources = {
        'general': [r for r in resources_url if 'general-results.csv' in r],
        'candidats': [r for r in resources_url if 'candidats-results.csv' in r]
    }

    for t in ['general', 'candidats']:
        print(t + ' resources')
        for url in resources[t]:
            print('- ' + url)
            df = pd.read_csv(url, sep=';', dtype=str)
            # adding blank columns if some are missing from overall template
            for c in columns[t]:
                if c not in df.columns:
                    df[c] = ['' for k in range(len(df))]
            df = df[columns[t]]
            process(
                df=df,
                int_cols=int_cols[t],
            )
            # concatenating all files (first one has header)
            df.to_csv(
                DATADIR + f'/{t}_results.csv',
                sep=';',
                index=False,
                mode="a",
                header=False
            )
            del df
        print('> Export en parquet')
        dtype = {}
        for c in columns[t]:
            if "%" in c:
                dtype[c] = "FLOAT"
            elif any(c == k for k in int_cols[t]):
                dtype[c] = "INT32"
            else:
                dtype[c] = "VARCHAR"
        csv_to_parquet(
            csv_file_path=DATADIR + f'/{t}_results.csv',
            dtype=dtype,
        )


def send_results_to_minio():
    minio_open.send_files(
        list_files=[
            {
                "source_path": f"{DATADIR}/",
                "source_name": f"{t}_results.{ext}",
                "dest_path": "elections/",
                "dest_name": f"{t}_results.{ext}",
            } for t in ['general', 'candidats'] for ext in ["csv", "parquet"]
        ],
    )


def publish_results_elections():
    with open(f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}elections/config/dgv.json") as fp:
        data = json.load(fp)
    post_remote_resource(
        remote_url=(
            f"https://object.files.data.gouv.fr/{MINIO_BUCKET_DATA_PIPELINE_OPEN}"
            f"/{AIRFLOW_ENV}/elections/general_results.csv"
        ),
        dataset_id=data["general"][AIRFLOW_ENV]["dataset_id"],
        resource_id=data["general"][AIRFLOW_ENV]["resource_id"],
        filesize=os.path.getsize(os.path.join(DATADIR, "general_results.csv")),
        title="Résultats généraux",
        format="csv",
        description=(
            f"Résultats généraux des élections agrégés au niveau des bureaux de votes,"
            " créés à partir des données du Ministère de l'Intérieur"
            f" (dernière modification : {datetime.today()})"
        ),
    )
    print('Done with general results')
    post_remote_resource(
        remote_url=(
            f"https://object.files.data.gouv.fr/{MINIO_BUCKET_DATA_PIPELINE_OPEN}"
            f"/{AIRFLOW_ENV}/elections/candidats_results.csv"
        ),
        dataset_id=data["candidats"][AIRFLOW_ENV]["dataset_id"],
        resource_id=data["candidats"][AIRFLOW_ENV]["resource_id"],
        filesize=os.path.getsize(os.path.join(DATADIR, "candidats_results.csv")),
        title="Résultats par candidat",
        format="csv",
        description=(
            f"Résultats des élections par candidat agrégés au niveau des bureaux de votes,"
            " créés à partir des données du Ministère de l'Intérieur"
            f" (dernière modification : {datetime.today()})"
        ),
    )
    print('Done with candidats results')
    post_remote_resource(
        remote_url=(
            f"https://object.files.data.gouv.fr/{MINIO_BUCKET_DATA_PIPELINE_OPEN}"
            f"/{AIRFLOW_ENV}/elections/general_results.parquet"
        ),
        dataset_id=data["general_parquet"][AIRFLOW_ENV]["dataset_id"],
        resource_id=data["general_parquet"][AIRFLOW_ENV]["resource_id"],
        filesize=os.path.getsize(os.path.join(DATADIR, "general_results.parquet")),
        title="Résultats généraux (format parquet)",
        format="parquet",
        description=(
            f"Résultats généraux des élections agrégés au niveau des bureaux de votes,"
            " créés à partir des données du Ministère de l'Intérieur"
            f" (dernière modification : {datetime.today()})"
        ),
    )
    print('Done with general results parquet')
    post_remote_resource(
        remote_url=(
            f"https://object.files.data.gouv.fr/{MINIO_BUCKET_DATA_PIPELINE_OPEN}"
            f"/{AIRFLOW_ENV}/elections/candidats_results.parquet"
        ),
        dataset_id=data["candidats_parquet"][AIRFLOW_ENV]["dataset_id"],
        resource_id=data["candidats_parquet"][AIRFLOW_ENV]["resource_id"],
        filesize=os.path.getsize(os.path.join(DATADIR, "candidats_results.parquet")),
        title="Résultats par candidat (format parquet)",
        format="parquet",
        description=(
            f"Résultats des élections par candidat agrégés au niveau des bureaux de votes,"
            " créés à partir des données du Ministère de l'Intérieur"
            f" (dernière modification : {datetime.today()})"
        ),
    )


def send_notification():
    with open(f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}elections/config/dgv.json") as fp:
        data = json.load(fp)
    send_message(
        text=(
            ":mega: Données élections mises à jour.\n"
            f"- Données stockées sur Minio - Bucket {MINIO_BUCKET_DATA_PIPELINE_OPEN}\n"
            f"- Données référencées [sur data.gouv.fr]({DATAGOUV_URL}/fr/datasets/"
            f"{data['general'][AIRFLOW_ENV]['dataset_id']})"
        )
    )


def get_files_minio_mirroring(ti):
    minio_files = sorted(minio_open.get_files_from_prefix('elections-mirroring/' + ID_CURRENT_ELECTION + '/', ignore_airflow_env=False, recursive=True))
    print(minio_files)
    arr = []
    for mf in minio_files:
        arr.append(mf.replace("elections-mirroring/", ""))
    ti.xcom_push(key="minio_files", value=arr)


def parse_http_server(url_source, url, arr):
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        links = soup.find_all('a')
        for link in links:
            href = link.get('href')
            if href != '../':
                if href.endswith('/'):
                    new_url = url + href
                    arr = parse_http_server(url_source, new_url, arr)
                else:
                    # C'est un fichier, on le télécharge si pas présent localement
                    file_url = url + href
                    arr.append(file_url.replace(url_source, ''))
    return arr


def get_all_files_miom(ti):
    url = "https://www.resultats-elections.interieur.gouv.fr/telechargements/" + ID_CURRENT_ELECTION + "/"
    arr = []
    arr = parse_http_server(url, url, arr)
    ti.xcom_push(key="miom_files", value=arr)


def compare_minio_miom(ti):
    minio_files = ti.xcom_pull(key="minio_files", task_ids="get_files_minio_mirroring")
    miom_files = ti.xcom_pull(key="miom_files", task_ids="get_all_files_miom")
    compare_files = list(set(miom_files) - set(minio_files))
    ti.xcom_push(key="compare_files", value=compare_files)


def download_local_files(ti):
    compare_files = ti.xcom_pull(key="compare_files", task_ids="compare_minio_miom")
    arr = []
    for cf in compare_files:
        arr.append(
            {
                "url": "https://www.resultats-elections.interieur.gouv.fr/telechargements/" + ID_CURRENT_ELECTION + "/" + cf,
                "dest_path": f"{AIRFLOW_DAG_TMP}elections-mirroring/" + '/'.join(cf.split("/")[:-1]) + "/",
                "dest_name": cf.split("/")[-1]
            }
        )
    download_files(arr)

   
def send_to_minio(ti):
    compare_files = ti.xcom_pull(key="compare_files", task_ids="compare_minio_miom")
    arr = []
    for cf in compare_files:
        arr.append(
            {
                "source_path": f"{AIRFLOW_DAG_TMP}elections-mirroring/" + ID_CURRENT_ELECTION + '/'.join(cf.split("/")[:-1]) + "/",
                "source_name": cf.split("/")[-1],
                "dest_path": "elections-mirroring/" + '/'.join(cf.split("/")[:-1]) + "/",
                "dest_name": cf.split("/")[-1],
            }
        )

    minio_open.send_files(
        list_files=arr
    )