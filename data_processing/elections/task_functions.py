from airflow.hooks.base import BaseHook
from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_HOME,
    AIRFLOW_DAG_TMP,
    DATAGOUV_SECRET_API_KEY,
    AIRFLOW_ENV,
    MINIO_URL,
    MINIO_BUCKET_DATA_PIPELINE_OPEN,
    SECRET_MINIO_DATA_PIPELINE_USER,
    SECRET_MINIO_DATA_PIPELINE_PASSWORD,
)
from datagouvfr_data_pipelines.utils.datagouv import post_resource, DATAGOUV_URL
from datagouvfr_data_pipelines.utils.mattermost import send_message
from datagouvfr_data_pipelines.utils.minio import send_files
import numpy as np
import os
import pandas as pd
import json
from itertools import chain

DAG_FOLDER = "datagouvfr_data_pipelines/data_processing/"
DATADIR = f"{AIRFLOW_DAG_TMP}elections/data"


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
    def strip_zeros(s):
        k = 0
        while s[k] == '0':
            k += 1
        return s[k:]

    map_outremer = {
        'ZD': '974',
        'ZA': '971',
        'ZB': '972',
        'ZN': '988',
        'ZP': '987',
        'ZZ': 'ZZ',
        'ZM': '976',
        'ZC': '973',
        'ZX': '978/977',
        'ZS': '975',
        'ZW': '986',
    }

    files = [f for f in os.listdir(DATADIR) if '.csv' if f]
    print(files)
    general_stats = []
    candidats_stats = []
    for file in files:
        print('Starting', file)
        df = pd.read_csv(
            DATADIR + '/' + file,
            sep=';',
            dtype=str,
        )
        df['id_election'] = file.replace('.csv', '')
        df['code_dep'] = df['Code du département'].apply(lambda x: map_outremer.get(x, x))
        df['code_insee'] = df['code_dep'].str.slice(0, 2) + df['Code de la commune']
        df['id_bv'] = df['code_insee'] + '_' + df['Code du b.vote'].apply(strip_zeros)
        # grapping where the candidates results start
        threshold = np.argwhere(['Panneau' in c for c in df.columns])[0][0]
        general_stats.append(df[['id_election', 'id_bv'] + list(df.columns[:threshold])])
        print('- Done with general data')

        cols_ref = ['id_election', 'id_bv', 'Code du département', 'Code de la commune', 'Code du b.vote']
        tmp_candidats = df[
            # -5 because we are adding 4 columns above
            cols_ref + list(df.columns[threshold:-5])
        ]
        nb_candidats = sum(['Panneau' in i for i in tmp_candidats.columns])
        tmp_cols = tmp_candidats.columns[len(cols_ref):]
        candidat_columns = [c.split('_')[0] for c in tmp_cols[:len(tmp_cols) // nb_candidats]]
        for cand in range(1, nb_candidats + 1):
            candidats_group = tmp_candidats[cols_ref + [c + f'_{cand}' for c in candidat_columns]]
            candidats_group.columns = cols_ref + candidat_columns
            candidats_stats.append(candidats_group)
        print('- Done with candidates data')

    general_stats = pd.concat(general_stats, ignore_index=True)
    candidats_stats = pd.concat(candidats_stats, ignore_index=True)
    # removing rows created from empty columns due to uneven canditates number
    candidats_stats = candidats_stats.dropna(subset=['Voix'])
    candidats_stats = candidats_stats.sort_values(by=['id_election', 'id_bv'])
    general_stats.to_csv(DATADIR + '/general_stats.csv', index=False)
    candidats_stats.to_csv(DATADIR + '/candidats_stats.csv', index=False)


def send_stats_to_minio():
    send_files(
        MINIO_URL=MINIO_URL,
        MINIO_BUCKET=MINIO_BUCKET_DATA_PIPELINE_OPEN,
        MINIO_USER=SECRET_MINIO_DATA_PIPELINE_USER,
        MINIO_PASSWORD=SECRET_MINIO_DATA_PIPELINE_PASSWORD,
        list_files=[
            {
                "source_path": f"{DATADIR}/",
                "source_name": "general_stats.csv",
                "dest_path": "elections/",
                "dest_name": "general_stats.csv",
            },
            {
                "source_path": f"{DATADIR}/",
                "source_name": "candidats_stats.csv",
                "dest_path": "elections/",
                "dest_name": "candidats_stats.csv",
            },
        ],
    )


def publish_stats_elections():
    with open(f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}elections/config/dgv.json") as fp:
        data = json.load(fp)
    post_resource(
        api_key=DATAGOUV_SECRET_API_KEY,
        file_to_upload={
            "dest_path": f"{DATADIR}/",
            "dest_name": data["general"]["file"]
        },
        dataset_id=data["general"][AIRFLOW_ENV]["dataset_id"],
        resource_id=data["general"][AIRFLOW_ENV]["resource_id"],
    )
    print('Done with general stats')
    post_resource(
        api_key=DATAGOUV_SECRET_API_KEY,
        file_to_upload={
            "dest_path": f"{DATADIR}/",
            "dest_name": data["candidats"]["file"]
        },
        dataset_id=data["candidats"][AIRFLOW_ENV]["dataset_id"],
        resource_id=data["candidats"][AIRFLOW_ENV]["resource_id"],
    )


def send_notification():
    with open(f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}elections/config/dgv.json") as fp:
        data = json.load(fp)
    send_message(
        text=(
            ":mega: Données élections mises à jour.\n"
            f"- Données stockées sur Minio - Bucket {MINIO_BUCKET_DATA_PIPELINE_OPEN}"
            f"- Données référencées [sur data.gouv.fr]({DATAGOUV_URL}/fr/datasets/"
            f"{data['general'][AIRFLOW_ENV]['dataset_id']})"
        )
    )