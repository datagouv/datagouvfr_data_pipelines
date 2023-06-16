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
from datagouvfr_data_pipelines.utils.datagouv import (
    post_resource,
    get_all_from_api_query,
    DATAGOUV_URL,
    ORGA_REFERENCE
)
from datagouvfr_data_pipelines.utils.mattermost import send_message
from datagouvfr_data_pipelines.utils.minio import send_files
import numpy as np
import os
import pandas as pd
import json
from itertools import chain
from datetime import datetime

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

    files = [f for f in os.listdir(DATADIR) if '.csv' in f]
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
        df['code_dep'] = df['Code du département'].apply(lambda x: map_outremer.get(x, x))
        df['code_insee'] = df['code_dep'].str.slice(0, 2) + df['Code de la commune']
        df['id_bv'] = df['code_insee'] + '_' + df['Code du b.vote'].apply(strip_zeros)
        # grapping where the candidates results start
        threshold = np.argwhere(['Panneau' in c for c in df.columns])[0][0]
        results['general'].append(df[['id_election', 'id_bv'] + list(df.columns[:threshold])])
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
            results['candidats'].append(candidats_group)
        print('- Done with candidats data')

    results['general'] = [pd.concat(results['general'], ignore_index=True)]
    # removing rows created from empty columns due to uneven canditates number
    results['candidats'] = [pd.concat(results['candidats'], ignore_index=True).dropna(subset=['Voix'])]

    # getting preprocessed resources
    resources = get_all_from_api_query(
        f'{DATAGOUV_URL}/api/1/datasets/community_resources/?organization={ORGA_REFERENCE}'
    )
    resources_url = {
        'general': [r['url'] for r in resources if 'general-results.csv' in r['url']],
        'candidats': [r['url'] for r in resources if 'candidats-results.csv' in r['url']]
    }

    for t in ['general', 'candidats']:
        print(t + ' preprocessed resources')
        for url in resources_url[t]:
            print(('- ' + url))
            df = pd.read_csv(url, sep=';', dtype=str)
            results[t].append(df)
        results[t] = pd.concat(results[t]).sort_values(by=['id_election', 'id_bv'])
        results[t].to_csv(DATADIR + f'/{t}_results.csv', index=False)


def send_results_to_minio():
    send_files(
        MINIO_URL=MINIO_URL,
        MINIO_BUCKET=MINIO_BUCKET_DATA_PIPELINE_OPEN,
        MINIO_USER=SECRET_MINIO_DATA_PIPELINE_USER,
        MINIO_PASSWORD=SECRET_MINIO_DATA_PIPELINE_PASSWORD,
        list_files=[
            {
                "source_path": f"{DATADIR}/",
                "source_name": f"{t}_results.csv",
                "dest_path": "elections/",
                "dest_name": f"{t}_results.csv",
            } for t in ['general', 'candidats']
        ],
    )


def publish_results_elections():
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
        resource_payload={
            'title': 'Résultats généraux',
            'description': f"Résultats généraux des élections agrégés au niveau des bureaux de votes, créés à partir des données du Ministère de l'Intérieur (dernière modification : {datetime.today()})",
        }
    )
    print('Done with general results')
    post_resource(
        api_key=DATAGOUV_SECRET_API_KEY,
        file_to_upload={
            "dest_path": f"{DATADIR}/",
            "dest_name": data["candidats"]["file"]
        },
        dataset_id=data["candidats"][AIRFLOW_ENV]["dataset_id"],
        resource_id=data["candidats"][AIRFLOW_ENV]["resource_id"],
        resource_payload={
            'title': 'Résultats par candidat',
            'description': f"Résultats des élections par candidat agrégés au niveau des bureaux de votes, créés à partir des données du Ministère de l'Intérieur (dernière modification : {datetime.today()})",
        }
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
