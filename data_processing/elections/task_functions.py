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
    post_remote_resource,
    get_all_from_api_query,
    DATAGOUV_URL
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
        df['id_brut_miom'] = df['Code du département'] + df['Code de la commune'] + '_' + df['Code du b.vote']
        # getting where the candidates results start
        threshold = np.argwhere(['Panneau' in c for c in df.columns])[0][0]
        results['general'].append(df[['id_election', 'id_brut_miom'] + list(df.columns[:threshold])])
        print('- Done with general data')

        cols_ref = ['id_election', 'id_brut_miom', 'Code du département', 'Code de la commune', 'Code du b.vote']
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
    results['general'].to_csv(
        DATADIR + '/general_results.csv',
        sep=';',
        index=False
    )
    # removing rows created from empty columns due to uneven canditates number
    results['candidats'] = pd.concat(results['candidats'], ignore_index=True).dropna(subset=['Voix'])
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
        'https://www.data.gouv.fr/api/1/datasets/community_resources/?organization=646b7187b50b2a93b1ae3d45&sort=-created_at_internal'
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
            # concatenating all files (first one has header)
            df.to_csv(
                DATADIR + f'/{t}_results.csv',
                sep=';',
                index=False,
                mode="a",
                header=False
            )
            del df


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
    post_remote_resource(
        api_key=DATAGOUV_SECRET_API_KEY,
        remote_url=f"https://object.files.data.gouv.fr/{MINIO_BUCKET_DATA_PIPELINE_OPEN}/{AIRFLOW_ENV}/elections/general_results.csv",
        dataset_id=data["general"][AIRFLOW_ENV]["dataset_id"],
        resource_id=data["general"][AIRFLOW_ENV]["resource_id"],
        filesize=os.path.getsize(os.path.join(DATADIR, "general_results.csv")),
        title="Résultats généraux",
        format="csv",
        description=f"Résultats généraux des élections agrégés au niveau des bureaux de votes, créés à partir des données du Ministère de l'Intérieur (dernière modification : {datetime.today()})",
    )
    print('Done with general results')
    post_remote_resource(
        api_key=DATAGOUV_SECRET_API_KEY,
        remote_url=f"https://object.files.data.gouv.fr/{MINIO_BUCKET_DATA_PIPELINE_OPEN}/{AIRFLOW_ENV}/elections/candidats_results.csv",
        dataset_id=data["candidats"][AIRFLOW_ENV]["dataset_id"],
        resource_id=data["candidats"][AIRFLOW_ENV]["resource_id"],
        filesize=os.path.getsize(os.path.join(DATADIR, "candidats_results.csv")),
        title="Résultats par candidat",
        format="csv",
        description=f"Résultats des élections par candidat agrégés au niveau des bureaux de votes, créés à partir des données du Ministère de l'Intérieur (dernière modification : {datetime.today()})",
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
