from airflow.hooks.base import BaseHook
from dag_datagouv_data_pipelines.config import (
    AIRFLOW_DAG_HOME,
    AIRFLOW_DAG_TMP,
    DATAGOUV_SECRET_API_KEY,
    AIRFLOW_ENV,
    MINIO_URL,
    MINIO_BUCKET_DATA_PIPELINE_OPEN,
    SECRET_MINIO_DATA_PIPELINE_USER,
    SECRET_MINIO_DATA_PIPELINE_PASSWORD,
)
from dag_datagouv_data_pipelines.utils.postgres import (
    execute_sql_file,
    copy_file
)
from dag_datagouv_data_pipelines.utils.datagouv import post_resource
from dag_datagouv_data_pipelines.utils.mattermost import send_message
from dag_datagouv_data_pipelines.utils.minio import send_files
import gc
import glob
from unidecode import unidecode
import numpy as np
import os
import pandas as pd
import requests
import json
from itertools import chain

DAG_FOLDER = "dag_datagouv_data_pipelines/data_processing/"
DATADIR = f"{AIRFLOW_DAG_TMP}elections/data"


def format_election_files_func():
    files = [f for f in os.listdir(DATADIR) if '.txt' if f]
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
            [[c + '_' + str(k) for c in candidat_columns] for k in range(nb_candidats)]
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


def process_election_data_func():
    def strip_zeros(s):
        k = 0
        while s[k] == '0':
            k += 1
        return s[k:]

    def process_id_bv(id_bv):
        try:
            if id_bv.count('_') == 1:
                insee, suffix = id_bv.split('_')
                suffix = suffix.replace(' ', '')
                suffix = suffix.split('-')[0]
                suffix = ''.join([c for c in suffix if c.isnumeric()])
                suffix = strip_zeros(suffix)
                return '_'.join([insee, suffix])
            else:
                return id_bv
        except:
            return id_bv

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
    elections = pd.DataFrame(None)
    for file in files:
        df = pd.read_csv(
            DATADIR + '/' + file,
            sep=';',
            dtype=str,
        )
        df['election'] = file.replace('.csv', '')
        elections = pd.concat([elections, df])
    nb_candidats_max = sum(['Panneau' in c for c in elections.columns])
    elections['code_dep'] = elections['Code du département'].apply(lambda x: map_outremer.get(x, x))
    elections['code_insee'] = elections['code_dep'].str.slice(0, 2) + \
        elections['Code de la commune']
    elections['id_bv'] = elections['code_insee'] + '_' + \
        elections['Code du b.vote'].apply(strip_zeros)
    to_cast = [
        'Inscrits', 'Abstentions', 'Blancs', 'Nuls', 'Exprimés'
    ] + [f'Voix_{k}' for k in range(nb_candidats_max)]
    for c in to_cast:
        elections[c] = elections[c].astype(float)
    for c in elections.columns:
        if c.startswith('%'):
            elections[c] = elections[c].apply(
                lambda s: float(s.replace(',', '.')) if isinstance(s, str) else s
            )
    print(elections['election'].value_counts())
    print(elections.sample(20))
