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


def format_election_files():
    files = [f for f in os.listdir(DATADIR) if '.txt' if f]












def process_():
    def strip_zeros(s):
        k = 0
        while s[k] == '0':
            k += 1
        return s[k:]

    t2 = pd.read_csv(
        DATADIR + "/t2.csv",
        sep=';',
        dtype=str,
        encoding='ansi'
    )
    presidentielle = t2.reset_index()
    nb_candidats = (len(presidentielle.columns) - len(t2.columns)) // 7 + 1
    # le fichier source ne contient pas les intitulés de toutes les colonnes
    # les 7 dernières colonnes sont identiques pour tous les candidats
    # on les ajoute, avec un incrément pour pouvoir les différencier
    presidentielle.columns = list(t2.columns)[:-7] + list(
        chain.from_iterable(
            [[c + '_' + str(k) for c in list(t2.columns[-7:])] for k in range(nb_candidats)]
        )
    )
    presidentielle['code_insee'] = presidentielle['Code du département'] + \
        presidentielle['Code de la commune']
    presidentielle['id_bv'] = presidentielle['code_insee'] + '_' + \
        presidentielle['Code du b.vote'].apply(strip_zeros)
    to_cast = [
        'Inscrits', 'Abstentions', 'Blancs', 'Nuls', 'Exprimés'
    ] + [f'Voix_{k}' for k in range(nb_candidats)]
    for c in to_cast:
        presidentielle[c] = presidentielle[c].astype(int)
    for c in presidentielle.columns:
        if c.startwith('%'):
            presidentielle[c] = presidentielle[c].apply(lambda s: float(s.replace(',', '.')))