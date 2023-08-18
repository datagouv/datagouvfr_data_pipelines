from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_HOME,
    AIRFLOW_DAG_TMP,
    DATAGOUV_SECRET_API_KEY,
    AIRFLOW_ENV,
)
from datagouvfr_data_pipelines.utils.datagouv import post_resource
from datagouvfr_data_pipelines.utils.mattermost import send_message
import os
from rdflib import Dataset
import pandas as pd
from functools import reduce
import json
from datetime import datetime

DAG_FOLDER = "datagouvfr_data_pipelines/data_processing/"
DATADIR = f"{AIRFLOW_DAG_TMP}geozones/data"


def process_geozones():
    file = [f for f in os.listdir(DATADIR + '/cog') if '.trig' in f][0]
    graph = Dataset()
    print(os.path.join(DATADIR, 'cog', file))
    print("Opening file...")
    graph.parse(os.path.join(DATADIR, 'cog', file))
    print("Done!")

    data = []
    for subj, pred, obj, context in graph.quads():
        data.append({
            'subj': subj,
            'pred': pred,
            'obj': obj,
            'context': context,
        })
    df = pd.DataFrame(data)

    df['geo_type'] = df['subj'].apply(lambda s: s.split('geo/')[1].split('/')[0])
    df['pred_clean'] = df['pred'].apply(lambda s: s.split('#')[1])

    to_process = [
        "commune",
        "communeDeleguee",
        "communeAssociee",
        "arrondissement",
        "departement",
        "pays",
        "region",
        # "territoireFrancais",
    ]
    to_keep = ['nom', 'nomSansArticle', 'codeINSEE', 'codeArticle']
    dfs = []
    for geo_type in to_process:
        restr = df.loc[df['geo_type'] == geo_type]
        tmp = {}
        tmp[to_keep[0]] = restr.loc[restr['pred_clean'] == to_keep[0], ['subj', 'obj']]
        tmp[to_keep[0]].columns = ['uri', to_keep[0]]
        for pred_type in to_keep[1:]:
            if pred_type in restr['pred_clean'].unique():
                # on s'assure qu'il y a le même nombre de lignes pour le merge
                if restr['pred_clean'].value_counts().loc[pred_type] == len(tmp[to_keep[0]]):
                    tmp[pred_type] = restr.loc[(restr['pred_clean'] == pred_type), ['subj', 'obj']]
                    tmp[pred_type].columns = ['uri', pred_type]
        tmp = reduce(lambda x, y: pd.merge(x, y, on='uri', how='outer'), list(tmp.values()))
        if "codeINSEE" in tmp.columns:
            tmp = tmp.sort_values('codeINSEE')
        tmp['type'] = geo_type
        dfs.append(tmp)
        print("Done with", geo_type, ':', len(tmp))

    final = pd.concat(dfs)
    # les colonnes sont de type rdflib.term.Literal
    for c in final.columns:
        final[c] = final[c].apply(str)

    map_type = {
        'commune': 80,
        'communeDeleguee': 80,
        'communeAssociee': 80,
        'arrondissement': 70,
        'departement': 60,
        'pays': 20,
        'region': 40
    }
    final['level'] = final['type'].apply(lambda x: map_type[x])

    export = {'data': json.loads(final.to_json(orient='records'))}
    with open(DATADIR + '/export_geozones.json', 'w', encoding='utf8') as f:
        json.dump(export, f, ensure_ascii=False, indent=4)
    return


def post_geozones():
    with open(f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}geozones/config/dgv.json") as fp:
        data = json.load(fp)
    file_to_upload = {
        "dest_path": f"{DATADIR}/",
        "dest_name": "export_geozones.json",
    }
    payload = {
        "description": f"Géozones créées à partir du [fichier de l'INSEE](https://rdf.insee.fr/geo/index.html) ({datetime.today()})",
        "filesize": os.path.getsize(os.path.join(DATADIR + '/export_geozones.json')),
        "mime": "application/json",
        "title": f"Géozones ({datetime.today()})",
        "type": "main",
    }
    post_resource(
        api_key=DATAGOUV_SECRET_API_KEY,
        file_to_upload=file_to_upload,
        dataset_id=data['geozones'][AIRFLOW_ENV]['dataset_id'],
        resource_id=None,
        resource_payload=payload
    )
    return
