import os
import pandas as pd
import json
from datetime import datetime
import requests
from io import BytesIO
from urllib.parse import quote_plus, urlencode

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
    AIRFLOW_ENV,
)
from datagouvfr_data_pipelines.utils.datagouv import local_client, DATAGOUV_URL
from datagouvfr_data_pipelines.utils.filesystem import File
from datagouvfr_data_pipelines.utils.mattermost import send_message

DAG_FOLDER = "datagouvfr_data_pipelines/data_processing/"
DATADIR = f"{AIRFLOW_DAG_TMP}geozones/data/"
dataset_id = "554210a9c751df2666a7b26c" if AIRFLOW_ENV == "prod" else "64bfc429d6e029048e577d3e"
dataset = local_client.dataset(dataset_id)
geozones_file = File(
    source_path=DATADIR,
    source_name="export_geozones.json",
)
countries_file = File(
    source_path=DATADIR,
    source_name="export_countries.json",
)
levels_file = File(
    source_path=DATADIR,
    source_name="export_levels.json",
)


def download_and_process_geozones():
    endpoint = "https://rdf.insee.fr/sparql?query="
    query = """PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX rdfschema:<http://www.w3.org/2000/01/rdf-schema#>
    PREFIX igeo:<http://rdf.insee.fr/def/geo#>

    SELECT *
    WHERE {
        ?zone rdf:type ?territory .
        ?zone igeo:nom ?nom .
        ?zone igeo:codeINSEE ?codeINSEE .
        OPTIONAL {
            ?zone igeo:nomSansArticle ?nomSansArticle .
            ?zone igeo:codeArticle ?codeArticle .
            ?suppression_evt igeo:suppression ?zone .
        }
    }"""
    params = {
        'format': 'application/csv;grid=true',
    }
    url = endpoint + quote_plus(query, safe='*') + '&' + urlencode(params)
    print(url)
    r = requests.get(url)
    bytes_io = BytesIO(r.content)
    df = pd.read_csv(bytes_io)
    df['type'] = df['territory'].apply(lambda x: x.split('#')[1])
    df['is_deleted'] = df['suppression_evt'].apply(lambda s: isinstance(s, str))
    for c in df.columns:
        if c != 'is_deleted':
            df[c] = df[c].apply(str)
    map_type = {
        "Etat": "country",
        "Region": "fr:region",
        "Departement": "fr:departement",
        "CollectiviteDOutreMer": "fr:departement",
        "Intercommunalite": "fr:epci",
        "Arrondissement": "fr:arrondissement",
        "ArrondissementMunicipal": "fr:arrondissement",
        "Commune": "fr:commune",
        "CommuneDeleguee": "fr:commune",
        "CommuneAssociee": "fr:commune",
    }
    df = df.loc[df['type'].isin(map_type)]
    df['level'] = df['type'].apply(lambda x: map_type.get(x, x))
    df['_id'] = df['level'] + ':' + df['codeINSEE']
    df = df.rename({"zone": "uri"}, axis=1)
    df = df.drop(['territory', 'suppression_evt'], axis=1)
    df = df.loc[
        (df['type'] != 'Arrondissement') |
        (
            (df['type'] == 'Arrondissement') &
            (df['nom'].str.contains('|'.join(['Paris', 'Lyon', 'Marseille'])))
        )
    ]
    df = df.loc[
        df['level'] != 'country'
    ]

    # get countries (with ISO alpha 2 code) from another source
    countries = pd.read_csv(
        "https://www.data.gouv.fr/fr/datasets/r/2b38f28d-15e7-4f0c-b61d-6ca1d9b1cfa2",
        sep=';',
        encoding='cp1252',
        dtype=str
    )
    countries = countries.loc[countries['SOUVERAIN'] == 'O']
    countries['uri'] = countries['CODE_COG'].apply(
        lambda x: "http://id.insee.fr/geo/etat/" + x if isinstance(x, str) else x
    )
    countries.rename({
        'NOM_COURT': 'nom',
    }, axis=1, inplace=True)
    countries['nomSansArticle'] = countries['nom']
    countries['codeArticle'] = None
    countries['type'] = 'country'
    countries['is_deleted'] = False
    countries['level'] = 'country'
    countries['codeINSEE'] = countries['ISO_alpha2'].apply(
        lambda x: x.lower() if isinstance(x, str) else x
    )
    countries['_id'] = countries['ISO_alpha2'].apply(
        lambda x: "country:" + x.lower() if isinstance(x, str) else x
    )
    countries = countries[
        ['uri', 'nom', 'codeINSEE', 'nomSansArticle', 'codeArticle', 'type', 'is_deleted', 'level', '_id']
    ]
    countries_json = json.loads(countries.to_json(orient='records'))

    export = json.loads(df.to_json(orient='records'))
    export.extend([
        {
            "uri": "http://id.insee.fr/geo/world",
            "nom": "Monde",
            "codeINSEE": "world",
            "nomSansArticle": "Monde",
            "codeArticle": None,
            "type": "country-group",
            "is_deleted": False,
            "level": "country-group",
            "_id": "country-group:world"
        },
        {
            "uri": "http://id.insee.fr/geo/europe",
            "nom": "Union Européenne",
            "codeINSEE": "ue",
            "nomSansArticle": "Union Européenne",
            "codeArticle": None,
            "type": "country-group",
            "is_deleted": False,
            "level": "country-group",
            "_id": "country-group:ue"
        },
        {
            "uri": None,
            "nom": "DROM",
            "codeINSEE": "fr:drom",
            "nomSansArticle": "DROM",
            "codeArticle": None,
            "type": "country-subset",
            "is_deleted": False,
            "level": "country-subset",
            "_id": "country-subset:fr:drom"
        },
        {
            "uri": None,
            "nom": "DROM-COM",
            "codeINSEE": "fr:dromcom",
            "nomSansArticle": "DROM-COM",
            "codeArticle": None,
            "type": "country-subset",
            "is_deleted": False,
            "level": "country-subset",
            "_id": "country-subset:fr:dromcom"
        },
        {
            "uri": None,
            "nom": "France métropolitaine",
            "codeINSEE": "fr:metro",
            "nomSansArticle": "France métropolitaine",
            "codeArticle": None,
            "type": "country-subset",
            "is_deleted": False,
            "level": "country-subset",
            "_id": "country-subset:fr:metro"
        },
    ])
    export.extend(countries_json)
    for geoz in export:
        for c in geoz.keys():
            if geoz[c] == 'nan':
                geoz[c] = None
    os.mkdir(DATADIR)
    with open(geozones_file.full_source_path, 'w', encoding='utf8') as f:
        json.dump(export, f, ensure_ascii=False, indent=4)

    with open(countries_file.full_source_path, 'w', encoding='utf8') as f:
        json.dump(
            countries_json,
            f, ensure_ascii=False, indent=4
        )

    levels = [
        {"id": "country-group", "label": "Country group", "admin_level": 10, "parents": []},
        {"id": "country", "label": "Country", "admin_level": 20, "parents": ["country-group"]},
        {"id": "country-subset", "label": "Country subset", "admin_level": 30, "parents": ["country"]},
        {"id": "fr:region", "label": "French region", "admin_level": 40, "parents": ["country"]},
        {"id": "fr:departement", "label": "French county", "admin_level": 60, "parents": ["fr:region"]},
        {"id": "fr:arrondissement", "label": "French arrondissement", "admin_level": 70, "parents": ["fr:departement"]},
        {"id": "fr:commune", "label": "French town", "admin_level": 80, "parents": ["fr:arrondissement", "fr:epci"]},
        {"id": "fr:iris", "label": "Iris (Insee districts)", "admin_level": 98, "parents": ["fr:commune"]},
        {"id": "fr:canton", "label": "French canton", "admin_level": 98, "parents": ["fr:departement"]},
        {"id": "fr:collectivite", "label": "French overseas collectivities", "admin_level": 60, "parents": ["fr:region"]},
        {"id": "fr:epci", "label": "French intermunicipal (EPCI)", "admin_level": 68, "parents": ["country"]}
    ]
    with open(levels_file.full_source_path, 'w', encoding='utf8') as f:
        json.dump(
            levels,
            f, ensure_ascii=False, indent=4
        )


def post_geozones():
    year = datetime.now().strftime('%Y')
    dataset.create_static(
        file_to_upload=geozones_file.full_source_path,
        payload={
            "description": (
                "Géozones créées à partir du [fichier de l'INSEE]"
                "(https://rdf.insee.fr/geo/index.html)"
            ),
            "filesize": os.path.getsize(geozones_file.full_source_path),
            "mime": "application/json",
            "title": f"Zones {year} (json)",
            "type": "main",
        },
    )

    dataset.create_static(
        file_to_upload=countries_file,
        payload={
            "description": (
                "Géozones (pays uniquement) créées à partir du [Référentiel des pays et des territoires]"
                "(https://www.data.gouv.fr/fr/datasets/64959ecae2bdc5448631a59c/)"
            ),
            "filesize": os.path.getsize(countries_file.full_source_path),
            "mime": "application/json",
            "title": f"Zones pays uniquement {year} (json)",
            "type": "main",
        }
    )

    dataset.create_static(
        file_to_upload=levels_file,
        payload={
            "filesize": os.path.getsize(levels_file.full_source_path),
            "mime": "application/json",
            "title": f"Niveaux {year} (json)",
            "type": "main",
        },
    )


def notification_mattermost():
    message = "Données Géozones mises à jours [ici]"
    message += f"({DATAGOUV_URL}/fr/datasets/{dataset_id})"
    send_message(message)
