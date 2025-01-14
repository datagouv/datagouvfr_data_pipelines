import json
import requests
import jsonschema

from datagouvfr_data_pipelines.config import AIRFLOW_DAG_TMP
from datagouvfr_data_pipelines.utils.datagouv import get_all_from_api_query

DAG_NAME = "schema_recommendations"
TMP_FOLDER = f"{AIRFLOW_DAG_TMP}{DAG_NAME}/"
DATA_GOUV_API = "https://www.data.gouv.fr/api/1"
RECOMMENDATION_SCORE = 50
CATALOG_SCHEMAS = 'https://schema.data.gouv.fr/schemas/schemas.json'
JSONSCHEMA_URL = "https://raw.githubusercontent.com/opendatateam/udata-recommendations/master/udata_recommendations/schema.json"


def consolidated_schemas():
    """Find TableSchema schemas that are consolidated"""
    r = requests.get(CATALOG_SCHEMAS)
    schemas = r.json()['schemas']
    return {
        s['name']: s['consolidation_dataset_id']
        for s in schemas
        if s['consolidation_dataset_id'] and s['schema_type'] == 'tableschema'
    }


def datasets_for_schema(schema):
    """Fetch datasets on datagouv with the schema attribute set to a specific value"""
    url = f"{DATA_GOUV_API}/datasets?schema={schema}"
    r = get_all_from_api_query(
        base_query=url,
        mask='data{id}'
    )
    return [d['id'] for d in r]


def build_recommendation(consolidated_dataset_id, dataset_id):
    return {
        "id": dataset_id,
        "recommendations": [
            {"id": consolidated_dataset_id, "score": RECOMMENDATION_SCORE}
        ],
    }


def validate_recommendations(recommendations):
    """" Validate recommendations according to the JSON schema"""
    r = requests.get(JSONSCHEMA_URL, timeout=10)
    r.raise_for_status()
    schema = r.json()
    jsonschema.validate(recommendations, schema=schema)


def create_and_export_recommendations():
    recommendations = []
    for schema_id, schema_details in consolidated_schemas().items():
        consolidated_dataset_id = schema_details
        print(
            f"Working on schema {schema_id}, consolidated on {consolidated_dataset_id}"
        )

        dataset_ids = datasets_for_schema(schema_id)
        print(f"Found {len(dataset_ids)} associated with schema {schema_id}")

        recommendations.extend([
            build_recommendation(consolidated_dataset_id, d) for d in dataset_ids
        ])

    ids = []
    recommendations_clean = []
    for r in recommendations:
        if r["id"] not in ids:
            ids.append(r["id"])
            recommendations_clean.append(r)
    validate_recommendations(recommendations_clean)
    with open(TMP_FOLDER + '/recommendations.json', 'w') as fp:
        json.dump(recommendations_clean, fp, indent=2)
