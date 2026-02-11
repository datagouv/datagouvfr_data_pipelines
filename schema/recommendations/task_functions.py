import json
import logging
import yaml

from airflow.decorators import task
import jsonschema
import requests

from datagouvfr_data_pipelines.config import AIRFLOW_DAG_TMP
from datagouvfr_data_pipelines.utils.datagouv import local_client

DAG_NAME = "schema_recommendations"
TMP_FOLDER = f"{AIRFLOW_DAG_TMP}{DAG_NAME}/"
DATA_GOUV_API = "https://www.data.gouv.fr/api/1/"
RECOMMENDATION_SCORE = 30
CONFIG_CONSOLIDATION = "https://raw.githubusercontent.com/datagouv/schema.data.gouv.fr/refs/heads/main/config_consolidation.yml"
JSONSCHEMA_URL = "https://raw.githubusercontent.com/opendatateam/udata-recommendations/master/udata_recommendations/schema.json"


def consolidated_schemas() -> dict[str, str]:
    """Find TableSchema schemas that are consolidated"""
    config: dict = yaml.safe_load(requests.get(CONFIG_CONSOLIDATION).content)
    return {
        name: params["consolidated_dataset_id"]
        for name, params in config.items()
        if params.get("consolidated_dataset_id")
    }


def datasets_for_schema(schema: str) -> list[str]:
    """Fetch datasets on datagouv with the schema attribute set to a specific value"""
    r = local_client.get_all_from_api_query(
        base_query=f"api/1/datasets/?schema={schema}", mask="data{slug}"
    )
    return [d["slug"] for d in r]


def build_recommendation(
    consolidated_slug: str, dataset_slug: str, schema_name: str
) -> dict:
    return {
        "id": dataset_slug,
        "recommendations": [{"id": consolidated_slug, "score": RECOMMENDATION_SCORE}],
        "type": "dataset",
        "reason": schema_name,
    }


def validate_recommendations(recommendations: list) -> None:
    """ " Validate recommendations according to the JSON schema"""
    r = requests.get(JSONSCHEMA_URL, timeout=10)
    r.raise_for_status()
    schema = r.json()
    jsonschema.validate(recommendations, schema=schema)


@task()
def create_and_export_recommendations() -> None:
    recommendations = []
    for schema_id, consolidated_dataset_id in consolidated_schemas().items():
        consolidated_slug = requests.get(
            f"{DATA_GOUV_API}datasets/{consolidated_dataset_id}/"
        ).json()["slug"]
        logging.info(
            f"Working on schema {schema_id}, consolidated at {consolidated_dataset_id}"
        )
        dataset_ids = datasets_for_schema(schema_id)
        print(f"Found {len(dataset_ids)} associated with schema {schema_id}")
        recommendations += [
            build_recommendation(
                consolidated_slug=consolidated_slug,
                dataset_slug=d,
                schema_name=schema_id,
            )
            for d in dataset_ids
        ]
    validate_recommendations(recommendations)
    with open(TMP_FOLDER + "/recommendations.json", "w") as fp:
        json.dump(recommendations, fp, indent=2)
