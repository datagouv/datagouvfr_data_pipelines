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

DAG_FOLDER = "datagouvfr_data_pipelines/data_processing/"
VALIDATA_BASE_URL = (
    "https://validata-api.app.etalab.studio/validate?schema={schema_url}&url={rurl}"
)
schema_name = "etalab/schema-irve-statique"










