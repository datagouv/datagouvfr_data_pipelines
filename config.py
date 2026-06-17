import logging
import os


def _get(key, default=""):
    # In envs where Airflow db is unreachable (e.g withing notebooks), we can't access the variables
    # so we use ad hoc env vars
    try:
        from airflow.sdk import Variable

        return Variable.get(key, default=default)
    except Exception:
        logging.warning(
            f"Could not retrieve `{key}` from Airflow variables, loading from env"
        )
        return os.environ.get(key.upper(), default)


# Global
AIRFLOW_DAG_HOME = _get("AIRFLOW_DAG_HOME", "/opt/airflow/dags/")
AIRFLOW_DAG_TMP = _get("AIRFLOW_DAG_TMP", "/tmp/")
AIRFLOW_ENV = _get("AIRFLOW_ENV", "dev")
AIRFLOW_URL = _get("AIRFLOW_URL", "")

# Datagouv
DATAGOUV_SECRET_API_KEY = _get("DATAGOUV_SECRET_API_KEY", "")
DEMO_DATAGOUV_SECRET_API_KEY = _get("DEMO_DATAGOUV_SECRET_API_KEY", "")

# Tchap
TCHAP_BASE_URL = _get("TCHAP_BASE_URL", "")
TCHAP_BOT_TOKEN = _get("TCHAP_BOT_TOKEN", "")
TCHAP_ROOM_DATAENG = _get("TCHAP_ROOM_DATAENG", "")
TCHAP_ROOM_DATAENG_TEST = _get("TCHAP_ROOM_DATAENG_TEST", "")
TCHAP_ROOM_ACTIVITES = _get("TCHAP_ROOM_ACTIVITES", "")
TCHAP_ROOM_MODERATION_NOUVEAUTES = _get("TCHAP_ROOM_MODERATION_NOUVEAUTES", "")
TCHAP_ROOM_SIMPLIFIONS = _get("TCHAP_ROOM_SIMPLIFIONS", "")

# Minio (to be removed when migration is complete)
MINIO_URL = _get("MINIO_URL", "object.files.data.gouv.fr")

# S3
S3_URL_EU_WEST = _get("S3_URL_EU_WEST", "")
S3_URL_SBG = _get("S3_URL_SBG", "")
S3_URL_RBX = _get("S3_URL_RBX", "")
S3_BUCKET_DATA_PIPELINE = _get("S3_BUCKET_DATA_PIPELINE", "")
S3_BUCKET_DATA_PIPELINE_OPEN = _get("S3_BUCKET_DATA_PIPELINE_OPEN", "")
S3_BUCKET_INFRA = _get("S3_BUCKET_INFRA", "")
SECRET_S3_DATA_PIPELINE_USER = _get("SECRET_S3_DATA_PIPELINE_USER", "")
SECRET_S3_DATA_PIPELINE_PASSWORD = _get("SECRET_S3_DATA_PIPELINE_PASSWORD", "")
S3_BUCKET_PNT = _get("S3_BUCKET_PNT", "")
SECRET_S3_USER = _get("SECRET_S3_USER", "")
SECRET_S3_PASSWORD = _get("SECRET_S3_PASSWORD", "")

# INSEE
INSEE_BASE_URL = _get("INSEE_BASE_URL", "")
SECRET_INSEE_LOGIN = _get("SECRET_INSEE_LOGIN", "")
SECRET_INSEE_PASSWORD = _get("SECRET_INSEE_PASSWORD", "")

# Twitter
TWITTER_CONSUMER_KEY = _get("TWITTER_CONSUMER_KEY", "")
TWITTER_CONSUMER_KEY_SECRET = _get("TWITTER_CONSUMER_KEY_SECRET", "")
TWITTER_ACCESS_TOKEN = _get("TWITTER_ACCESS_TOKEN", "")
TWITTER_SECRET_TOKEN = _get("TWITTER_SECRET_TOKEN", "")

# emails
SECRET_MAIL_DATAGOUV_BOT_USER = _get("SECRET_MAIL_DATAGOUV_BOT_USER", "")
SECRET_MAIL_DATAGOUV_BOT_PASSWORD = _get("SECRET_MAIL_DATAGOUV_BOT_PASSWORD", "")
SECRET_MAIL_DATAGOUV_BOT_RECIPIENTS_PROD = _get(
    "SECRET_MAIL_DATAGOUV_BOT_RECIPIENTS_PROD", ""
).split(",")

# meteo
SECRET_FTP_METEO_USER = _get("SECRET_FTP_METEO_USER", "")
SECRET_FTP_METEO_PASSWORD = _get("SECRET_FTP_METEO_PASSWORD", "")
SECRET_FTP_METEO_ADDRESS = _get("SECRET_FTP_METEO_ADDRESS", "")
METEO_PNT_APPLICATION_ID = _get("METEO_PNT_APPLICATION_ID", "")

# notion
SECRET_NOTION_KEY_IMPACT = _get("SECRET_NOTION_KEY_IMPACT", "")

# grist
GRIST_API_URL = _get("GRIST_API_URL", "")
SECRET_GRIST_API_KEY = _get("SECRET_GRIST_API_KEY", "")

# sentry
SECRET_SENTRY_API_TOKEN = _get("SECRET_SENTRY_API_TOKEN", "")
SENTRY_BASE_URL = _get("SENTRY_BASE_URL", "")

# crisp
CRISP_WEBSITE_ID = _get("CRISP_WEBSITE_ID", "")
CRISP_PLUGIN_ID = _get("CRISP_PLUGIN_ID", "")
CRISP_PLUGIN_TOKEN = _get("CRISP_PLUGIN_TOKEN", "")
CRISP_USER_ID = _get("CRISP_USER_ID", "")
CRISP_USER_TOKEN = _get("CRISP_USER_TOKEN", "")

# matomo
MATOMO_TOKEN = _get("MATOMO_TOKEN", "")
