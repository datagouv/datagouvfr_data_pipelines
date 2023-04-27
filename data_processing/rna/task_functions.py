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
from datagouvfr_data_pipelines.utils.postgres import (
    execute_sql_file,
    copy_file
)
from datagouvfr_data_pipelines.utils.datagouv import post_remote_communautary_resource
from datagouvfr_data_pipelines.utils.mattermost import send_message
from datagouvfr_data_pipelines.utils.minio import send_files
import pandas as pd
import os
from datetime import datetime
from unidecode import unidecode
from csv_detective.explore_csv import routine

DAG_FOLDER = "dag_datagouv_data_pipelines/data_processing/"
DATADIR = f"{AIRFLOW_DAG_TMP}rna/data"
SQLDIR = f"{AIRFLOW_DAG_TMP}rna/sql"

conn = BaseHook.get_connection("postgres_localhost")


def process_rna():
    # concatenate all files
    df_rna = pd.DataFrame(None)
    for f in sorted(os.listdir(os.path.join(DATADIR, 'rna'))):
        print(f)
        _ = pd.read_csv(os.path.join(DATADIR, 'rna', f),
                        sep=';',
                        encoding='ISO-8859-1',
                        dtype=str)
        df_rna = pd.concat([df_rna, _])
    punc_to_remove = '!"#$%&\'()*+/;?@[]^_`{|}~'
    for c in df_rna.columns:
        df_rna[c] = df_rna[c].apply(
            lambda s: unidecode(s).translate(str.maketrans('', '', punc_to_remove))
            .encode('unicode-escape').decode().replace('\\', '')
            if isinstance(s, str) else s
        )
    # export to csv
    df_rna.to_csv(
        os.path.join(DATADIR, "base_rna.csv"),
        index=False,
        encoding='utf8'
    )
    # analyse csv file just created
    infos = routine(
        csv_file_path=os.path.join(DATADIR, "base_rna.csv"),
        num_rows=-1,
        encoding='utf8',
        verbose=True
    )
    # use analysis results to create the query that creates the table
    map_types = {
        'string': 'CHARACTER VARYING',
        'date': 'DATE',
        'int': 'INTEGER'
    }
    sql_columns = ',\n'.join(
        [f"{k} {map_types.get(v['python_type'], 'CHARACTER VARYING')}" for k, v in infos['columns'].items()]
    )
    query = f"""DROP TABLE IF EXISTS base_rna CASCADE;
CREATE UNLOGGED TABLE base_rna (
{sql_columns},
PRIMARY KEY (id));
"""
    sql_file = os.path.join(SQLDIR, 'create_rna_table.sql')
    with open(sql_file, 'w') as f:
        f.write(query)
        f.close()

    # create query to index table
    index_query = '\n'.join(
        [f"CREATE INDEX {k}_idx ON base_rna USING btree ({k});" for k in infos['columns'].keys()]
    )
    index_file = os.path.join(SQLDIR, 'index_rna_table.sql')
    with open(index_file, 'w') as f:
        f.write(index_query)
        f.close()


def create_rna_table():
    execute_sql_file(
        conn.host,
        conn.port,
        conn.schema,
        conn.login,
        conn.password,
        [
            {
                "source_path": SQLDIR,
                "source_name": "create_rna_table.sql",
            }
        ],
    )


def populate_utils(files, table):
    format_files = []
    for file in files:
        format_files.append(
            {"source_path": f"{DATADIR}/", "source_name": file.split("/")[-1]}
        )
    copy_file(
        PG_HOST=conn.host,
        PG_PORT=conn.port,
        PG_DB=conn.schema,
        PG_TABLE=table,
        PG_USER=conn.login,
        PG_PASSWORD=conn.password,
        list_files=format_files,
    )


def populate_rna_table():
    populate_utils([f"{DATADIR}/base_rna.csv"], "public.base_rna")


def index_rna_table():
    execute_sql_file(
        conn.host,
        conn.port,
        conn.schema,
        conn.login,
        conn.password,
        [
            {
                "source_path": SQLDIR,
                "source_name": "index_rna_table.sql",
            }
        ],
    )


def send_rna_to_minio():
    send_files(
        MINIO_URL=MINIO_URL,
        MINIO_BUCKET="data-pipeline-open",
        MINIO_USER=SECRET_MINIO_DATA_PIPELINE_USER,
        MINIO_PASSWORD=SECRET_MINIO_DATA_PIPELINE_PASSWORD,
        list_files=[
            {
                "source_path": f"{DATADIR}/",
                "source_name": "base_rna.csv",
                "dest_path": "rna/",
                "dest_name": "base_rna.csv",
            }
        ],
    )


def publish_rna_communautaire():
    post_remote_communautary_resource(
        api_key=DATAGOUV_SECRET_API_KEY,
        dataset_id="58e53811c751df03df38f42d",
        title="RNA agrégé",
        format="csv",
        remote_url=f"https://object.files.data.gouv.fr/data-pipeline-open/{AIRFLOW_ENV}/rna/base_rna.csv",
        description=f"Répertoire National des Associations en un seul fichier, agrégé à partir des données brutes ({datetime.now()})"
    )
