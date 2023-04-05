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
import pandas as pd
import os
from unicode import unidecode
from datetime import date
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


# def send_distribution_to_minio():
#     send_files(
#         MINIO_URL=MINIO_URL,
#         MINIO_BUCKET=MINIO_BUCKET_DATA_PIPELINE_OPEN,
#         MINIO_USER=SECRET_MINIO_DATA_PIPELINE_USER,
#         MINIO_PASSWORD=SECRET_MINIO_DATA_PIPELINE_PASSWORD,
#         list_files=[
#             {
#                 "source_path": f"{DATADIR}/",
#                 "source_name": "distribution_prix.csv",
#                 "dest_path": "dvf/",
#                 "dest_name": "distribution_prix.csv",
#             }
#         ],
#     )


# def publish_stats_dvf(ti):
#     with open(f"{AIRFLOW_DAG_HOME}{DAG_FOLDER}dvf/config/dgv.json") as fp:
#         data = json.load(fp)
#     post_resource(
#         api_key=DATAGOUV_SECRET_API_KEY,
#         file_to_upload={
#             "dest_path": f"{DATADIR}/",
#             "dest_name": data["file"]
#         },
#         dataset_id=data[AIRFLOW_ENV]["dataset_id"],
#         resource_id=data[AIRFLOW_ENV]["resource_id"],
#     )
#     ti.xcom_push(key="dataset_id", value=data[AIRFLOW_ENV]["dataset_id"])


# def notification_mattermost(ti):
#     dataset_id = ti.xcom_pull(key="dataset_id", task_ids="publish_stats_dvf")
#     if AIRFLOW_ENV == "dev":
#         url = "https://demo.data.gouv.fr/fr/datasets/"
#     if AIRFLOW_ENV == "prod":
#         url = "https://www.data.gouv.fr/fr/datasets/"
#     send_message(
#         f"Stats DVF générées :"
#         f"\n- intégré en base de données"
#         f"\n- publié [sur {'demo.' if AIRFLOW_ENV == 'dev' else ''}data.gouv.fr]"
#         f"({url}{dataset_id})"
#         f"\n- données upload [sur Minio]({MINIO_URL}/buckets/{MINIO_BUCKET_DATA_PIPELINE_OPEN}/browse)"
#     )
