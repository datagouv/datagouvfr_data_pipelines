from datetime import timedelta, datetime
import pandas as pd
import requests
from unidecode import unidecode
from io import StringIO

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
    MINIO_BUCKET_DATA_PIPELINE_OPEN,
    MATTERMOST_MODERATION_NOUVEAUTES,
)
from datagouvfr_data_pipelines.utils.mattermost import send_message
from datagouvfr_data_pipelines.utils.minio import MinIOClient

DAG_NAME = "dgv_hvd"
DATADIR = f"{AIRFLOW_DAG_TMP}{DAG_NAME}/data/"
minio_open = MinIOClient(bucket=MINIO_BUCKET_DATA_PIPELINE_OPEN)


def slugify(s):
    return unidecode(s.lower().replace(" ", "-").replace("'", "-"))


def get_hvd(ti):
    print("Getting suivi ouverture")
    ouverture_hvd_api = 'https://grist.incubateur.net/api/docs/eJxok2H2va3E/tables/Hvd/records'
    r = requests.get(ouverture_hvd_api).json()
    df_ouverture = pd.DataFrame([k['fields'] for k in r['records']])
    goal = df_ouverture['Ensemble_de_donnees'].nunique()
    # goal_list = list(df_ouverture['Ensemble_de_donnees'].unique())
    categories = {
        slugify(cat): cat
        for cat in set(df_ouverture['Thematique'])
    }

    print("Getting datasets catalog")
    df_datasets = pd.read_csv(
        'https://www.data.gouv.fr/fr/datasets/r/f868cca6-8da1-4369-a78d-47463f19a9a3',
        delimiter=';',
    )
    df_datasets['url'] = df_datasets['url'].str.replace('http://', 'https://')

    print("Merging")
    df_merge = df_datasets.merge(
        df_ouverture,
        on='url',
        how='outer',
    )
    df_merge['tagged_hvd'] = df_merge['tags'].str.contains('hvd') | False
    df_merge['in_ouverture'] = df_merge['Statut'].notna()
    df_merge['hvd_name'] = df_merge['Ensemble_de_donnees']
    df_merge['hvd_category'] = (
        df_merge['tags'].fillna('').apply(
            lambda tags: next((tag for tag in tags.split(',') if tag in categories.keys()), None)
        )
    )
    df_merge = (
        df_merge.loc[
            df_merge['tagged_hvd'] | (df_merge['in_ouverture'] & (df_merge['Statut'].apply(
                lambda s: any(s == k for k in ['Disponible sur data.gouv.fr', 'Disponible'])
                if isinstance(s, str) else False
            ))),
            [
                'title', 'url', 'in_ouverture', 'tagged_hvd', 'hvd_name',
                'hvd_category', 'organization', 'organization_id', 'license'
            ]
        ]
    )
    print(df_merge)
    filename = f'hvd_{datetime.now().strftime("%Y-%m-%d")}.csv'
    df_merge.to_csv(f"{DATADIR}/{filename}", index=False)
    ti.xcom_push(key="filename", value=filename)
    ti.xcom_push(key="goal", value=goal)
    # ti.xcom_push(key="goal_list", value=goal_list)


def send_to_minio(ti):
    filename = ti.xcom_pull(key="filename", task_ids="get_hvd")
    minio_open.send_files(
        list_files=[
            {
                "source_path": f"{DATADIR}/",
                "source_name": filename,
                "dest_path": "hvd/",
                "dest_name": filename,
            }
        ],
        ignore_airflow_env=True,
    )


def markdown_item(row):
    category = row['hvd_category']
    cat_item = (
        f"tagué : _{category}_" if isinstance(category, str)
        else ":warning: tag manquant sur data.gouv"
    )
    hvd = row['hvd_name']
    hvd_item = (
        f"HVD : _{hvd}_" if isinstance(hvd, str)
        else ":warning: HVD non renseigné sur ouverture"
    )
    return (
        f"- [{row['title']}]({row['url']})\n"
        f"   - publié par [{row['organization']}]"
        f"(https://www.data.gouv.fr/fr/organizations/{row['organization_id']}/)\n"
        f"   - {cat_item}\n"
        f"   - {hvd_item}\n"
    )


def publish_mattermost(ti):
    filename = ti.xcom_pull(key="filename", task_ids="get_hvd")
    goal = ti.xcom_pull(key="goal", task_ids="get_hvd")
    # goal_list = ti.xcom_pull(key="goal_list", task_ids="get_hvd")
    minio_files = sorted(minio_open.get_files_from_prefix('hvd/', ignore_airflow_env=True))
    print(minio_files)
    if len(minio_files) == 1:
        return

    previous_week = pd.read_csv(StringIO(
        minio_open.get_file_content(minio_files[-2])
    ))
    this_week = pd.read_csv(f"{DATADIR}/{filename}")

    new = this_week.loc[
        (~this_week['title'].isin(previous_week['title']))
        & (~this_week['title'].isna())
    ]
    removed = previous_week.loc[
        (~previous_week['title'].isin(this_week['title']))
        & (~previous_week['title'].isna())
    ]
    # tmp = list(this_week['hvd_name'].unique())
    # print([k for k in tmp if k not in goal_list])
    # print([k for k in goal_list if k not in tmp])

    message = "#### :flag-eu: :pokeball: Suivi HVD\n"
    if len(this_week['hvd_name'].unique()) == goal:
        message += f"# :tada: :tada: {this_week['hvd_name'].nunique()}/{goal} HVD référencés :tada: :tada: "
    else:
        message += f"{len(this_week['hvd_name'].unique())}/{goal} HVD référencés, "
    message += f"soit {round(this_week['hvd_name'].nunique() / goal * 100, 1)}% "
    message += f"et un total de {this_week['url'].nunique()} JdD "
    message += "([:arrow_down: télécharger le dernier fichier]"
    message += f"({minio_open.get_file_url('hvd/' + filename, ignore_airflow_env=True)}))\n"
    if len(new):
        message += (
            f":heavy_plus_sign: {len(new)} JDD (pour {new['hvd_name'].nunique()} HVD) "
            "par rapport à la semaine dernière\n"
        )
        for _, row in new.iterrows():
            message += markdown_item(row)
    if len(removed):
        if len(new):
            message += '\n\n'
        message += (
            f":heavy_minus_sign: {len(removed)} JDD (pour {removed['hvd_name'].nunique()} HVD) "
            "par rapport à la semaine dernière\n"
        )
        for _, row in removed.iterrows():
            message += markdown_item(row)

    if not (len(new) or len(removed)):
        # could also delete the latest file
        message += "Pas de changement par rapport à la semaine dernière"
    send_message(message, MATTERMOST_MODERATION_NOUVEAUTES)


default_args = {}

with DAG(
    dag_id=DAG_NAME,
    schedule_interval="0 4 * * 1",
    start_date=datetime(2024, 6, 1),
    dagrun_timeout=timedelta(minutes=60),
    tags=["hvd", "datagouv"],
    default_args=default_args,
    catchup=False,
) as dag:

    clean_previous_outputs = BashOperator(
        task_id="clean_previous_outputs",
        bash_command=f"rm -rf {DATADIR} && mkdir -p {DATADIR}",
    ),

    get_hvd = PythonOperator(
        task_id="get_hvd",
        python_callable=get_hvd
    )

    send_to_minio = PythonOperator(
        task_id="send_to_minio",
        python_callable=send_to_minio
    )

    publish_mattermost = PythonOperator(
        task_id="publish_mattermost",
        python_callable=publish_mattermost,
    )

    get_hvd.set_upstream(clean_previous_outputs)
    send_to_minio.set_upstream(get_hvd)
    publish_mattermost.set_upstream(send_to_minio)
