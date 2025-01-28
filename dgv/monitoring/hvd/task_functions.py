from datetime import datetime
from math import isnan
import pandas as pd
import requests
from unidecode import unidecode
from io import StringIO

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
    MINIO_BUCKET_DATA_PIPELINE_OPEN,
    MATTERMOST_MODERATION_NOUVEAUTES,
)
from datagouvfr_data_pipelines.utils.mattermost import send_message, MAX_MESSAGE_LENGTH
from datagouvfr_data_pipelines.utils.minio import MinIOClient
from datagouvfr_data_pipelines.utils.grist import (
    get_table_as_df,
    update_records,
    df_to_grist,
)

DAG_NAME = "dgv_hvd"
DATADIR = f"{AIRFLOW_DAG_TMP}{DAG_NAME}/data/"
DOC_ID = "eJxok2H2va3E"
minio_open = MinIOClient(bucket=MINIO_BUCKET_DATA_PIPELINE_OPEN)


# %% Recap HVD mattermost
def slugify(s):
    return unidecode(s.lower().replace(" ", "-").replace("'", "-"))


def get_hvd(ti):
    print("Getting suivi ouverture")
    df_ouverture = get_table_as_df(
        doc_id=DOC_ID,
        table_id="Hvd",
        columns_labels=True,
    )
    goal = df_ouverture['Ensemble_de_donnees'].nunique()
    # goal_list = list(df_ouverture['Ensemble_de_donnees'].unique())
    categories = {
        slugify(cat): cat
        for cat in set(df_ouverture['Thematique'])
    }
    dfs = []
    for _type in ['Telechargement', 'API']:
        tmp = df_ouverture[
            ['Titre', 'Ensemble_de_donnees', 'Thematique'] +
            [c for c in df_ouverture.columns if c.endswith(_type)]
        ]
        tmp['type'] = 'dataservices' if _type == 'API' else 'datasets'
        tmp.rename(
            {c: c.replace(f'_{_type}', '') for c in df_ouverture.columns if c.endswith(_type)},
            axis=1, inplace=True
        )
        dfs.append(tmp)
    df_ouverture = pd.concat(dfs)
    df_ouverture.rename({
        'URL': 'url',
    }, axis=1, inplace=True)

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
    df_merge['in_ouverture'] = df_merge['type'].notna()
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

    issues = this_week.loc[
        this_week['hvd_category'].isna()
        & ~this_week['title'].isna()
    ]
    if len(issues):
        message += "\n\n :small_red_triangle: Les jeux de données suivants ne sont pas 100% renseignés\n"
        for _, row in issues.iterrows():
            message += markdown_item(row)
            if len(message) > round(MAX_MESSAGE_LENGTH * 0.95):
                break
    send_message(message, MATTERMOST_MODERATION_NOUVEAUTES)


# %% Grist
HVD_CATEGORIES = [
    "meteorologiques",
    "entreprises-et-propriete-dentreprises",
    "geospatiales",
    "mobilite",
    "observation-de-la-terre-et-environnement",
    "statistiques",
]
API_TABULAIRE_ID = '673b0e6774a23d9eac2af8ce'


def get_hvd_category_from_tags(tags):
    # "L" is because the column is "multiple choices" in Grist
    if not tags:
        return "L"
    tags = tags.split(",")
    return ";".join(["L"] + [
        tag.replace("-", " ").capitalize()
        for tag in tags if tag in HVD_CATEGORIES
    ])


def dataservice_information(dataset_id, df_dataservices, df_resources):
    """
    Gets:
        - api_title_datagouv
        - endpoint_url_datagouv
        - endpoint_description_datagouv
        - api_web_datagouv
        - contact_point_datagouv
    from a dataset_id
    """
    dataservices = df_dataservices.loc[df_dataservices["datasets"].str.contains(dataset_id)]
    # Skip tabular for now
    if len(dataservices.loc[dataservices["id"] != API_TABULAIRE_ID]):
        contact_point = requests.get(
            f"https://www.data.gouv.fr/api/1/dataservices/{dataservices.iloc[0]['id']}/"
        ).json()["contact_point"] or {}
        return (
            dataservices.iloc[0]["title"],
            dataservices.iloc[0]["base_api_url"],
            dataservices.iloc[0]["endpoint_description_url"],
            dataservices.iloc[0]["url"],
            contact_point.get("name")
        )
    # coming here means no dataservice is linked to this dataset
    dataset_resources = df_resources.loc[df_resources["dataset.id"] == dataset_id]
    # we loop twice because if we can match the first condition somewhere in the dataset
    # we like it better than the fallback condition (for the endpoint description)
    for _, row in dataset_resources.iterrows():
        # We return the first one matching
        url = row["url"]
        if (
            "request=getcapabilities" in url.lower()
            or url.endswith(("wms", "wfs"))
        ):
            # "fake" resources that are actually dataservices
            contact_point = requests.get(
                f"https://www.data.gouv.fr/api/1/datasets/{dataset_id}/"
            ).json()["contact_point"] or {}
            return (
                row["title"],
                url,
                url,
                row["dataset.url"] + "#/resources/" + row["id"],
                contact_point.get("name"),
            )
    for _, row in dataset_resources.iterrows():
        url = row["url"]
        if row["format"] in ["ogc:wms", "ogc:wfs", "wms", "wfs"]:
            contact_point = requests.get(
                f"https://www.data.gouv.fr/api/1/datasets/{dataset_id}/"
            ).json()["contact_point"] or {}
            return (
                row["title"],
                url,
                "",
                row["dataset.url"] + "#/resources/" + row["id"],
                contact_point.get("name"),
            )
    # Fallback on tabular if available
    if len(dataservices):
        contact_point = requests.get(
            f"https://www.data.gouv.fr/api/1/dataservices/{dataservices.iloc[0]['id']}/"
        ).json()["contact_point"] or {}
        return (
            dataservices.iloc[0]["title"],
            dataservices.iloc[0]["base_api_url"],
            dataservices.iloc[0]["endpoint_description_url"],
            dataservices.iloc[0]["url"],
            contact_point.get("name")
        )
    return None, None, None, None, None


def build_df_for_grist():
    print("Getting datasets")
    df_datasets = pd.read_csv(
        "https://www.data.gouv.fr/fr/datasets.csv?tag=hvd",
        delimiter=";",
        usecols=["id", "url", "title", "organization", "resources_count", "tags", "license"],
    )
    print("Getting resources")
    df_resources = pd.read_csv(
        "https://www.data.gouv.fr/fr/resources.csv?tag=hvd",
        delimiter=";",
        usecols=["dataset.id", "title", "url", "id", "format", "dataset.url"]
    )
    print("Getting dataservices")
    df_dataservices = pd.read_csv(
        "https://www.data.gouv.fr/fr/dataservices.csv",
        delimiter=";",
        usecols=["id", "datasets", "endpoint_description_url", "base_api_url", "url", "title"],
    ).dropna(subset="datasets")
    df_datasets['hvd_category'] = df_datasets["tags"].apply(get_hvd_category_from_tags)
    df_datasets.rename({"license": "license_datagouv"}, axis=1, inplace=True)
    print("Processing...")
    (
        df_datasets['api_title_datagouv'],
        df_datasets['endpoint_url_datagouv'],
        df_datasets['endpoint_description_datagouv'],
        df_datasets['api_web_datagouv'],
        df_datasets['contact_point_datagouv']
    ) = zip(*df_datasets["id"].apply(
        lambda _id: dataservice_information(_id, df_dataservices=df_dataservices, df_resources=df_resources)
    ))
    df_datasets.to_csv(DATADIR + "fresh_hvd_metadata.csv", index=False)


def update_grist(ti):
    old_hvd_metadata = get_table_as_df(
        doc_id=DOC_ID,
        table_id="Hvd_metadata_res",
        columns_labels=False,
    )
    if old_hvd_metadata["id2"].nunique() != len(old_hvd_metadata):
        raise ValueError("Grist table has duplicated dataset ids")
    fresh_hvd_metadata = pd.read_csv(DATADIR + "fresh_hvd_metadata.csv").rename(
        # because the "id" column in grist has the identifier "id2"
        {"id": "id2"},
        axis=1
    )
    fresh_hvd_metadata["hvd_category"] = fresh_hvd_metadata["hvd_category"].apply(
        lambda s: s.split(";")
    )
    if fresh_hvd_metadata["id2"].nunique() != len(fresh_hvd_metadata):
        raise ValueError("New table has duplicated dataset ids")
    removed_hvd = set(old_hvd_metadata["id2"]) - set(fresh_hvd_metadata["id2"])
    for hvd_id in removed_hvd:
        r = requests.get(
            f"https://www.data.gouv.fr/api/1/datasets/{hvd_id}/",
            headers={"X-fields": "title,organization{name}"},
        )
        r.raise_for_status()
        r = r.json()
        send_message(
            f":alert: Le jeu de données [{r['title']}](https://www.data.gouv.fr/fr/datasets/{hvd_id}/)"
            f" de l'organisation {r['organization']['name']}"
            " a perdu son tag HVD @clarisse",
            MATTERMOST_MODERATION_NOUVEAUTES
        )
    columns_to_update = [
        "resources_count",
        "hvd_category",
        "api_title_datagouv",
        "endpoint_url_datagouv",
        "endpoint_description_datagouv",
        "api_web_datagouv",
    ]
    # updating existing rows
    updates = 0
    for dataset_id in old_hvd_metadata["id2"]:
        if dataset_id in removed_hvd:
            continue
        row_old = old_hvd_metadata.loc[old_hvd_metadata["id2"] == dataset_id].iloc[0]
        row_new = fresh_hvd_metadata.loc[fresh_hvd_metadata["id2"] == dataset_id].iloc[0]
        new_values = {}
        for col in columns_to_update:
            if (
                (isinstance(row_new[col], str) and row_new[col])
                or (isinstance(row_new[col], list) and row_new[col])
                or (isinstance(row_new[col], float) and not isnan(row_new[col]))
            ) and row_old[col] != row_new[col]:
                print(f"dataset {dataset_id} changing column '{col}':", row_old[col], "for", row_new[col])
                new_values[col] = row_new[col]
        if new_values:
            updates += 1
            update_records(
                doc_id=DOC_ID,
                table_id="Hvd_metadata_res",
                conditions={"id2": dataset_id},
                new_values=new_values,
            )
    print(f"Updated {updates} rows")
    # adding new rows
    new_rows = []
    for dataset_id in (set(fresh_hvd_metadata["id2"]) - set(old_hvd_metadata["id2"])):
        new_rows.append(fresh_hvd_metadata.loc[
            fresh_hvd_metadata["id2"] == dataset_id,
            ["id2", "url", "title"] + columns_to_update,
        ].iloc[0])
    if not new_rows:
        return
    print(f"Adding {len(new_rows)} rows")
    df_to_grist(
        df=pd.DataFrame(new_rows).rename({"id2": "id"}, axis=1),
        doc_id=DOC_ID,
        table_id="Hvd_metadata_res",
        append="lazy",
    )
    ti.xcom_push(key="new_rows", value=[(r["title"], r["url"]) for r in new_rows])
    return new_rows


def publish_mattermost_grist(ti):
    new_rows = ti.xcom_pull(key="new_rows", task_ids="update_grist")
    message = (
        f"#### {len(new_rows)} nouvelles lignes dans [la table Grist HVD]"
        "(https://grist.numerique.gouv.fr/o/datagouv/eJxok2H2va3E/suivi-des-ouvertures-CITP-et-HVD/p/4)"
    )
    for (title, url) in new_rows:
        message += f"\n- [{title}]({url})"
    send_message(message, MATTERMOST_MODERATION_NOUVEAUTES)
