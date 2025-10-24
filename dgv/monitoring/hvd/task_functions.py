from datetime import datetime
from io import StringIO
import logging

import pandas as pd
import requests
from unidecode import unidecode

from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
    AIRFLOW_ENV,
    MINIO_BUCKET_DATA_PIPELINE_OPEN,
    MATTERMOST_MODERATION_NOUVEAUTES,
)
from datagouvfr_data_pipelines.utils.filesystem import File
from datagouvfr_data_pipelines.utils.mattermost import send_message
from datagouvfr_data_pipelines.utils.minio import MinIOClient
from datagouvfr_data_pipelines.utils.grist import (
    GristTable,
    get_unique_values_from_multiple_choice_column,
)

DAG_NAME = "dgv_hvd"
DATADIR = f"{AIRFLOW_DAG_TMP}{DAG_NAME}/data/"
DOC_ID = "eJxok2H2va3E" if AIRFLOW_ENV == "prod" else "fdg8zhb22dTp"
table = GristTable(DOC_ID, "Hvd_metadata_res")
minio_open = MinIOClient(bucket=MINIO_BUCKET_DATA_PIPELINE_OPEN)


# %% Recap HVD mattermost
def slugify(s):
    return unidecode(s.lower().replace(" ", "-").replace("'", "-"))


def has_unavailable_resources(dataset_id: str, df_resources: pd.DataFrame) -> bool:
    dataset_resources = df_resources.loc[df_resources["dataset.id"] == dataset_id]
    for _, row in dataset_resources.iterrows():
        r = requests.head(row["url"])
        if not r.ok:
            return True
    return False


def get_hvd(ti):
    logging.info("Getting suivi ouverture")
    df_ouverture = table.to_dataframe(
        columns_labels=False,
        usecols=[
            "title",
            "url",
            "hvd_name",
            "hvd_category",
            "organization",
            "endpoint_url_datagouv",
            "contact_point_datagouv",
            "endpoint_description_datagouv",
            "status_telechargement_automatique",
            "status_api_automatique",
        ],
    )

    logging.info("Getting datasets catalog")
    df_datasets = pd.read_csv(
        "https://www.data.gouv.fr/api/1/datasets/r/f868cca6-8da1-4369-a78d-47463f19a9a3",
        sep=";",
        usecols=["url", "tags", "organization_id", "license", "id"],
    )

    logging.info("Merging")
    df_merge = pd.merge(
        df_ouverture,
        df_datasets,
        on="url",
        how="left",
    )

    logging.info("Getting HVD datasets with unavailable resources")
    df_resources = pd.read_csv(
        "https://www.data.gouv.fr/api/1/datasets/r/4babf5f2-6a9c-45b5-9144-ca5eae6a7a6d",
        sep=";",
        usecols=["dataset.id", "url"],
    )
    df_merge["has_unavailable_resources"] = df_merge["id"].apply(
        lambda dataset_id: has_unavailable_resources(dataset_id, df_resources)
    )

    valid_statuses = [
        "Disponible sur data.gouv.fr",
        "Partiellement disponible",
        "Non requis",
    ]
    df_merge = df_merge.loc[
        (
            df_merge["status_telechargement_automatique"].isin(valid_statuses)
            | df_merge["status_api_automatique"].isin(valid_statuses)
        ),
        [
            "title",
            "url",
            "hvd_name",
            "hvd_category",
            "organization",
            "organization_id",
            "license",
            "endpoint_url_datagouv",
            "contact_point_datagouv",
            "endpoint_description_datagouv",
            "has_unavailable_resources",
        ],
    ]
    logging.info(df_merge)
    filename = f"hvd_{datetime.now().strftime('%Y-%m-%d')}.csv"
    df_merge.to_csv(f"{DATADIR}/{filename}", index=False)
    ti.xcom_push(key="filename", value=filename)


def send_to_minio(ti):
    filename = ti.xcom_pull(key="filename", task_ids="get_hvd")
    minio_open.send_files(
        list_files=[
            File(
                source_path=f"{DATADIR}/",
                source_name=filename,
                dest_path="hvd/",
                dest_name=filename,
            )
        ],
        ignore_airflow_env=True,
    )


def markdown_item(row):
    category = row["hvd_category"]
    cat_item = (
        f"tagué : _{category}_"
        if isinstance(category, str)
        else ":warning: tag manquant sur data.gouv"
    )
    hvd = row["hvd_name"]
    hvd_item = (
        f"HVD : _{hvd}_"
        if isinstance(hvd, str)
        else ":warning: HVD non renseigné sur ouverture"
    )
    return (
        f"- [{row['title']}]({row['url']})\n"
        f"   - publié par [{row['organization']}]"
        f"(https://www.data.gouv.fr/organizations/{row['organization_id']}/)\n"
        f"   - {cat_item}\n"
        f"   - {hvd_item}\n"
    )


def publish_mattermost(ti):
    filename = ti.xcom_pull(key="filename", task_ids="get_hvd")
    minio_files = sorted(
        minio_open.get_files_from_prefix("hvd/", ignore_airflow_env=True)
    )
    logging.info(minio_files)
    if len(minio_files) == 1:
        return
    all_hvd_names = set(
        GristTable(DOC_ID, "Hvd_names").to_dataframe(columns_labels=False)["hvd_name"]
    )
    goal = len(all_hvd_names)

    previous_week = pd.read_csv(StringIO(minio_open.get_file_content(minio_files[-2])))
    this_week = pd.read_csv(f"{DATADIR}/{filename}")
    this_week["hvd_name"] = this_week["hvd_name"].apply(
        lambda str_list: eval(str_list) if isinstance(str_list, str) else []
    )

    new = this_week.loc[
        (~this_week["title"].isin(previous_week["title"]))
        & (~this_week["title"].isna())
    ]
    removed = previous_week.loc[
        (~previous_week["title"].isin(this_week["title"]))
        & (~previous_week["title"].isna())
    ]

    message = "#### :flag-eu: :pokeball: Suivi HVD\n"
    hvds = get_unique_values_from_multiple_choice_column(this_week["hvd_name"])

    if len(hvds) == goal:
        message += f"# :tada: :tada: {len(hvds)}/{goal} HVD référencés :tada: :tada: "
    else:
        message += f"{len(hvds)}/{goal} HVD référencés, "
    message += f"soit {round(len(hvds) / goal * 100, 1)}% "
    message += f"et un total de {this_week['url'].nunique()} JdD "
    message += "([:arrow_down: télécharger le dernier fichier]"
    message += f"({minio_open.get_file_url('hvd/' + filename)}))\n"
    if len(new):
        message += (
            f":heavy_plus_sign: {len(new)} JDD (pour {len(get_unique_values_from_multiple_choice_column(new['hvd_name']))} HVD) "
            "par rapport à la semaine dernière\n"
        )
        for _, row in new.iterrows():
            message += markdown_item(row)
    if len(removed):
        if len(new):
            message += "\n\n"
        message += (
            f":heavy_minus_sign: {len(removed)} JDD (pour {len(get_unique_values_from_multiple_choice_column(['hvd_name']))} HVD) "
            "par rapport à la semaine dernière\n"
        )
        for _, row in removed.iterrows():
            message += markdown_item(row)

    if not (len(new) or len(removed)):
        # could also delete the latest file
        message += "Pas de changement par rapport à la semaine dernière"

    df_ouverture = table.to_dataframe(columns_labels=False)
    for col in [
        "hvd_category",
        "endpoint_url_datagouv",
        "contact_point_datagouv",
        "endpoint_description_datagouv",
    ]:
        df_ouverture[col] = df_ouverture[col].fillna("")

    pct_cat_hvd = round(
        len(df_ouverture.loc[df_ouverture["hvd_category"] != ""])
        / len(df_ouverture)
        * 100,
        1,
    )
    pct_api = round(
        len(df_ouverture.loc[df_ouverture["endpoint_url_datagouv"] != ""])
        / len(df_ouverture)
        * 100,
        1,
    )
    pct_contact_point = round(
        len(
            df_ouverture.loc[
                (df_ouverture["endpoint_url_datagouv"] != "")
                & (df_ouverture["contact_point_datagouv"] != "")
            ]
        )
        / len(df_ouverture.loc[df_ouverture["endpoint_url_datagouv"] != ""])
        * 100,
        1,
    )
    pct_endpoint_doc = round(
        len(
            df_ouverture.loc[
                (df_ouverture["endpoint_url_datagouv"] != "")
                & (df_ouverture["endpoint_description_datagouv"] != "")
            ]
        )
        / len(df_ouverture.loc[df_ouverture["endpoint_url_datagouv"] != ""])
        * 100,
        1,
    )

    message += f"\n- {pct_cat_hvd}% ont une catégorie HVD renseignée"
    message += f"\n- {pct_api}% ont au moins une API"

    message += f"\n- {pct_contact_point}% des APIs ont un point de contact"
    message += f"\n- {pct_endpoint_doc}% des APIs ont un endpoint de documentation"

    missing_hvd_name = df_ouverture.loc[df_ouverture["hvd_name"].isna()]
    if len(missing_hvd_name):
        message += f"\n\n{len(missing_hvd_name)} jeux de données n'ont pas d'ensemble de données renseigné :"
    for _, row in missing_hvd_name.iterrows():
        message += f"\n- [{row['title']}]({row['url']}) de {row['organization']}"

    missing_hvd_tag = df_ouverture.loc[df_ouverture["missing_hvd_tag"] == "True"]
    if len(missing_hvd_tag):
        message += f"\n\n{len(missing_hvd_tag)} jeux de données n'ont plus le tag HVD :"
    for _, row in missing_hvd_tag.iterrows():
        message += f"\n- [{row['title']}]({row['url']}) de {row['organization']}"

    have_unavailable_resources = this_week.loc[this_week["has_unavailable_resources"]]
    if len(have_unavailable_resources):
        message += f"\n\n{len(have_unavailable_resources)} HVD ont des ressources inaccessibles :"
    for _, row in have_unavailable_resources.iterrows():
        message += f"\n- [{row['title']}]({row['url']}) de {row['organization']}"
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
API_TABULAIRE_ID = "673b0e6774a23d9eac2af8ce"


def get_hvd_category_from_tags(tags):
    # "L" is because the column is "multiple choices" in Grist
    if not tags:
        return "L"
    tags = tags.split(",")
    return ";".join(
        ["L"]
        + [tag.replace("-", " ").capitalize() for tag in tags if tag in HVD_CATEGORIES]
    )


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
    dataservices = df_dataservices.loc[
        df_dataservices["datasets"].str.contains(dataset_id)
    ]
    # Skip tabular for now
    if len(dataservices.loc[dataservices["id"] != API_TABULAIRE_ID]):
        contact_point = (
            requests.get(
                f"https://www.data.gouv.fr/api/1/dataservices/{dataservices.iloc[0]['id']}/"
            )
            .json()
            .get("contact_point", {})
        )
        return (
            dataservices.iloc[0]["title"],
            dataservices.iloc[0]["base_api_url"],
            dataservices.iloc[0]["machine_documentation_url"],
            dataservices.iloc[0]["url"],
            contact_point.get("name"),
        )
    # coming here means no dataservice is linked to this dataset
    dataset_resources = df_resources.loc[df_resources["dataset.id"] == dataset_id]
    # we loop twice because if we can match the first condition somewhere in the dataset
    # we like it better than the fallback condition (for the endpoint description)
    for _, row in dataset_resources.iterrows():
        # We return the first one matching
        url = row["url"]
        if "request=getcapabilities" in url.lower() or url.endswith(("wms", "wfs")):
            # "fake" resources that are actually dataservices
            contact_point = (
                requests.get(f"https://www.data.gouv.fr/api/1/datasets/{dataset_id}/")
                .json()
                .get("contact_point", {})
            )
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
            contact_point = (
                requests.get(f"https://www.data.gouv.fr/api/1/datasets/{dataset_id}/")
                .json()
                .get("contact_point", {})
            )
            return (
                row["title"],
                url,
                "",
                row["dataset.url"] + "#/resources/" + row["id"],
                contact_point.get("name"),
            )
    # Fallback on tabular if available
    if len(dataservices):
        contact_point = (
            requests.get(
                f"https://www.data.gouv.fr/api/1/dataservices/{dataservices.iloc[0]['id']}/"
            )
            .json()
            .get("contact_point", {})
        )
        return (
            dataservices.iloc[0]["title"],
            dataservices.iloc[0]["base_api_url"],
            dataservices.iloc[0]["machine_documentation_url"],
            dataservices.iloc[0]["url"],
            contact_point.get("name"),
        )
    return None, None, None, None, None


def build_df_for_grist():
    logging.info("Getting datasets")
    df_datasets = pd.read_csv(
        "https://www.data.gouv.fr/datasets.csv?tag=hvd",
        sep=";",
        usecols=[
            "id",
            "url",
            "title",
            "organization",
            "resources_count",
            "tags",
            "license",
            "archived",
        ],
    )
    logging.info("Getting resources")
    df_resources = pd.read_csv(
        "https://www.data.gouv.fr/resources.csv?tag=hvd",
        sep=";",
        usecols=["dataset.id", "title", "url", "id", "format", "dataset.url"],
    )
    logging.info("Getting dataservices")
    df_dataservices = pd.read_csv(
        "https://www.data.gouv.fr/dataservices.csv",
        sep=";",
        usecols=[
            "id",
            "datasets",
            "machine_documentation_url",
            "base_api_url",
            "url",
            "title",
        ],
    ).dropna(subset="datasets")
    df_datasets["hvd_category"] = df_datasets["tags"].apply(get_hvd_category_from_tags)
    df_datasets.rename({"license": "license_datagouv"}, axis=1, inplace=True)
    logging.info("Processing...")
    (
        df_datasets["api_title_datagouv"],
        df_datasets["endpoint_url_datagouv"],
        df_datasets["endpoint_description_datagouv"],
        df_datasets["api_web_datagouv"],
        df_datasets["contact_point_datagouv"],
    ) = zip(
        *df_datasets["id"].apply(
            lambda _id: dataservice_information(
                _id, df_dataservices=df_dataservices, df_resources=df_resources
            )
        )
    )
    df_datasets.to_csv(DATADIR + "fresh_hvd_metadata.csv", index=False, sep=";")


def update_grist(ti):
    def update_quality(dataset_id: str):
        r = requests.get(
            f"https://www.data.gouv.fr/api/1/datasets/{dataset_id}/",
            headers={"X-fields": "quality"},
        )
        if not r.ok:
            logging.error(
                f"https://www.data.gouv.fr/api/1/datasets/{dataset_id}/ is unreachable"
            )
            table.update_records(
                conditions={"id2": dataset_id},
                new_values={"unreachable": True},
            )
            return
        dataset_quality = r.json()["quality"]
        table.update_records(
            conditions={"id2": dataset_id},
            new_values={k: v for k, v in dataset_quality.items() if k != "score"}
            | {"unreachable": False},
        )

    old_hvd_metadata = table.to_dataframe(columns_labels=False)
    if old_hvd_metadata["id2"].nunique() != len(old_hvd_metadata):
        raise ValueError("Grist table has duplicated dataset ids")
    fresh_hvd_metadata = pd.read_csv(
        DATADIR + "fresh_hvd_metadata.csv", sep=";"
    ).rename(
        # because the "id" column in grist has the identifier "id2"
        {"id": "id2"},
        axis=1,
    )
    fresh_hvd_metadata["hvd_category"] = fresh_hvd_metadata["hvd_category"].apply(
        lambda s: s.split(";")
    )
    if fresh_hvd_metadata["id2"].nunique() != len(fresh_hvd_metadata):
        raise ValueError("New table has duplicated dataset ids")
    removed_hvd = set(old_hvd_metadata["id2"]) - set(fresh_hvd_metadata["id2"])
    columns_to_update = [
        "resources_count",
        "hvd_category",
        "api_title_datagouv",
        "endpoint_url_datagouv",
        "endpoint_description_datagouv",
        "api_web_datagouv",
        "organization",
        "license_datagouv",
        "contact_point_datagouv",
        "archived",
        "tags",
    ]
    # updating existing rows
    updates = 0
    for dataset_id in old_hvd_metadata["id2"]:
        if dataset_id in removed_hvd:
            continue
        row_old = old_hvd_metadata.loc[old_hvd_metadata["id2"] == dataset_id].iloc[0]
        row_new = fresh_hvd_metadata.loc[fresh_hvd_metadata["id2"] == dataset_id].iloc[
            0
        ]
        new_values = {}
        for col in columns_to_update:
            if (
                (isinstance(row_new[col], str) and row_new[col])
                or (isinstance(row_new[col], list) and row_new[col])
                or (isinstance(row_new[col], float) and not pd.isna(row_new[col]))
            ) and row_old[col] != row_new[col]:
                logging.info(
                    f"dataset {dataset_id} changing column '{col}': {row_old[col]} for {row_new[col]}"
                )
                new_values[col] = row_new[col]
        if new_values:
            updates += 1
            table.update_records(
                conditions={"id2": dataset_id},
                new_values=new_values,
            )
    logging.info(f"Updated {updates} rows")
    # adding new rows
    new_rows = []
    for dataset_id in set(fresh_hvd_metadata["id2"]) - set(old_hvd_metadata["id2"]):
        new_rows.append(
            fresh_hvd_metadata.loc[
                fresh_hvd_metadata["id2"] == dataset_id,
                ["id2", "url", "title"] + columns_to_update,
            ].iloc[0]
        )
    if new_rows:
        logging.info(f"Adding {len(new_rows)} rows")
        table.from_dataframe(
            df=pd.DataFrame(new_rows).rename({"id2": "id"}, axis=1),
            append="lazy",
        )
        ti.xcom_push(
            key="new_rows",
            value=[(r["title"], r["url"], r["organization"]) for r in new_rows],
        )
    if removed_hvd:
        to_send = []
        for hvd_id in removed_hvd:
            r = requests.get(
                f"https://www.data.gouv.fr/api/1/datasets/{hvd_id}/",
                headers={"X-fields": "title,organization{name}"},
            )
            if r.ok:
                r = r.json()
                to_send.append((hvd_id, r["title"], r["organization"]["name"]))
            else:
                logging.warning(
                    f"Issue with https://www.data.gouv.fr/datasets/{hvd_id}/"
                )
                table.update_records(
                    conditions={"id2": hvd_id},
                    new_values={"unreachable": True},
                )
        if to_send:
            message = ":alert: @clarisse Les jeux de données suivants ont perdu leur tag HVD :"
            for _id, title, orga in to_send:
                message += (
                    f"\n- [{title}](https://www.data.gouv.fr/datasets/{_id}/)"
                    f" de l'organisation {orga}"
                )
                table.update_records(
                    conditions={"id2": _id},
                    new_values={"missing_hvd_tag": True},
                )
            send_message(message, MATTERMOST_MODERATION_NOUVEAUTES)

    # updating quality metrics
    logging.info("Updating datasets quality metrics columns")
    df = table.to_dataframe(columns_labels=False, usecols=["id2"])
    df["id2"].apply(update_quality)
    file_name = "grist_hvd.csv"
    table.to_dataframe().to_csv(DATADIR + file_name, sep=";", index=False)
    minio_open.send_files(
        [
            File(
                source_path=DATADIR,
                source_name=file_name,
                dest_path="hvd/",
                dest_name=f"{datetime.today().strftime('%Y-%m')}_" + file_name,
            )
        ],
        ignore_airflow_env=True,
        burn_after_sending=True,
    )
    return len(new_rows)


def publish_mattermost_grist(ti):
    new_rows = ti.xcom_pull(key="new_rows", task_ids="update_grist")
    message = (
        f"#### {len(new_rows)} nouvelles lignes dans [la table Grist HVD]"
        "(https://grist.numerique.gouv.fr/o/datagouv/eJxok2H2va3E/suivi-des-ouvertures-CITP-et-HVD/p/4)"
    )
    for title, url, orga in new_rows:
        message += f"\n- [{title}]({url}) de l'organisation {orga}"
    send_message(message, MATTERMOST_MODERATION_NOUVEAUTES)


