from typing import Dict, Optional
from datetime import datetime
import json
import numpy as np
import os
import pandas as pd
from pathlib import Path
import pickle
import requests
import shutil
import yaml

import emails


# Template for consolidation datasets title

datasets_title_template = (
    'Fichiers consolidés des données respectant le schéma "{schema_title}"'
)

# Template for consolidation datasets description (Markdown)

datasets_description_template = """
Ceci est un jeu de données généré automatiquement par Etalab. Il regroupe les données qui respectent le schéma {schema_name}, par version du schéma.

La fiche présentant le schéma et ses caractéristiques est disponible sur [https://schema.data.gouv.fr/{schema_name}/latest.html](https://schema.data.gouv.fr/{schema_name}/latest.html)

### Qu'est-ce qu'un schéma ?

Les schémas de données permettent de décrire des modèles de données : quels sont les différents champs, comment sont représentées les données, quelles sont les valeurs possibles, etc.

Vous pouvez retrouver l'ensemble des schémas au référentiel sur le site schema.data.gouv.fr

### Comment sont produites ces données ?

Ces données sont produites à partir des ressources publiées sur le site [data.gouv.fr](http://data.gouv.fr) par différents producteurs. Etalab détecte automatiquement les ressources qui obéissent à un schéma et concatène l'ensemble des données en un seul fichier, par version de schéma.

Ces fichiers consolidés permettent aux réutilisateurs de manipuler un seul fichier plutôt qu'une multitude de ressources et contribue ainsi à améliorer la qualité de l'open data.

### Comment intégrer mes données dans ces fichiers consolidés ?

Si vous êtes producteurs de données et que vous ne retrouvez pas vos données dans ces fichiers consolidés, c'est probablement parce que votre ressource sur [data.gouv.fr](http://data.gouv.fr) n'est pas conforme au schéma. Vous pouvez vérifier la conformité de votre ressource via l'outil [https://publier.etalab.studio/upload?schema={schema_name}](https://publier.etalab.studio/upload?schema={schema_name})

En cas de problème persistant, vous pouvez contacter le support data.gouv [lien vers [https://support.data.gouv.fr/](https://support.data.gouv.fr/)].

### Comment produire des données conformes ?

Un certain nombre d'outils existent pour accompagner les producteurs de données. Vous pouvez notamment vous connecter sur le site [https://publier.etalab.studio/select?schema={schema_name}](https://publier.etalab.studio/select?schema={schema_name}) pour pouvoir saisir vos données selon trois modes :

- upload de fichier existant
- saisie via formulaire
- saisie via tableur
"""

# Template for mail/comment (added, updated and deleted schema)

added_schema_comment_template = """
Bonjour,

Vous recevez ce message car suite à un contrôle automatique de vos données par notre robot de validation, nous constatons que le fichier {resource_title} de ce jeu de données est conforme au schéma {schema_name} (version {most_recent_valid_version}).
Nous avons donc automatiquement ajouté à ce fichier la métadonnée de schéma correspondante, ce qui atteste de la qualité des données que vous avez publiées.

Une question ? Écrivez à validation@data.gouv.fr en incluant l'URL du jeu de données concerné.
"""

updated_schema_comment_template = """
Bonjour,

Vous recevez ce message car suite à un contrôle automatique de vos données par notre robot de validation, nous constatons que le fichier {resource_title} de ce jeu de données (qui respecte le schéma {schema_name}) n'avait pas dans ses métadonnées la version de schéma la plus récente qu'il respecte.
Nous avons donc automatiquement mis à jour les métadonnées du fichier en indiquant la version adéquate du schéma.

Version précédemment indiquée : {initial_version_name}
Version mise à jour : {most_recent_valid_version}

Une question ? Écrivez à validation@data.gouv.fr en incluant l'URL du jeu de données concerné.
"""

deleted_schema_mail_template_org = """
Bonjour,<br />
<br />
Vous recevez ce message automatique car vous êtes admin de l'organisation {organisation_name} sur data.gouv.fr. Votre organisation a publié le jeu de données {dataset_title}, dont le fichier {resource_title} se veut conforme au schéma {schema_name}.<br />
Cependant, suite à un contrôle automatique de vos données par notre robot de validation, il s'avère que ce fichier ne respecte aucune version de ce schéma.<br />
Nous avons donc automatiquement supprimé la métadonnée de schéma associée à ce fichier.<br />
<br />
Vous pouvez consulter le [rapport de validation](https://validata.etalab.studio/table-schema?input=url&schema_url={schema_url}&url={resource_url}&repair=true) pour vous aider à corriger les erreurs (ce rapport est relatif à la version la plus récente du schéma, mais votre fichier a bien été testé vis-à-vis de toutes les versions possibles du schéma).<br />
<br />
Vous pourrez alors restaurer la métadonnée de schéma une fois un fichier valide publié.<br />
<br />
Une question ? Écrivez à validation@data.gouv.fr en incluant l'URL du jeu de données concerné.<br />
<br />
Cordialement,<br />
<br />
L'équipe de data.gouv.fr
"""

deleted_schema_mail_template_own = """
Bonjour,<br />
<br />
Vous recevez ce message automatique car vous avez publié sur data.gouv.fr le jeu de données {dataset_title}, dont le fichier {resource_title} se veut conforme au schéma {schema_name}.<br />
Cependant, suite à un contrôle automatique de vos données par notre robot de validation, il s'avère que ce fichier ne respecte aucune version de ce schéma.<br />
Nous avons donc automatiquement supprimé la métadonnée de schéma associée à ce fichier.<br />
<br />
Vous pouvez consulter le [rapport de validation](https://validata.etalab.studio/table-schema?input=url&schema_url={schema_url}&url={resource_url}&repair=true) pour vous aider à corriger les erreurs (ce rapport est relatif à la version la plus récente du schéma, mais votre fichier a bien été testé vis-à-vis de toutes les versions possibles du schéma).<br />
<br />
Vous pourrez alors restaurer la métadonnée de schéma une fois un fichier valide publié.<br />
<br />
Une question ? Écrivez à validation@data.gouv.fr en incluant l'URL du jeu de données concerné.<br />
<br />
Cordialement,<br />
<br />
L'équipe de data.gouv.fr
"""

deleted_schema_comment_template = """
Bonjour,

Vous recevez ce message car suite à un contrôle automatique de vos données par notre robot de validation, nous constatons que le fichier {resource_title} de ce jeu de données se veut conforme au schéma {schema_name} alors qu'il ne respecte aucune version de ce schéma.
Nous avons donc automatiquement supprimé la métadonnée de schéma associée à ce fichier.

Vous pouvez consulter le [rapport de validation](https://validata.etalab.studio/table-schema?input=url&schema_url={schema_url}&url={resource_url}&repair=true) pour vous aider à corriger les erreurs (ce rapport est relatif à la version la plus récente du schéma, mais votre fichier a bien été testé vis-à-vis de toutes les versions possibles du schéma).

Vous pourrez alors restaurer la métadonnée de schéma une fois un fichier valide publié.

Une question ? Écrivez à validation@data.gouv.fr en incluant l'URL du jeu de données concerné.
"""

# Duplicated functions


def remove_old_schemas(config_dict, schemas_catalogue_list):
    schemas_list = [schema["name"] for schema in schemas_catalogue_list]
    mydict = {}
    # Remove old schemas still in yaml
    for schema_name in config_dict.keys():
        if schema_name in schemas_list:
            mydict[schema_name] = config_dict[schema_name]
        else:
            print("{} - Remove old schema not anymore in catalog".format(schema_name))
    return mydict


def get_schema_dict(schema_name: str, schemas_catalogue_list: list) -> Optional[dict]:
    """Get the dictionnary with information on the schema (when schemas catalogue list already loaded)"""
    res = None
    for schema in schemas_catalogue_list:
        if schema["name"] == schema_name:
            res = schema

    if res is None:
        print("No schema named '{}' found.".format(schema_name))

    return res


def add_most_recent_valid_version(df_ref: pd.DataFrame) -> pd.DataFrame:
    """Based on validation columns by version, adds a column to the ref_table that shows the most recent version of the schema for which the resource is valid"""
    version_cols_list = [col for col in df_ref.columns if col.startswith("is_valid_v_")]

    df_ref["most_recent_valid_version"] = ""

    for col in sorted(version_cols_list, reverse=True):
        df_ref.loc[
            (df_ref["most_recent_valid_version"] == ""),
            "most_recent_valid_version",
        ] = df_ref.loc[(df_ref["most_recent_valid_version"] == ""), col].apply(
            lambda x: x * col.replace("is_valid_v_", "")
        )

    df_ref.loc[
        (df_ref["most_recent_valid_version"] == ""),
        "most_recent_valid_version",
    ] = np.nan

    return df_ref


# Utility functions


# Creates a dataset on data.gouv.fr for consolidation files (used only if does not exist yet in config file)
def create_schema_consolidation_dataset(
    schema_name, schemas_catalogue_list, api_url, headers
):
    global datasets_description_template, datasets_title_template

    schema_title = get_schema_dict(schema_name, schemas_catalogue_list)["title"]

    response = requests.post(
        api_url + "datasets/",
        json={
            "title": datasets_title_template.format(schema_title=schema_title),
            "description": datasets_description_template.format(
                schema_name=schema_name
            ),
            "organization": "534fff75a3a7292c64a77de4",
            "license": "lov2",
        },
        headers=headers,
    )

    return response


# Generic function to update a field (key) in the config file
def update_config_file(schema_name, key, value, config_path):
    with open(config_path, "r") as f:
        config_dict = yaml.safe_load(f)

    config_dict[schema_name][key] = value

    with open(config_path, "w") as outfile:
        yaml.dump(config_dict, outfile, default_flow_style=False)


# Adds the resource ID of the consolidated file for a given schema version in the config file
def update_config_version_resource_id(schema_name, version_name, r_id, config_path):
    with open(config_path, "r") as f:
        config_dict = yaml.safe_load(f)

    if "latest_resource_ids" not in config_dict[schema_name]:
        config_dict[schema_name]["latest_resource_ids"] = {version_name: r_id}
    else:
        config_dict[schema_name]["latest_resource_ids"][version_name] = r_id

    with open(config_path, "w") as outfile:
        yaml.dump(config_dict, outfile, default_flow_style=False)


# Returns if resource schema (version) metadata should be updated or not based on what we know about the resource
def is_schema_version_to_update(row):
    initial_version_name = row["initial_version_name"]
    most_recent_valid_version = row["most_recent_valid_version"]
    resource_found_by = row["resource_found_by"]

    return (
        (resource_found_by == "1 - schema request")
        and (most_recent_valid_version == most_recent_valid_version)
        and (initial_version_name != most_recent_valid_version)
    )


# Returns if resource schema (version) metadata should be added or not based on what we know about the resource
def is_schema_to_add(row):
    resource_found_by = row["resource_found_by"]
    is_valid_one_version = row["is_valid_one_version"]

    return (resource_found_by != "1 - schema request") and is_valid_one_version


# Returns if resource schema (version) metadata should be deleted or not based on what we know about the resource
def is_schema_to_drop(row):
    resource_found_by = row["resource_found_by"]
    is_valid_one_version = row["is_valid_one_version"]

    return (resource_found_by == "1 - schema request") and (
        is_valid_one_version == False
    )


# Function that adds a schema (version) metadata on a resource
def add_resource_schema(
    api_url: str,
    dataset_id: str,
    resource_id: str,
    schema_name: str,
    version_name: str,
    headers: Dict[str, str],
) -> bool:
    schema = {"name": schema_name, "version": version_name}

    try:
        url = api_url + "datasets/{}/resources/{}/".format(dataset_id, resource_id)
        r = requests.get(url, headers=headers)
        extras = r.json()["extras"]
    except:
        extras = {}

    extras["consolidation_schema:add_schema"] = schema_name

    obj = {"schema": schema, "extras": extras}

    url = api_url + "datasets/{}/resources/{}/".format(dataset_id, resource_id)
    response = requests.put(url, json=obj, headers=headers)

    if response.status_code != 200:
        print(
            "🔴 Schema could not be added on resource. Dataset ID: {} - Resource ID: {}".format(
                dataset_id, resource_id
            )
        )

    return response.status_code == 200


# Function that updates a schema (version) metadata on a resource
def update_resource_schema(
    api_url: str,
    dataset_id: str,
    resource_id: str,
    schema_name: str,
    version_name: str,
    headers: Dict[str, str],
) -> bool:
    schema = {"name": schema_name, "version": version_name}

    try:
        url = api_url + "datasets/{}/resources/{}/".format(dataset_id, resource_id)
        r = requests.get(url, headers=headers)
        extras = r.json()["extras"]
    except:
        extras = {}

    extras["consolidation_schema:update_schema"] = schema_name

    obj = {"schema": schema, "extras": extras}

    url = api_url + "datasets/{}/resources/{}/".format(dataset_id, resource_id)
    response = requests.put(url, json=obj, headers=headers)

    if response.status_code != 200:
        print(
            "🔴 Resource schema could not be updated. Dataset ID: {} - Resource ID: {}".format(
                dataset_id, resource_id
            )
        )

    return response.status_code == 200


# Function that deletes a schema (version) metadata on a resource
def delete_resource_schema(
    api_url: str,
    dataset_id: str,
    resource_id: str,
    schema_name: str,
    headers: Dict[str, str],
) -> bool:
    schema = {}

    try:
        url = api_url + "datasets/{}/resources/{}/".format(dataset_id, resource_id)
        r = requests.get(url, headers=headers)
        extras = r.json()["extras"]
    except:
        extras = {}

    extras["consolidation_schema:remove_schema"] = schema_name

    obj = {"schema": schema, "extras": extras}

    url = api_url + "datasets/{}/resources/{}/".format(dataset_id, resource_id)
    response = requests.put(url, json=obj, headers=headers)

    if response.status_code != 200:
        print(
            "🔴 Resource schema could not be deleted. Dataset ID: {} - Resource ID: {}".format(
                dataset_id, resource_id
            )
        )

    return response.status_code == 200


# Get the (list of) e-mail address(es) of the owner or of the admin(s) of the owner organization of a dataset
def get_owner_or_admin_mails(dataset_id, api_url):
    r = requests.get(api_url + "datasets/{}/".format(dataset_id))
    r_dict = r.json()

    if r_dict["organization"] is not None:
        org_id = r_dict["organization"]["id"]
    else:
        org_id = None

    if r_dict["owner"] is not None:
        owner_id = r_dict["owner"]["id"]
    else:
        owner_id = None

    mails_type = None
    mails_list = []

    if org_id is not None:
        mails_type = "organisation_admins"
        r_org = requests.get(api_url + "organizations/{}/".format(org_id))
        members_list = r_org.json()["members"]
        for member in members_list:
            if member["role"] == "admin":
                user_id = member["user"]["id"]
                r_user = requests.get(
                    api_url + "users/{}/".format(user_id), headers=HEADER
                )
                user_mail = r_user.json()["email"]
                mails_list += [user_mail]

    else:
        if owner_id is not None:
            mails_type = "owner"
            r_user = requests.get(
                api_url + "users/{}/".format(owner_id), headers=HEADER
            )
            user_mail = r_user.json()["email"]
            mails_list += [user_mail]

    return (mails_type, mails_list)


# Function to send a e-mail
def send_email(
    subject, message, mail_from, mail_to, smtp_host, smtp_user, smtp_password
):
    message = emails.html(
        html="<p>%s</p>" % message, subject=subject, mail_from=mail_from
    )
    smtp = {
        "host": smtp_host,
        "port": 587,
        "tls": True,
        "user": smtp_user,
        "password": smtp_password,
    }

    _ = message.send(to=mail_to, smtp=smtp)

    return _


# Function to post a comment on a dataset
def post_comment_on_dataset(dataset_id, title, comment, api_url):
    global HEADER

    post_object = {
        "title": title,
        "comment": comment,
        "subject": {"class": "Dataset", "id": dataset_id},
    }

    _ = requests.post(api_url + "discussions/", json=post_object, headers=HEADER)

    return _


def add_validation_extras(
    dataset_id, resource_id, validata_report_path, api_url, headers
):
    if os.path.isfile(validata_report_path):
        with open(validata_report_path) as out:
            validation_report = json.load(out)

        try:
            url = api_url + "datasets/{}/resources/{}/".format(dataset_id, resource_id)
            r = requests.get(url, headers=headers)
            extras = r.json()["extras"]
        except:
            extras = {}

        extras = {**extras, **validation_report}

        obj = {"extras": extras}

        url = api_url + "datasets/{}/resources/{}/".format(dataset_id, resource_id)
        response = requests.put(url, json=obj, headers=headers)

        if response.status_code != 200:
            print(
                "🔴 Schema could not be added on resource. Dataset ID: {} - Resource ID: {}".format(
                    dataset_id, resource_id
                )
            )

        return response.status_code == 200


def upload_geojson(
    api_url: str,
    api_key: str,
    config_path: str,
    schema_consolidated_data_path: str,
    consolidation_date_str: str,
    schema_name: str,
) -> bool:
    headers = {
        "X-API-KEY": api_key,
    }

    with open(config_path, "r") as f:
        config_dict = yaml.safe_load(f)

    geojson_version_names_list = sorted(
        [
            filename.replace(
                "consolidation_" + schema_name.replace("/", "_") + "_v_",
                "",
            ).replace("_" + consolidation_date_str + ".json", "")
            for filename in os.listdir(schema_consolidated_data_path)
            if filename.endswith(".json") and not filename.startswith(".")
        ]
    )
    if len(geojson_version_names_list) > 1:
        print("Warning: multiple versions of GeoJSON found for {}".format(schema_name))

    geojson_path = os.path.join(
        schema_consolidated_data_path,
        "consolidation_{}_v_{}_{}.json".format(
            schema_name.replace("/", "_"),
            geojson_version_names_list[-1],
            consolidation_date_str,
        ),
    )

    # Uploading file
    consolidated_dataset_id = config_dict[schema_name]["consolidated_dataset_id"]
    r_id = config_dict[schema_name]["geojson_resource_id"]
    url = (
        api_url
        + "datasets/"
        + consolidated_dataset_id
        + "/resources/"
        + r_id
        + "/upload/"
    )
    expected_status_code = 200

    with open(geojson_path, "rb") as file:
        files = {"file": (geojson_path.split("/")[-1], file.read())}

    response = requests.post(url, files=files, headers=headers)

    if response.status_code != expected_status_code:
        print("{} --- ⚠️: GeoJSON file could not be uploaded.".format(datetime.today()))
    else:
        obj = {}
        obj["type"] = "main"
        obj["title"] = "Export au format geojson"
        obj["format"] = "json"

        r_url = api_url + "datasets/{}/resources/{}/".format(
            consolidated_dataset_id, r_id
        )
        r_response = requests.put(r_url, json=obj, headers=headers)

        if r_response.status_code == 200:
            print(
                "{} --- ✅ Successfully updated GeoJSON file with metadata.".format(
                    datetime.today()
                )
            )
        else:
            print(
                "{} --- ⚠️: file uploaded but metadata could not be updated.".format(
                    datetime.today()
                )
            )


def run_consolidation_upload(
    api_url: str,
    api_key: str,
    tmp_folder: str,
    working_dir: str,
    date_airflow: str,
    schema_catalog: str,
    output_data_folder: str,
    tmp_config_file: str,
) -> None:
    headers = {
        "X-API-KEY": api_key,
    }

    # MAIL PARAMETERS
    # smtp_host = os.environ['SCHEMA_BOT_MAIL_SMTP']
    # smtp_user = os.environ['SCHEMA_BOT_MAIL_USER']
    # smtp_password = os.environ['SCHEMA_BOT_MAIL_PASSWORD']

    tmp_path = Path(tmp_folder)

    consolidated_data_path = tmp_path / "consolidated_data"
    consolidated_data_path.mkdir(parents=True, exist_ok=True)

    ref_tables_path = tmp_path / "ref_tables"
    ref_tables_path.mkdir(parents=True, exist_ok=True)

    report_tables_path = tmp_path / "report_tables"
    report_tables_path.mkdir(parents=True, exist_ok=True)

    validata_reports_path = tmp_path / "validata_reports"
    validata_reports_path.mkdir(parents=True, exist_ok=True)

    current_path = Path(working_dir)

    config_path = Path(tmp_config_file)

    with open(config_path, "r") as f:
        config_dict = yaml.safe_load(f)

    with open(tmp_path / "schemas_report_dict.pickle", "rb") as f:
        schemas_report_dict = pickle.load(f)

    consolidation_date_str = date_airflow.replace("-", "")

    schemas_list_url = schema_catalog
    schemas_catalogue_dict = requests.get(schemas_list_url).json()
    schemas_catalogue_list = [
        schema
        for schema in schemas_catalogue_dict["schemas"]
        if schema["schema_type"] == "tableschema"
    ]

    config_dict = remove_old_schemas(config_dict, schemas_catalogue_list)

    # ## Upload
    for schema_name in config_dict.keys():
        with open(config_path, "r") as f:
            config_dict = yaml.safe_load(f)

        config_dict = remove_old_schemas(config_dict, schemas_catalogue_list)

        print("{} - ℹ️ STARTING SCHEMA: {}".format(datetime.now(), schema_name))

        schema_consolidated_data_path = Path(
            consolidated_data_path
        ) / schema_name.replace("/", "_")

        if os.path.exists(schema_consolidated_data_path):
            # Check if dataset_id is in config. If not, create a dataset on datagouv
            schema_config = config_dict[schema_name]
            if ("publication" in schema_config.keys()) and schema_config[
                "publication"
            ] == True:
                if "consolidated_dataset_id" not in schema_config.keys():
                    response = create_schema_consolidation_dataset(
                        schema_name, schemas_catalogue_list, api_url, headers
                    )
                    if response.status_code == 201:
                        consolidated_dataset_id = response.json()["id"]
                        update_config_file(
                            schema_name,
                            "consolidated_dataset_id",
                            consolidated_dataset_id,
                            config_path,
                        )
                        print(
                            "{} -- 🟢 No consolidation dataset for this schema - Successfully created (id: {})".format(
                                datetime.today(), consolidated_dataset_id
                            )
                        )
                    else:
                        print(
                            "{} -- 🔴 No consolidation dataset for this schema - Failed to create one".format(
                                datetime.today()
                            )
                        )
                else:
                    consolidated_dataset_id = schema_config["consolidated_dataset_id"]

                schemas_report_dict[schema_name][
                    "consolidated_dataset_id"
                ] = consolidated_dataset_id

                # Creating last consolidation resources
                version_names_list = [
                    filename.replace(
                        "consolidation_" + schema_name.replace("/", "_") + "_v_",
                        "",
                    ).replace("_" + consolidation_date_str + ".csv", "")
                    for filename in os.listdir(schema_consolidated_data_path)
                    if filename.endswith(".csv") and not filename.startswith(".")
                ]

                for version_name in sorted(version_names_list):
                    with open(config_path, "r") as f:
                        config_dict = yaml.safe_load(f)
                        config_dict = remove_old_schemas(
                            config_dict, schemas_catalogue_list
                        )

                    schema = {"name": schema_name, "version": version_name}
                    obj = {}
                    obj["schema"] = schema
                    obj["type"] = "main"
                    obj[
                        "title"
                    ] = "Dernière version consolidée (v{} du schéma) - {}".format(
                        version_name, consolidation_date_str
                    )
                    obj["format"] = "csv"

                    file_path = os.path.join(
                        schema_consolidated_data_path,
                        "consolidation_{}_v_{}_{}.csv".format(
                            schema_name.replace("/", "_"),
                            version_name,
                            consolidation_date_str,
                        ),
                    )

                    # Uploading file (creating a new resource if version was not there before)
                    try:
                        r_id = config_dict[schema_name]["latest_resource_ids"][
                            version_name
                        ]
                        url = (
                            api_url
                            + "datasets/"
                            + consolidated_dataset_id
                            + "/resources/"
                            + r_id
                            + "/upload/"
                        )
                        r_to_create = False
                        expected_status_code = 200

                    except KeyError:
                        url = (
                            api_url + "datasets/" + consolidated_dataset_id + "/upload/"
                        )
                        r_to_create = True
                        expected_status_code = 201

                    with open(file_path, "rb") as file:
                        files = {"file": (file_path.split("/")[-1], file.read())}

                    response = requests.post(url, files=files, headers=headers)

                    if response.status_code == expected_status_code:
                        if r_to_create == True:
                            r_id = response.json()["id"]
                            update_config_version_resource_id(
                                schema_name, version_name, r_id, config_path
                            )
                            print(
                                "{} --- ➕ New latest resource ID created for {} v{} (id: {})".format(
                                    datetime.today(),
                                    schema_name,
                                    version_name,
                                    r_id,
                                )
                            )
                    else:
                        r_id = None
                        print(
                            "{} --- ⚠️ Version {}: file could not be uploaded.".format(
                                datetime.today(), version_name
                            )
                        )

                    if r_id is not None:
                        r_url = api_url + "datasets/{}/resources/{}/".format(
                            consolidated_dataset_id, r_id
                        )
                        r_response = requests.put(r_url, json=obj, headers=headers)

                        if r_response.status_code == 200:
                            if r_to_create == True:
                                print(
                                    "{} --- ✅ Version {}: Successfully created consolidated file.".format(
                                        datetime.today(), version_name
                                    )
                                )
                            else:
                                print(
                                    "{} --- ✅ Version {}: Successfully updated consolidated file.".format(
                                        datetime.today(), version_name
                                    )
                                )
                        else:
                            print(
                                "{} --- ⚠️ Version {}: file uploaded but metadata could not be updated.".format(
                                    datetime.today(), version_name
                                )
                            )

                # Update IRVE GeoJSON file
                if schema_name == "etalab/schema-irve-statique":
                    upload_geojson(
                        api_url,
                        api_key,
                        config_path,
                        schema_consolidated_data_path,
                        consolidation_date_str,
                        schema_name,
                    )
            else:
                schemas_report_dict[schema_name]["consolidated_dataset_id"] = np.nan
                print(
                    "{} -- ❌ No publication for this schema.".format(datetime.today())
                )

        else:
            schemas_report_dict[schema_name]["consolidated_dataset_id"] = np.nan
            print(
                "{} -- ❌ No consolidated file for this schema.".format(datetime.today())
            )

    # Reopening config file to update config_dict (in case it has to be reused right after)
    with open(config_path, "r") as f:
        config_dict = yaml.safe_load(f)
        config_dict = remove_old_schemas(config_dict, schemas_catalogue_list)

    # ## Schemas (versions) feedback loop on resources
    # ### Adding needed infos for each resource in reference tables

    for schema_name in config_dict.keys():
        ref_table_path = os.path.join(
            ref_tables_path,
            "ref_table_{}.csv".format(schema_name.replace("/", "_")),
        )

        if os.path.isfile(ref_table_path):
            df_ref = pd.read_csv(ref_table_path)

            df_ref = add_most_recent_valid_version(df_ref)
            df_ref["is_schema_version_to_update"] = df_ref.apply(
                is_schema_version_to_update, axis=1
            )
            df_ref["is_schema_to_add"] = df_ref.apply(is_schema_to_add, axis=1)
            df_ref["is_schema_to_drop"] = df_ref.apply(is_schema_to_drop, axis=1)

            df_ref.to_csv(ref_table_path, index=False)

            print(
                "{} - ✅ Infos added for schema {}".format(datetime.today(), schema_name)
            )

        else:
            print(
                "{} - ❌ No reference table for schema {}".format(
                    datetime.today(), schema_name
                )
            )

    # ### Updating resources schemas and sending comments/mails to notify producers
    # ⚠️⚠️⚠️ **TODO: UNCOMMENT MAIL SENDING AND DISCUSSION COMMENTING (+ DELETE PRINTS) FOR NOTIFICATION TO PRODUCERS.**

    for schema_name in config_dict.keys():
        ref_table_path = os.path.join(
            ref_tables_path,
            "ref_table_{}.csv".format(schema_name.replace("/", "_")),
        )

        if os.path.isfile(ref_table_path):
            df_ref = pd.read_csv(ref_table_path)
            df_ref["resource_schema_update_success"] = np.nan
            df_ref["producer_notification_success"] = np.nan

            for idx, row in df_ref.iterrows():
                if row["is_schema_version_to_update"]:
                    resource_update_success = update_resource_schema(
                        api_url,
                        row["dataset_id"],
                        row["resource_id"],
                        schema_name,
                        row["most_recent_valid_version"],
                        headers,
                    )
                    df_ref.loc[
                        (df_ref["resource_id"] == row["resource_id"]),
                        "resource_schema_update_success",
                    ] = resource_update_success

                    if resource_update_success == True:
                        title = "Mise à jour de la version de la métadonnée schéma"
                        comment = updated_schema_comment_template.format(
                            resource_title=row["resource_title"],
                            schema_name=schema_name,
                            initial_version_name=row["initial_version_name"],
                            most_recent_valid_version=row["most_recent_valid_version"],
                        )
                        # comment_post = post_comment_on_dataset(dataset_id=row['dataset_id'],
                        #                                       title=title,
                        #                                       comment=comment,
                        #                                       api_url=api_url
                        #                                      )
                        #
                        # producer_notification_success = (comment_post.status_code == 201)

                        # df_ref.loc[(df_ref['resource_id'] == row['resource_id']), 'producer_notification_success'] = producer_notification_success
                        # No notification at the moment:
                        df_ref.loc[
                            (df_ref["resource_id"] == row["resource_id"]),
                            "producer_notification_success",
                        ] = False

                elif row["is_schema_to_add"]:
                    resource_update_success = add_resource_schema(
                        api_url,
                        row["dataset_id"],
                        row["resource_id"],
                        schema_name,
                        row["most_recent_valid_version"],
                        headers,
                    )
                    df_ref.loc[
                        (df_ref["resource_id"] == row["resource_id"]),
                        "resource_schema_update_success",
                    ] = resource_update_success

                    if resource_update_success == True:
                        title = "Ajout de la métadonnée schéma"
                        comment = added_schema_comment_template.format(
                            resource_title=row["resource_title"],
                            schema_name=schema_name,
                            most_recent_valid_version=row["most_recent_valid_version"],
                        )
                        # comment_post = post_comment_on_dataset(dataset_id=row['dataset_id'],
                        #                                       title=title,
                        #                                       comment=comment,
                        #                                       api_url=api_url
                        #                                      )
                        #
                        # producer_notification_success = (comment_post.status_code == 201)
                        # df_ref.loc[(df_ref['resource_id'] == row['resource_id']), 'producer_notification_success'] = producer_notification_success
                        # No notification at the moment:
                        df_ref.loc[
                            (df_ref["resource_id"] == row["resource_id"]),
                            "producer_notification_success",
                        ] = False

                # Right now, we don't drop schema and do no notification
                elif row["is_schema_to_drop"]:
                    #    resource_update_success = delete_resource_schema(api_url, row['dataset_id'], row['resource_id'], schema_name, headers)
                    #    df_ref.loc[(df_ref['resource_id'] == row['resource_id']), 'resource_schema_update_success'] = resource_update_success
                    #
                    #    if resource_update_success == True :
                    #        title = 'Suppression de la métadonnée schéma'
                    #
                    #        mails_type, mails_list = get_owner_or_admin_mails(row['dataset_id'], api_url)
                    #
                    #        if len(mails_list) > 0 : #If we found some email addresses, we send mails
                    #
                    #            if mails_type == 'organisation_admins' :
                    #                message = deleted_schema_mail_template_org.format(organisation_name=row['organization_or_owner'],
                    #                                                                  dataset_title=row['dataset_title'],
                    #                                                                  resource_title=row['resource_title'],
                    #                                                                  schema_name=schema_name,
                    #                                                                  schema_url=get_schema_dict(schema_name, schemas_catalogue_list)['schema_url'],
                    #                                                                  resource_url=row['resource_url']
                    #                                                                 )
                    #            elif mails_type == 'owner' :
                    #                message = deleted_schema_mail_template_own.format(dataset_title=row['dataset_title'],
                    #                                                                  resource_title=row['resource_title'],
                    #                                                                  schema_name=schema_name,
                    #                                                                  schema_url=get_schema_dict(schema_name, schemas_catalogue_list)['schema_url'],
                    #                                                                  resource_url=row['resource_url']
                    #                                                                 )
                    #
                    #
                    #            #Sending mail
                    #
                    #            producer_notification_success_list = []
                    #            print('- {} | {}:'.format(row['dataset_title'], row['resource_title']))
                    #            for mail_to in mails_list :
                    #                #mail_send = send_email(subject=title,
                    #                #                       message=message,
                    #                #                       mail_from=mail_from,
                    #                #                       mail_to=mail_to,
                    #                #                       smtp_host=smtp_host,
                    #                #                       smtp_user=smtp_user,
                    #                #                       smtp_password=smtp_password)

                    #                #producer_notification_success_list += [(mail_send.status_code == 250)]
                    #
                    #            #producer_notification_success = any(producer_notification_success_list) # Success if at least one person receives the mail
                    #
                    #        else : #If no mail address, we post a comment on dataset
                    #            comment = deleted_schema_comment_template.format(resource_title=row['resource_title'],
                    #                                                             schema_name=schema_name,
                    #                                                             schema_url=get_schema_dict(schema_name, schemas_catalogue_list)['schema_url'],
                    #                                                             resource_url=row['resource_url']
                    #                                                            )
                    #
                    #            #comment_post = post_comment_on_dataset(dataset_id=row['dataset_id'],
                    #            #                                       title=title,
                    #            #                                       comment=comment,
                    #            #                                       api_url=api_url
                    #            #                                      )
                    #
                    #            #producer_notification_success = (comment_post.status_code == 201)
                    #
                    #        #df_ref.loc[(df_ref['resource_id'] == row['resource_id']), 'producer_notification_success'] = producer_notification_success

                    # TO DROP when schema will be deleted and producer notified:
                    df_ref.loc[
                        (df_ref["resource_id"] == row["resource_id"]),
                        "resource_schema_update_success",
                    ] = False
                    df_ref.loc[
                        (df_ref["resource_id"] == row["resource_id"]),
                        "producer_notification_success",
                    ] = False

            df_ref.to_csv(ref_table_path, index=False)

            print(
                "{} - ✅ Resources updated for schema {}".format(
                    datetime.today(), schema_name
                )
            )

        else:
            print(
                "{} - ❌ No reference table for schema {}".format(
                    datetime.today(), schema_name
                )
            )

    # ### Add validata report to extras for each resource
    for schema_name in config_dict.keys():
        ref_table_path = os.path.join(
            ref_tables_path,
            "ref_table_{}.csv".format(schema_name.replace("/", "_")),
        )

        if os.path.isfile(ref_table_path):
            df_ref = pd.read_csv(ref_table_path)
            df_ref["resource_schema_update_success"] = np.nan
            df_ref["producer_notification_success"] = np.nan

            for idx, row in df_ref.iterrows():
                validata_report_path = (
                    str(validata_reports_path)
                    + "/"
                    + row["dataset_id"]
                    + "_"
                    + row["resource_id"]
                    + "_"
                )

                # If there is a valid version, put validata report from it
                if row["most_recent_valid_version"] == row["most_recent_valid_version"]:
                    validata_report_path += row["most_recent_valid_version"] + ".json"
                # Else, check if declarative version
                else:
                    # If so, put validation report from it
                    if row["initial_version_name"] == row["initial_version_name"]:
                        validata_report_path += row["initial_version_name"] + ".json"
                    # If not, put validation report from latest version
                    else:
                        validata_report_path += (
                            max(
                                [
                                    x.replace("is_valid_v_", "")
                                    for x in list(row.keys())
                                    if "is_valid_v_" in x
                                ]
                            )
                            + ".json"
                        )

                add_validation_extras(
                    row["dataset_id"],
                    row["resource_id"],
                    validata_report_path,
                    api_url,
                    headers,
                )

    # ## Updating consolidation documentation resource

    for schema_name in config_dict.keys():
        ref_table_path = os.path.join(
            ref_tables_path,
            "ref_table_{}.csv".format(schema_name.replace("/", "_")),
        )

        with open(config_path, "r") as f:
            config_dict = yaml.safe_load(f)
            config_dict = remove_old_schemas(config_dict, schemas_catalogue_list)

        print("{} - ℹ️ STARTING SCHEMA: {}".format(datetime.now(), schema_name))

        schema_config = config_dict[schema_name]
        if ("publication" in schema_config.keys()) and schema_config[
            "publication"
        ] == True:
            if os.path.isfile(ref_table_path):
                if "consolidated_dataset_id" in schema_config.keys():
                    consolidated_dataset_id = schema_config["consolidated_dataset_id"]

                    obj = {}
                    obj["type"] = "documentation"
                    obj["title"] = "Documentation sur la consolidation - {}".format(
                        consolidation_date_str
                    )

                    # Uploading documentation file (creating a new resource if version was not there before)
                    try:
                        doc_r_id = config_dict[schema_name]["documentation_resource_id"]
                        url = (
                            api_url
                            + "datasets/"
                            + consolidated_dataset_id
                            + "/resources/"
                            + doc_r_id
                            + "/upload/"
                        )
                        doc_r_to_create = False
                        expected_status_code = 200

                    except KeyError:
                        url = (
                            api_url + "datasets/" + consolidated_dataset_id + "/upload/"
                        )
                        doc_r_to_create = True
                        expected_status_code = 201

                    with open(ref_table_path, "rb") as file:
                        files = {
                            "file": (
                                ref_table_path.split("/")[-1],
                                file.read(),
                            )
                        }

                    response = requests.post(url, files=files, headers=headers)

                    if response.status_code == expected_status_code:
                        if doc_r_to_create == True:
                            doc_r_id = response.json()["id"]
                            update_config_file(
                                schema_name,
                                "documentation_resource_id",
                                doc_r_id,
                                config_path,
                            )
                            print(
                                "{} --- ➕ New documentation resource ID created for {} (id: {})".format(
                                    datetime.today(), schema_name, doc_r_id
                                )
                            )
                    else:
                        doc_r_id = None
                        print(
                            "{} --- ⚠️ Documentation file could not be uploaded.".format(
                                datetime.today()
                            )
                        )

                    if doc_r_id is not None:
                        doc_r_url = api_url + "datasets/{}/resources/{}/".format(
                            consolidated_dataset_id, doc_r_id
                        )
                        doc_r_response = requests.put(
                            doc_r_url, json=obj, headers=headers
                        )
                        if doc_r_response.status_code == 200:
                            if doc_r_to_create == True:
                                print(
                                    "{} --- ✅ Successfully created documentation file.".format(
                                        datetime.today()
                                    )
                                )
                            else:
                                print(
                                    "{} --- ✅ Successfully updated documentation file.".format(
                                        datetime.today()
                                    )
                                )
                        else:
                            print(
                                "{} --- ⚠️ Documentation file uploaded but metadata could not be updated.".format(
                                    datetime.today()
                                )
                            )

                else:
                    print(
                        "{} -- ❌ No consolidation dataset ID for this schema.".format(
                            datetime.today()
                        )
                    )

            else:
                print(
                    "{} -- ❌ No reference table for this schema.".format(
                        datetime.today()
                    )
                )

        else:
            print("{} -- ❌ No publication for this schema.".format(datetime.today()))

    # Reopening config file to update config_dict (in case it has to be reused right after)
    with open(config_path, "r") as f:
        config_dict = yaml.safe_load(f)
        config_dict = remove_old_schemas(config_dict, schemas_catalogue_list)

    # ## Consolidation Reports
    # ### Report by schema
    reports_list = []

    for schema_name in schemas_report_dict.keys():
        schema_report_dict = schemas_report_dict[schema_name]
        schema_report_dict["schema_name"] = schema_name
        reports_list += [schema_report_dict]

    reports_df = pd.DataFrame(reports_list)

    reports_df = reports_df[
        ["schema_name"] + [col for col in reports_df.columns if col != "schema_name"]
    ].rename(
        columns={"config_created": "new_config_created"}
    )  # rename to drop at next launch

    stats_df_list = []
    for schema_name in config_dict.keys():
        ref_table_path = os.path.join(
            ref_tables_path,
            "ref_table_{}.csv".format(schema_name.replace("/", "_")),
        )

        if os.path.isfile(ref_table_path):
            df_ref = pd.read_csv(ref_table_path)
            df_ref["schema_name"] = schema_name
            df_ref["is_schema_version_updated"] = (
                df_ref["is_schema_version_to_update"]
                & df_ref["resource_schema_update_success"]
            )
            df_ref["is_schema_added"] = (
                df_ref["is_schema_to_add"] & df_ref["resource_schema_update_success"]
            )
            df_ref["is_schema_dropped"] = (
                df_ref["is_schema_to_drop"] & df_ref["resource_schema_update_success"]
            )
            df_ref["resource_schema_update_success"] = False
            df_ref.to_csv(ref_table_path, index=False)
            stats_df_list += [
                df_ref[
                    [
                        "schema_name",
                        "is_schema_version_to_update",
                        "is_schema_to_add",
                        "is_schema_to_drop",
                        "resource_schema_update_success",
                        "is_schema_version_updated",
                        "is_schema_added",
                        "is_schema_dropped",
                    ]
                ]
                .fillna(False)
                .groupby("schema_name")
                .sum()
                .reset_index()
            ]

    stats_df = pd.concat(stats_df_list).reset_index(drop=True)

    reports_df = reports_df.merge(stats_df, on="schema_name", how="left")

    reports_df.head()

    reports_df.to_excel(
        os.path.join(
            report_tables_path,
            "report_by_schema_{}.xlsx".format(consolidation_date_str),
        ),
        index=False,
    )
    reports_df.to_csv(
        os.path.join(
            report_tables_path,
            "report_by_schema_{}.csv".format(consolidation_date_str),
        ),
        index=False,
    )

    # ## Detailed reports (by schema and resource source)
    for schema_name in config_dict.keys():
        ref_table_path = os.path.join(
            ref_tables_path,
            "ref_table_{}.csv".format(schema_name.replace("/", "_")),
        )

        if os.path.isfile(ref_table_path):
            df_ref = pd.read_csv(ref_table_path)

            df_ref["total_nb_resources"] = 1
            df_ref["error_type"].fillna("no-error", inplace=True)

            cols_to_sum = ["total_nb_resources"]
            cols_to_sum += [col for col in df_ref.columns if col.startswith("is_")]
            df_report = (
                df_ref.groupby(["resource_found_by", "error_type"])
                .agg({col: sum for col in cols_to_sum})
                .reset_index()
            )

            df_report.to_excel(
                os.path.join(
                    report_tables_path,
                    "report_table_{}.xlsx".format(schema_name.replace("/", "_")),
                ),
                index=False,
            )

            print(
                "{} - ✅ Report done for schema {}".format(datetime.today(), schema_name)
            )

        else:
            print(
                "{} - ❌ No reference table for schema {}".format(
                    datetime.today(), schema_name
                )
            )

    # %%
    shutil.move(tmp_folder + "consolidated_data", output_data_folder)
    shutil.move(tmp_folder + "ref_tables", output_data_folder)
    shutil.move(tmp_folder + "report_tables", output_data_folder)
