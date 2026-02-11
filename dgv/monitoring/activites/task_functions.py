from datetime import timedelta, datetime, time as dtime, timezone
from difflib import SequenceMatcher
import logging
from time import sleep

from airflow.decorators import task
from airflow.models import Variable
from langdetect import detect, LangDetectException
import pandas as pd
import requests
from unidecode import unidecode

from datagouvfr_data_pipelines.config import (
    MATTERMOST_DATAGOUV_ACTIVITES,
    MATTERMOST_DATAGOUV_SCHEMA_ACTIVITE,
    MATTERMOST_MODERATION_NOUVEAUTES,
)
from datagouvfr_data_pipelines.utils.mattermost import send_message
from datagouvfr_data_pipelines.utils.datagouv import (
    VALIDATA_BASE_URL,
    get_last_items,
    get_latest_comments,
    get_awaiting_spam_comments,
    check_duplicated_orga,
    local_client,
    SPAM_WORDS,
)
from datagouvfr_data_pipelines.utils.utils import check_if_monday, time_is_between
from datagouvfr_data_pipelines.utils.grist import GristTable


DAG_NAME = "dgv_notification_activite"

TIME_PERIOD = {"minutes": 5}
entreprises_api_url = "https://recherche-entreprises.api.gouv.fr/search?q="
grist_curation = "muvJRZ9cTGep"


def detect_spam(name, description):
    if not (name or description):
        return
    for spam in SPAM_WORDS:
        for field in [name, description]:
            if field and unidecode(spam) in unidecode(field.lower()):
                return unidecode(spam)
    if not description or len(description) < 30:
        return
    else:
        try:
            lang = detect(description.lower())
            if lang not in ["fr", "ca"]:
                return "language:" + lang
        except LangDetectException:
            return
    return


def detect_potential_certif(siret):
    if siret is None:
        return False
    try:
        r = requests.get(entreprises_api_url + siret).json()
    except Exception:
        sleep(1)
        r = requests.get(entreprises_api_url + siret).json()
    if len(r["results"]) == 0:
        logging.warning("No match for: ", siret)
        return False
    if len(r["results"]) > 1:
        logging.warning("Ambiguous: ", siret)
    complements = r["results"][0]["complements"]
    return bool(
        complements["collectivite_territoriale"] or complements["est_service_public"]
    )


def check_new(object_type: str, ti):
    # we want everything that happened since this date
    start_date = datetime.now() - timedelta(**TIME_PERIOD)
    end_date = datetime.now()
    items = get_last_items(object_type, start_date, end_date)
    # items = get_last_items(templates_dict['type'], start_date)
    ti.xcom_push(key="nb", value=str(len(items)))
    arr = []
    for item in items:
        mydict = {}
        # when dataservices' metadata are harmonized we can change this back
        for k in ["name", "title", "page", "self_web_url"]:
            if k in item:
                mydict[k.replace("self_web_url", "page")] = item[k]
        # add field to check if it's the first publication of this type
        # for this organization/user, and check for potential spam
        mydict["duplicated"] = False
        mydict["potential_certif"] = False
        if object_type != "organizations":
            mydict["spam"] = False
            # if certified orga, no spam check
            badges = (
                item["organization"].get("badges", [])
                if item.get("organization", None)
                else []
            )
            if "certified" not in [badge["kind"] for badge in badges]:
                mydict["spam"] = detect_spam(item["title"], item["description"])
            if item.get("organization"):
                owner = requests.get(
                    f"https://data.gouv.fr/api/1/organizations/{item['organization']['id']}/"
                ).json()
                mydict["owner_type"] = "organization"
                mydict["owner_name"] = owner["name"]
                mydict["owner_id"] = owner["id"]
            elif item.get("owner"):
                owner = requests.get(
                    f"https://data.gouv.fr/api/1/users/{item['owner']['id']}/"
                ).json()
                mydict["owner_type"] = "user"
                mydict["owner_name"] = owner["slug"]
                mydict["owner_id"] = owner["id"]
            else:
                mydict["owner_type"] = None
            if (
                mydict["owner_type"]
                and owner["metrics"].get(object_type, 0) < 2
            ):
                # if it's a dataset and it's labelled with a schema and not potential spam, no ping
                # NB: this is to prevent being pinged for entities publishing small data (IRVE, LOM...)
                if (
                    object_type == "datasets"
                    and any([r["schema"] for r in item.get("resources")])
                    and not mydict["spam"]
                ):
                    logging.info("This dataset has a schema:", item)
                    mydict["first_publication"] = False
                else:
                    # private/draft objects are not counted in metrics, so we can't be sure they're new
                    # also not counting objects that have been deleted since
                    mydict["first_publication"] = (
                        not item.get("private")
                        and not item.get("deleted")
                        and not item.get("deleted_at")
                    )
            else:
                mydict["first_publication"] = False
        else:
            mydict["spam"] = detect_spam(item.get("name"), item.get("description"))
            if mydict["spam"]:
                mydict["description"] = item.get("description")
            mydict["potential_certif"] = detect_potential_certif(
                item["business_number_id"]
            )
            # checking for potential duplicates in organization creation
            mydict["duplicated"] = check_duplicated_orga(item["slug"]) is not None
        arr.append(mydict)
    ti.xcom_push(key=object_type, value=arr)


@task()
def get_inactive_orgas(cutoff_days=30, days_before_flag=7):
    # DAG runs every 5min, we want this to run every Monday at ~10:00
    start, end = dtime(9, 1, 0), dtime(9, 6, 0)
    if not (check_if_monday() and time_is_between(start, end)):
        logging.info("Not running now")
        return
    orgas = local_client.get_all_from_api_query(
        "api/1/organizations/?sort=-created",
        mask="data{id,metrics,created_at,name}",
    )
    inactive = {}
    threshold = (datetime.today() - timedelta(days=cutoff_days)).strftime("%Y-%m-%d")
    too_soon = (datetime.today() - timedelta(days=days_before_flag)).strftime(
        "%Y-%m-%d"
    )
    for o in orgas:
        if o["created_at"] < threshold:
            logging.info("Too old:", o["id"])
            break
        if o["created_at"] >= too_soon:
            logging.info("Too recent:", o["id"])
            continue
        if all(o["metrics"][k] == 0 for k in ["datasets", "reuses"]):
            inactive[o["id"]] = o["name"]
    if inactive:
        message = (
            f"#### :sloth: Organisations créées il y a entre {days_before_flag} et "
            f"{cutoff_days} jours, sans dataset ni réutilisation\n- "
        )
        message += "\n- ".join(
            [
                f"[{n}](https://www.data.gouv.fr/organizations/{i}/)"
                for i, n in inactive.items()
            ]
        )
        send_message(message, MATTERMOST_MODERATION_NOUVEAUTES)


@task()
def alert_if_awaiting_spam_comments():
    # DAG runs every 5min, we want this to run everyday at ~11:00
    start, end = dtime(8, 1, 0), dtime(8, 6, 0)
    if not time_is_between(start, end):
        logging.info("Not running now")
        return
    comments = get_awaiting_spam_comments()
    if comments:
        n = len(comments)
        message = (
            f"@all Il y a {n} commentaire{'s' if n > 1 else ''} en attente "
            "de validation (voir [ici](https://www.data.gouv.fr/api/1/spam/))"
        )
        send_message(message, MATTERMOST_MODERATION_NOUVEAUTES)


@task()
def alert_if_new_reports():
    # DAG runs every 5min but if it fails we catch up with this variable
    previous_report_check = Variable.get(
        "previous_report_check",
        (datetime.now(timezone.utc) - timedelta(**TIME_PERIOD)).isoformat(),
    )
    reports = local_client.get_all_from_api_query("api/1/reports/")
    unseen_reports = [r for r in reports if r["reported_at"] >= previous_report_check]
    if not unseen_reports:
        return
    Variable.set("previous_report_check", datetime.now(timezone.utc).isoformat())
    message = ":triangular_flag_on_post: @all De nouveaux signalements ont été faits :"
    for r in unseen_reports:
        if r["by"]:
            by = f"[{r['by']['slug']}]({r['by']['page']})"
        else:
            by = "un utilisateur non connecté"
        subject = (
            f"https://www.data.gouv.fr/api/1/{r['subject']['class'].lower()}s/"
            f"{r['subject']['id']}/"
        )
        _ = requests.get(subject)
        try:
            _.raise_for_status()
            _ = _.json()
            subject = (
                f"[cet objet]({_.get('page') or _.get('self_web_url ')}): "
                f"{r['subject']['class']} `{_.get('title') or _.get('name')}`"
            )
        except requests.exceptions.HTTPError:
            subject = f"[cet objet qui a été supprimé depuis]({subject})"
        user_message = r["message"].replace("\n", " ")
        message += f"\n- par {by}, pour `{r['reason']}`, au sujet de {subject} avec le message suivant : `{user_message}`"
    send_message(message, MATTERMOST_MODERATION_NOUVEAUTES)


@task()
def check_new_comments(**context):
    latest_comments = get_latest_comments(
        start_date=datetime.now() - timedelta(**TIME_PERIOD)
    )
    spam_comments = [
        k
        for k in latest_comments
        if detect_spam("", k["comment"]["content"].replace("\n", " "))
    ]
    context["ti"].xcom_push(key="spam_comments", value=spam_comments)


def similar(a, b) -> float:
    return SequenceMatcher(None, a, b).ratio()


def get_organization(data: dict) -> str:
    orga = ""
    if data.get("organization") is not None:
        if "name" in data["organization"]:
            orga = f"(Organisation {data['organization']['name']})"
    if data.get("owner") is not None:
        if "first_name" in data["owner"]:
            orga = f"(Utilisateur {data['owner']['first_name']} {data['owner']['last_name']})"
    return orga


def schema_suspicion(catalog: dict, resource: dict, orga: str):
    schemas = [schema["title"] for schema in catalog]
    best_score = 0
    schema_title = ""
    for schema in schemas:
        score = similar(schema, resource["name"])
        if score > best_score:
            best_score = score
            schema_title = schema
    if best_score > 0.6:
        message = (
            ":mega: Nouveau jeu de donnée suspecté d'appartenir au schéma "
            f"**{schema_title}** {orga}: \n - [{resource['name']}]({resource['page']})"
        )
        send_message(message, MATTERMOST_DATAGOUV_SCHEMA_ACTIVITE)


def parse_schema_catalog(catalog: dict, resource: dict) -> tuple:
    for schema in catalog:
        if schema["name"] == resource["schema"]["name"]:
            if schema["schema_type"] == "tableschema":
                schema_type = "tableschema"
                result2 = requests.get(
                    f"{VALIDATA_BASE_URL}/validate?schema="
                    f"{schema['schema_url']}&url={resource['url']}"
                )
                validata_url = None
                try:
                    is_valid = result2.json()["report"]["valid"]
                    validata_url = (
                        f"{VALIDATA_BASE_URL}/table-schema?input=url&url="
                        f"{resource['url']}&schema_url={schema['schema_url']}"
                    )
                except Exception:
                    is_valid = False
            else:
                schema_type = "other"
            return (
                schema["title"],
                resource["extras"].get("publish_source") == "publier.etalab.studio",
                schema_type,
                validata_url,
                is_valid,
            )
    return None, None, None, None, None


def parse_resource_if_schema(
    catalog: dict,
    resource: dict,
    item: dict,
    orga: str,
) -> bool:
    if resource["schema"]:
        (
            schema_name,
            publierDetection,
            schema_type,
            validata_url,
            is_valid,
        ) = parse_schema_catalog(catalog, resource)
        if not schema_name:
            schema_name = resource["schema"]["name"]
        message = (
            ":mega: Nouvelle ressource déclarée appartenant au schéma "
            f"**{schema_name}** {orga}: \n - [Lien vers le jeu de donnée]({item['page']})"
        )
        if schema_type == "tableschema":
            if is_valid:
                message += f"\n - [Ressource valide]({validata_url}) :partying_face:"
            else:
                message += f"\n - [Ressource non valide]({validata_url}) :weary:"
        if publierDetection:
            message += "\n - Made with publier.etalab.studio :doge-cool:"
        send_message(message, MATTERMOST_DATAGOUV_SCHEMA_ACTIVITE)
        return True
    return False


@task()
def check_schema(**context):
    nb_datasets = float(context["ti"].xcom_pull(key="nb", task_ids="check_new_datasets"))
    datasets = context["ti"].xcom_pull(key="datasets", task_ids="check_new_datasets")
    r = requests.get("https://schema.data.gouv.fr/schemas/schemas.json")
    catalog = r.json()["schemas"]
    if nb_datasets > 0:
        for item in datasets:
            r = requests.get(
                item["page"].replace("data.gouv.fr/", "data.gouv.fr/api/1/")
            )
            data = r.json()
            orga = get_organization(data)
            try:
                for r in data["resources"]:
                    is_schema = parse_resource_if_schema(
                        catalog, r, item, orga
                    )

                if not is_schema:
                    schema_suspicion(catalog, item, orga)
            except Exception:
                pass


@task()
def send_spam_to_grist(**context):
    records = []
    for _type in [
        "datasets",
        "reuses",
        "organizations",
        "dataservices",
    ]:
        arr = context["ti"].xcom_pull(key=_type, task_ids=f"check_new_{_type}")
        logging.info(arr)
        if not arr:
            continue
        for obj in arr:
            if obj.get("spam"):
                obj["type"] = _type
                # standardize title and name for clarity
                if obj.get("title"):
                    obj["name"] = obj["title"]
                    del obj["title"]
                records.append(obj)
    if records:
        df = pd.DataFrame(records)
        df["date"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        GristTable(grist_curation, "Alertes_spam_potentiel").from_dataframe(
            df, append="lazy"
        )


def publish_item(item, item_type):
    if item_type == "dataset":
        message = ":loudspeaker: :label: Nouveau **Jeu de données** :\n"
    elif item_type == "reuse":
        message = ":loudspeaker: :art: Nouvelle **réutilisation** : \n"
    else:
        message = ":loudspeaker: :robot_face: Nouvelle **API** : \n"

    if item["owner_type"] == "organization":
        message += f"Organisation : [{item['owner_name']}]"
        message += f"(https://data.gouv.fr/{item['owner_type']}s/{item['owner_id']}/)"
    elif item["owner_type"] == "user":
        message += f"Utilisateur : [{item['owner_name']}]"
        message += f"(https://data.gouv.fr/{item['owner_type']}s/{item['owner_id']}/)"
    else:
        message += "**/!\\ sans rattachement**"
    message += f"\n*{item['title'].strip()}* \n\n\n:point_right: {item['page']}"
    send_message(message, MATTERMOST_DATAGOUV_ACTIVITES)

    if item["first_publication"] or item["spam"]:
        if item["spam"]:
            message = f":warning: @all Spam potentiel ({item['spam']})\n"
        else:
            message = ""

        if item["first_publication"]:
            message += ":loudspeaker: :one: "
            message += "Premier " if item_type == "dataset" else "Première "

        if item_type == "dataset":
            message += ":books: jeu de données "
        elif item_type == "reuse":
            message += ":recycle: réutilisation "
        else:
            message += ":robot_face: API "

        if item["owner_type"] == "organization":
            message += f"de l'organisation : [{item['owner_name']}]"
            message += (
                f"(https://data.gouv.fr/{item['owner_type']}s/{item['owner_id']}/)"
            )
        elif item["owner_type"] == "user":
            message += f"de l'utilisateur : [{item['owner_name']}]"
            message += (
                f"(https://data.gouv.fr/{item['owner_type']}s/{item['owner_id']}/)"
            )
        else:
            message += "**/!\\ sans rattachement**"
        message += f"\n*{item['title'].strip()}* \n\n\n:point_right: {item['page']}"
        send_message(message, MATTERMOST_MODERATION_NOUVEAUTES)


@task()
def publish_mattermost(**context):
    nb_datasets = float(context["ti"].xcom_pull(key="nb", task_ids="check_new_datasets"))
    datasets = context["ti"].xcom_pull(key="datasets", task_ids="check_new_datasets")
    nb_reuses = float(context["ti"].xcom_pull(key="nb", task_ids="check_new_reuses"))
    reuses = context["ti"].xcom_pull(key="reuses", task_ids="check_new_reuses")
    nb_orgas = float(context["ti"].xcom_pull(key="nb", task_ids="check_new_organizations"))
    orgas = context["ti"].xcom_pull(key="organizations", task_ids="check_new_organizations")
    nb_dataservices = float(context["ti"].xcom_pull(key="nb", task_ids="check_new_dataservices"))
    dataservices = context["ti"].xcom_pull(key="dataservices", task_ids="check_new_dataservices")
    # spam_comments = ti.xcom_pull(key="spam_comments", task_ids="check_new_comments")

    if nb_orgas > 0:
        for item in orgas:
            if item["spam"]:
                message = f":warning: @all Spam potentiel ({item['spam']})\n"
            else:
                message = ""
            if item["duplicated"]:
                message += ":busts_in_silhouette: Duplicata potentiel\n"
            else:
                message += ""
            if item["potential_certif"]:
                message += ":ballot_box_with_check: Certification potentielle\n"
            else:
                message += ""
            message += (
                ":loudspeaker: :office: Nouvelle **organisation** : "
                f"*{item['name'].strip()}* \n\n\n:point_right: {item['page']}"
            )
            send_message(message, MATTERMOST_MODERATION_NOUVEAUTES)

    if nb_datasets > 0:
        for item in datasets:
            publish_item(item, "dataset")

    if nb_reuses > 0:
        for item in reuses:
            publish_item(item, "reuse")

    if nb_dataservices > 0:
        for item in dataservices:
            publish_item(item, "dataservice")

    # removing notifications for discussions due to https://github.com/opendatateam/udata/pull/2954
    # if spam_comments:
    #     for comment in spam_comments:
    #         if comment['discussion_subject']['class'] in ['Dataset', 'Reuse']:
    #             discussion_url = (
    #                 f"https://www.data.gouv.fr/{comment['discussion_subject']['class'].lower()}s/"
    #                 f"{comment['discussion_subject']['id']}/#/"
    #                 f"discussions/{comment['comment_id'].split('|')[0]}"
    #             )
    #         else:
    #             discussion_url = (
    #                 f"https://www.data.gouv.fr/api/1/discussions/{comment['comment_id'].split('|')[0]}"
    #             )
    #         owner_url = (
    #             f"https://www.data.gouv.fr/{comment['comment']['posted_by']['class'].lower()}s/"
    #             f"{comment['comment']['posted_by']['id']}/"
    #         )
    #         message = (
    #             ':warning: @all Spam potentiel\n'
    #             ':right_anger_bubble: Commentaire suspect de'
    #             f' [{comment["comment"]["posted_by"]["slug"]}]({owner_url})'
    #             f' dans la discussion :\n:point_right: {discussion_url}'
    #         )
    #         send_message(message, MATTERMOST_MODERATION_NOUVEAUTES)
