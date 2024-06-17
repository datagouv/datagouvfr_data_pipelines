from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime, time as dtime
from difflib import SequenceMatcher
from datagouvfr_data_pipelines.config import (
    MATTERMOST_DATAGOUV_ACTIVITES,
    MATTERMOST_DATAGOUV_SCHEMA_ACTIVITE,
    MATTERMOST_MODERATION_NOUVEAUTES
)
from datagouvfr_data_pipelines.utils.mattermost import send_message
from datagouvfr_data_pipelines.utils.datagouv import (
    get_last_items,
    get_latest_comments,
    get_all_from_api_query,
    get_awaiting_spam_comments,
    SPAM_WORDS,
)
from datagouvfr_data_pipelines.utils.utils import check_if_monday, time_is_between
import requests
import re
from langdetect import detect, LangDetectException
from unidecode import unidecode
from time import sleep

DAG_NAME = "dgv_notification_activite"

TIME_PERIOD = {"minutes": 5}
duplicate_slug_pattern = r'-\d+$'
entreprises_api_url = "https://recherche-entreprises.api.gouv.fr/search?q="


def detect_spam(name, description):
    contains_spam_word = any([
        unidecode(spam) in unidecode(field.lower()) if field else False
        for spam in SPAM_WORDS
        for field in [name, description]
    ])
    if not description or len(description) < 30:
        doesnt_look_french = False
    else:
        try:
            doesnt_look_french = detect(description.lower()) != 'fr'
        except LangDetectException:
            doesnt_look_french = False
    return contains_spam_word or doesnt_look_french


def detect_potential_certif(siret):
    if siret is None:
        return False
    try:
        r = requests.get(entreprises_api_url + siret).json()
    except:
        sleep(1)
        r = requests.get(entreprises_api_url + siret).json()
    if len(r['results']) == 0:
        print('No match for: ', siret)
        return False
    if len(r['results']) > 1:
        print('Ambiguous: ', siret)
    complements = r['results'][0]['complements']
    return bool(complements['collectivite_territoriale'] or complements['est_service_public'])


def check_new(ti, **kwargs):
    templates_dict = kwargs.get("templates_dict")
    # we want everything that happened since this date
    start_date = datetime.now() - timedelta(**TIME_PERIOD)
    end_date = datetime.now()
    items = get_last_items(templates_dict["type"], start_date, end_date)
    # items = get_last_items(templates_dict['type'], start_date)
    ti.xcom_push(key="nb", value=str(len(items)))
    arr = []
    for item in items:
        mydict = {}
        for k in ["name", "title", "page"]:
            if k in item:
                mydict[k] = item[k]
        # add field to check if it's the first publication of this type
        # for this organization/user, and check for potential spam
        mydict['duplicated'] = False
        mydict['potential_certif'] = False
        if templates_dict["type"] != 'organizations':
            mydict['spam'] = False
            # if certified orga, no spam check
            badges = item['organization'].get('badges', []) if item.get('organization', None) else []
            if 'certified' not in [badge['kind'] for badge in badges]:
                mydict['spam'] = detect_spam(item['title'], item['description'])
            if item['organization']:
                owner = requests.get(
                    f"https://data.gouv.fr/api/1/organizations/{item['organization']['id']}/"
                ).json()
                mydict['owner_type'] = "organization"
                mydict['owner_name'] = owner['name']
                mydict['owner_id'] = owner['id']
            elif item['owner']:
                owner = requests.get(
                    f"https://data.gouv.fr/api/1/users/{item['owner']['id']}/"
                ).json()
                mydict['owner_type'] = "user"
                mydict['owner_name'] = owner['slug']
                mydict['owner_id'] = owner['id']
            else:
                mydict['owner_type'] = None
            if mydict['owner_type'] and owner['metrics'][templates_dict["type"]] < 2:
                # if it's a dataset and it's labelled with a schema and not potential spam, no ping
                # NB: this is to prevent being pinged for entities publishing small data (IRVE, LOM...)
                if (
                    templates_dict["type"] == 'datasets'
                    and any([r['schema'] for r in item['resources']])
                    and not mydict['spam']
                ):
                    print("This dataset has a schema:", item)
                    mydict['first_publication'] = False
                else:
                    mydict['first_publication'] = True
            else:
                mydict['first_publication'] = False
        else:
            mydict['spam'] = detect_spam(item['name'], item['description'])
            mydict['potential_certif'] = detect_potential_certif(item['business_number_id'])
            # checking for potential duplicates in organization creation
            slug = item["slug"]
            if re.search(duplicate_slug_pattern, slug) is not None:
                suffix = re.findall(duplicate_slug_pattern, slug)[0]
                original_orga = slug[:-len(suffix)]
                test_orga = requests.get(f"https://data.gouv.fr/api/1/organizations/{original_orga}/")
                # only considering a duplicate if the original slug is taken (not not found or deleted)
                if test_orga.status_code not in [404, 410]:
                    mydict['duplicated'] = True
        arr.append(mydict)
    ti.xcom_push(key=templates_dict["type"], value=arr)


def check_user_wave():
    start_date = datetime.now() - timedelta(hours=1)
    end_date = datetime.now()
    items = get_last_items("users", start_date, end_date)
    if len(items) >= 50:
        send_message(
            f":warning: @all {len(items)} utilisateurs créés en 1h",
            MATTERMOST_MODERATION_NOUVEAUTES
        )


def get_inactive_orgas(cutoff_days=30, days_before_flag=7):
    # DAG runs every 5min, we want this to run every Monday at ~10:00
    start, end = dtime(9, 1, 0), dtime(9, 6, 0)
    if not (check_if_monday() and time_is_between(start, end)):
        print("Not running now")
        return
    orgas = get_all_from_api_query(
        "https://www.data.gouv.fr/api/1/organizations/?sort=-created",
        mask="data{id,metrics,created_at,name}",
    )
    inactive = {}
    threshold = (datetime.today() - timedelta(days=cutoff_days)).strftime("%Y-%m-%d")
    too_soon = (datetime.today() - timedelta(days=days_before_flag)).strftime("%Y-%m-%d")
    for o in orgas:
        if o["created_at"] < threshold:
            print("Too old:", o["id"])
            break
        if o["created_at"] >= too_soon:
            print("Too recent:", o["id"])
            continue
        if all(o["metrics"][k] == 0 for k in ["datasets", "reuses"]):
            inactive[o["id"]] = o["name"]
    if inactive:
        message = (
            f"#### :sloth: Organisations créées il y a entre {days_before_flag} et "
            f"{cutoff_days} jours, sans dataset ni réutilisation\n- "
        )
        message += (
            "\n- ".join([
                f"[{n}](https://www.data.gouv.fr/fr/organizations/{i}/)"
                for i, n in inactive.items()
            ])
        )
        send_message(message, MATTERMOST_MODERATION_NOUVEAUTES)


def alert_if_awaiting_spam_comments():
    # DAG runs every 5min, we want this to run everyday at ~11:00
    start, end = dtime(8, 1, 0), dtime(8, 6, 0)
    if not time_is_between(start, end):
        print("Not running now")
        return
    comments = get_awaiting_spam_comments()
    if comments:
        n = len(comments)
        message = (
            f"@all Il y a {n} commentaire{'s' if n > 1 else ''} en attente "
            "de validation (voir [ici](https://www.data.gouv.fr/api/1/spam/))"
        )
        send_message(message, MATTERMOST_MODERATION_NOUVEAUTES)


def check_new_comments(ti):
    latest_comments = get_latest_comments(
        start_date=datetime.now() - timedelta(**TIME_PERIOD)
    )
    spam_comments = [
        k for k in latest_comments if detect_spam('', k['comment']['content'].replace('\n', ' '))
    ]
    ti.xcom_push(key="spam_comments", value=spam_comments)


def similar(a, b):
    return SequenceMatcher(None, a, b).ratio()


def get_organization(data):
    orga = ""
    if data["organization"] is not None:
        if "name" in data["organization"]:
            orga = f"(Organisation {data['organization']['name']})"
    if data["owner"] is not None:
        if "first_name" in data["owner"]:
            orga = f"(Utilisateur {data['owner']['first_name']} {data['owner']['last_name']})"
    return orga


def schema_suspicion(catalog, resource, orga):
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


def parse_schema_catalog(
    schema, resource, schema_name, publierDetection, schema_type, validata_url
):
    if schema["name"] == resource["schema"]["name"]:
        schema_name = schema["title"]
        publierDetection = False
        if "publish_source" in resource["extras"]:
            if resource["extras"]["publish_source"] == "publier.etalab.studio":
                publierDetection = True
        if schema["schema_type"] == "tableschema":
            schema_type = "tableschema"
            result2 = requests.get(
                "https://api.validata.etalab.studio/validate?schema="
                f"{schema['schema_url']}&url={resource['url']}"
            )
            try:
                res = result2.json()["report"]["valid"]
                validata_url = (
                    "https://validata.fr/table-schema?input=url&url="
                    f"{resource['url']}&schema_url={schema['schema_url']}"
                )
            except:
                res = False
        else:
            schema_type = "other"
    return schema_name, publierDetection, schema_type, res, validata_url


def parse_resource_if_schema(catalog, resource, item, orga, is_schema):
    if resource["schema"]:
        is_schema = True
        schema_name = None
        publierDetection = False
        schema_type = ""
        res = None
        validata_url = ""
        for s in catalog:
            (
                schema_name,
                publierDetection,
                schema_type,
                res,
                validata_url,
            ) = parse_schema_catalog(
                s,
                resource,
                schema_name,
                publierDetection,
                schema_type,
                res,
                validata_url,
            )
        if not schema_name:
            schema_name = resource["schema"]["name"]
        message = (
            ":mega: Nouvelle ressource déclarée appartenant au schéma "
            f"**{schema_name}** {orga}: \n - [Lien vers le jeu de donnée]({item['page']})"
        )
        if schema_type == "tableschema":
            if res:
                message += f"\n - [Ressource valide]({validata_url}) :partying_face:"
            else:
                message += f"\n - [Ressource non valide]({validata_url}) :weary:"
        if publierDetection:
            message += "\n - Made with publier.etalab.studio :doge-cool:"
        send_message(message, MATTERMOST_DATAGOUV_SCHEMA_ACTIVITE)
    return is_schema


def check_schema(ti):
    nb_datasets = float(ti.xcom_pull(key="nb", task_ids="check_new_datasets"))
    datasets = ti.xcom_pull(key="datasets", task_ids="check_new_datasets")
    r = requests.get("https://schema.data.gouv.fr/schemas/schemas.json")
    catalog = r.json()["schemas"]
    if nb_datasets > 0:
        for item in datasets:
            r = requests.get(
                item["page"].replace("data.gouv.fr/fr/", "data.gouv.fr/api/1/")
            )
            data = r.json()
            orga = get_organization(data)
            try:
                is_schema = False
                for r in data["resources"]:
                    is_schema = parse_resource_if_schema(
                        catalog, r, item, orga, is_schema
                    )

                if not is_schema:
                    schema_suspicion(catalog, item, orga)
            except:
                pass


def publish_item(item, item_type):
    if item_type == "dataset":
        message = ":loudspeaker: :label: Nouveau **Jeu de données** :\n"
    else:
        message = ":loudspeaker: :art: Nouvelle **réutilisation** : \n"

    if item['owner_type'] == "organization":
        message += f"Organisation : [{item['owner_name']}]"
        message += f"(https://data.gouv.fr/fr/{item['owner_type']}s/{item['owner_id']}/)"
    elif item['owner_type'] == "user":
        message += f"Utilisateur : [{item['owner_name']}]"
        message += f"(https://data.gouv.fr/fr/{item['owner_type']}s/{item['owner_id']}/)"
    else:
        message += "**/!\\ sans rattachement**"
    message += f"\n*{item['title'].strip()}* \n\n\n:point_right: {item['page']}"
    send_message(message, MATTERMOST_DATAGOUV_ACTIVITES)

    if item['first_publication']:
        if item['spam']:
            message = ':warning: @all Spam potentiel\n'
        else:
            message = ''

        if item_type == "dataset":
            message += ":loudspeaker: :one: Premier jeu de données "
        else:
            message += ":loudspeaker: :one: Première réutilisation "

        if item['owner_type'] == "organization":
            message += f"de l'organisation : [{item['owner_name']}]"
            message += f"(https://data.gouv.fr/fr/{item['owner_type']}s/{item['owner_id']}/)"
        elif item['owner_type'] == "user":
            message += f"de l'utilisateur : [{item['owner_name']}]"
            message += f"(https://data.gouv.fr/fr/{item['owner_type']}s/{item['owner_id']}/)"
        else:
            message += "**/!\\ sans rattachement**"
        message += f"\n*{item['title'].strip()}* \n\n\n:point_right: {item['page']}"
        send_message(message, MATTERMOST_MODERATION_NOUVEAUTES)


def publish_mattermost(ti):
    nb_datasets = float(ti.xcom_pull(key="nb", task_ids="check_new_datasets"))
    datasets = ti.xcom_pull(key="datasets", task_ids="check_new_datasets")
    nb_reuses = float(ti.xcom_pull(key="nb", task_ids="check_new_reuses"))
    reuses = ti.xcom_pull(key="reuses", task_ids="check_new_reuses")
    nb_orgas = float(ti.xcom_pull(key="nb", task_ids="check_new_orgas"))
    orgas = ti.xcom_pull(key="organizations", task_ids="check_new_orgas")
    # spam_comments = ti.xcom_pull(key="spam_comments", task_ids="check_new_comments")

    if nb_orgas > 0:
        for item in orgas:
            if item['spam']:
                message = ':warning: @all Spam potentiel\n'
            else:
                message = ''
            if item['duplicated']:
                message += ':busts_in_silhouette: Duplicata potentiel\n'
            else:
                message += ''
            if item['potential_certif']:
                message += ':ballot_box_with_check: Certification potentielle @clarisse\n'
            else:
                message += ''
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

    # removing notifications for discussions due to https://github.com/opendatateam/udata/pull/2954
    # if spam_comments:
    #     for comment in spam_comments:
    #         if comment['discussion_subject']['class'] in ['Dataset', 'Reuse']:
    #             discussion_url = (
    #                 f"https://www.data.gouv.fr/fr/{comment['discussion_subject']['class'].lower()}s/"
    #                 f"{comment['discussion_subject']['id']}/#/"
    #                 f"discussions/{comment['comment_id'].split('|')[0]}"
    #             )
    #         else:
    #             discussion_url = (
    #                 f"https://www.data.gouv.fr/api/1/discussions/{comment['comment_id'].split('|')[0]}"
    #             )
    #         owner_url = (
    #             f"https://www.data.gouv.fr/fr/{comment['comment']['posted_by']['class'].lower()}s/"
    #             f"{comment['comment']['posted_by']['id']}/"
    #         )
    #         message = (
    #             ':warning: @all Spam potentiel\n'
    #             ':right_anger_bubble: Commentaire suspect de'
    #             f' [{comment["comment"]["posted_by"]["slug"]}]({owner_url})'
    #             f' dans la discussion :\n:point_right: {discussion_url}'
    #         )
    #         send_message(message, MATTERMOST_MODERATION_NOUVEAUTES)


default_args = {
    "email": ["geoffrey.aldebert@data.gouv.fr"],
    "email_on_failure": False,
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id=DAG_NAME,
    schedule_interval=f"*/{TIME_PERIOD['minutes']} * * * *",
    start_date=days_ago(0, hour=1),
    dagrun_timeout=timedelta(minutes=60),
    tags=["notification", "hourly", "datagouv", "activite", "schemas"],
    default_args=default_args,
    catchup=False,
) as dag:
    check_new_datasets = PythonOperator(
        task_id="check_new_datasets",
        python_callable=check_new,
        templates_dict={"type": "datasets"},
    )

    check_new_reuses = PythonOperator(
        task_id="check_new_reuses",
        python_callable=check_new,
        templates_dict={"type": "reuses"},
    )

    check_new_orgas = PythonOperator(
        task_id="check_new_orgas",
        python_callable=check_new,
        templates_dict={"type": "organizations"},
    )

    # check_new_comments = PythonOperator(
    #     task_id="check_new_comments",
    #     python_callable=check_new_comments,
    # )

    publish_mattermost = PythonOperator(
        task_id="publish_mattermost",
        python_callable=publish_mattermost,
    )

    check_schema = PythonOperator(
        task_id="check_schema",
        python_callable=check_schema,
    )

    get_inactive_orgas = PythonOperator(
        task_id="get_inactive_orgas",
        python_callable=get_inactive_orgas,
    )

    alert_if_awaiting_spam_comments = PythonOperator(
        task_id="alert_if_awaiting_spam_comments",
        python_callable=alert_if_awaiting_spam_comments,
    )

    check_user_wave = PythonOperator(
        task_id="check_user_wave",
        python_callable=check_user_wave,
    )

    publish_mattermost.set_upstream(check_new_datasets)
    publish_mattermost.set_upstream(check_new_reuses)
    publish_mattermost.set_upstream(check_new_orgas)

    check_schema.set_upstream(publish_mattermost)

    get_inactive_orgas
    alert_if_awaiting_spam_comments
    check_user_wave
