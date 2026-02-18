import json
import re
from datetime import datetime

import requests
from airflow.decorators import task
from datagouvfr_data_pipelines.config import (
    AIRFLOW_DAG_TMP,
    MATTERMOST_DATAGOUV_ACTIVITES,
    MATTERMOST_DATASERVICES_ONLY,
    SECRET_MAIL_DATAGOUV_BOT_PASSWORD,
    SECRET_MAIL_DATAGOUV_BOT_RECIPIENTS_PROD,
    SECRET_MAIL_DATAGOUV_BOT_USER,
)
from datagouvfr_data_pipelines.utils.datagouv import (
    check_duplicated_orga,
    get_last_items,
    get_latest_comments,
)
from datagouvfr_data_pipelines.utils.mails import send_mail_datagouv
from datagouvfr_data_pipelines.utils.mattermost import send_message
from IPython.display import HTML, display

DAG_FOLDER = "datagouvfr_data_pipelines/dgv/monitoring/digest/"
DAG_NAME = "dgv_digests"
TMP_FOLDER = AIRFLOW_DAG_TMP + DAG_NAME

URL_PATTERN = "https?:\/\/[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}(\/[^ \n]*)?"


def show_html(html):
    display(HTML(html))


def make_link(text: str, link: str):
    return f"<a href='{link.replace(' ', '')}' target='_blank'>{text}</a>"


def show_link(text: str, link: str):
    show_html(make_link(text, link))


def fullname(user: dict):
    return user["first_name"] + " " + user["last_name"]


def is_first_content(publisher_id: str, publisher_type: str, content_type: str):
    r = requests.get(
        f"https://www.data.gouv.fr/api/1/{publisher_type}/{publisher_id}/",
        headers={"X-fields": "metrics"},
    )
    if not r.ok:
        return False
    return r.json()["metrics"][content_type] < 2


def get_url(about: str):
    if not about:
        return
    searched = re.search(URL_PATTERN, about)
    if not searched:
        return
    return searched.group(0)


def show_users(start_date: datetime, end_date: datetime | None = None):
    users = get_last_items(
        "users",
        start_date,
        end_date,
        date_key="since",
    )

    show_html(
        f"<h3>{len(users)} utilisateur{'s' if len(users) > 1 else ''} "
        f"créé{'s' if len(users) > 1 else ''}</h3>"
    )

    for user in users:
        html = make_link(fullname(user), user["page"])
        site = user["website"] or get_url(user["about"])
        if site:
            html += " avec comme site "
            html += make_link(site, site)
        show_html(html)
    return len(users), users


params = {
    "datasets": {
        "date_key": "internal.created_at_internal",
        "label_singular": "jeu de données",
        "label_plural": "jeux de données",
    },
    "reuses": {
        "date_key": "created_at",
        "label_singular": "réutilisation",
        "label_plural": "réutilisations",
    },
    "dataservices": {
        "date_key": "created_at",
        "label_singular": "API",
        "label_plural": "APIs",
    },
}


def accorde(object_class: str, nb: int):
    return (
        params[object_class]["label_plural"]
        if nb > 1
        else params[object_class]["label_singular"]
    )


def show_objects(
    object_class: str, start_date: datetime, end_date: datetime | None = None
):
    feminin = "e" if object_class in ["reuses", "dataservices"] else ""
    objects = get_last_items(
        object_class,
        start_date,
        end_date,
        date_key=params[object_class]["date_key"],
    )

    show_html(
        f"<h3>{len(objects)} {accorde(object_class, len(objects))} "
        f"créé{feminin}{'s' if len(objects) > 1 else ''}</h3>"
    )
    first_objects = []
    other_objects = []

    for obj in objects:
        is_first = None
        if not obj["organization"] and not obj["owner"]:
            owner = "- orphan -"
            owner_url = "#"
        elif obj["organization"]:
            owner = obj["organization"]["name"]
            owner_url = obj["organization"]["page"]
            is_first = is_first_content(
                obj["organization"]["id"],
                "organizations",
                "datasets",
            )
        else:
            owner = fullname(obj["owner"])
            owner_url = obj["owner"]["page"]
            is_first = is_first_content(
                obj["owner"]["id"],
                "users",
                "datasets",
            )
        html = (
            make_link(obj["title"], obj.get("page", obj.get("self_web_url")))
            + " par "
            + make_link(owner, owner_url)
        )
        if is_first:
            first_objects.append(html)
        else:
            other_objects.append(html)
    if first_objects:
        show_html(
            f"<h4>Dont {len(first_objects)} "
            f"premier{feminin}{'s' if len(first_objects) > 1 else ''} "
            f"{accorde(object_class, len(first_objects))} :</h4>"
        )
        for d in first_objects:
            show_html(d)
        if other_objects:
            show_html(
                f"<h4>Et {len(other_objects)} autre{'s' if len(other_objects) > 1 else ''} "
                f"{accorde(object_class, len(other_objects))} :</h4>"
            )
            for d in other_objects:
                show_html(d)
    else:
        for d in other_objects:
            show_html(d)
    return len(objects), objects


def show_orgas(start_date: datetime, end_date: datetime | None = None):
    orgs = get_last_items("organizations", start_date, end_date)

    show_html(
        f"<h3>{len(orgs)} organisation{'s' if len(orgs) > 1 else ''} "
        f"créée{'s' if len(orgs) > 1 else ''}</h3>"
    )
    duplicated_orgas = []
    other_orgas = []

    for org in orgs:
        url_dup = check_duplicated_orga(org["slug"])
        if url_dup is not None:
            html = make_link(org["name"], org["page"])
            html += f" (duplicata potentiel avec {make_link(url_dup, url_dup)})"
            duplicated_orgas.append(html)
        else:
            other_orgas.append(make_link(org["name"], org["page"]))
    if duplicated_orgas:
        show_html(
            f"<h4>Dont {len(duplicated_orgas)} organisation{'s' if len(duplicated_orgas) > 1 else ''} "
            f"dupliquée{'s' if len(duplicated_orgas) > 1 else ''} :</h4>"
        )
        for o in duplicated_orgas:
            show_html(o)
        if other_orgas:
            show_html(
                f"<h4>Et {len(other_orgas)} autre{'s' if len(other_orgas) > 1 else ''} "
                f"organisation{'s' if len(other_orgas) > 1 else ''} :</h4>"
            )
            for o in other_orgas:
                show_html(o)
    else:
        for o in other_orgas:
            show_html(o)
    return len(orgs), orgs


def show_discussions(
    start_date: datetime,
    end_date: datetime | None = None,
    subjects_of_interest: list | None = None,
):
    discussions = get_latest_comments(start_date, end_date)

    if not subjects_of_interest:
        show_html(
            f"<h3>{len(discussions)} commentaire{'s' if len(discussions) > 1 else ''} "
            f"créée{'s' if len(discussions) > 1 else ''}</h3>"
        )

    for d in discussions:
        subject = d["discussion_subject"]
        if subjects_of_interest and subject["class"] not in subjects_of_interest:
            continue
        comment = d["comment"]
        try:
            object_title = requests.get(
                f"https://www.data.gouv.fr/api/1/{subject['class'].lower()}s/{subject['id']}/"
            ).json()["title"]
        except Exception:
            object_title = None
        url = "#"
        if subject["class"] in ["Dataset", "Reuse", "Dataservice"]:
            url = (
                f"https://www.data.gouv.fr/{subject['class'].lower()}s/{subject['id']}/"
            )
        user = make_link(fullname(comment["posted_by"]), comment["posted_by"]["page"])
        to_be_shown = (
            object_title + f" ({subject['class']})"
            if object_title
            else subject["class"]
        )
        show_html(
            f"<span>{d['discussion_title']}</span> sur {make_link(to_be_shown, url)} par {user}"
        )
        show_html(f"<pre>{comment['content']}</pre>")
        show_html("<hr/>")

    return len(discussions), discussions


def get_stats_period(today: str, period: str, scope: str) -> str | None:
    with open(
        TMP_FOLDER + f"/digest_{period}/{today}/output/stats.json", "r"
    ) as json_file:
        res = json.load(json_file)
    if scope == "api":
        if not (
            res["stats"]["nb_dataservices"]
            or res["stats"]["nb_discussions_dataservices"]
        ):
            # no message if no new API and no comment
            return
        return (
            f"- {res['stats']['nb_dataservices']} APIs créées\n"
            f"- {res['stats']['nb_discussions_dataservices']} discussions sur les APIs\n"
        )
    recap = (
        f"- {res['stats']['nb_datasets']} datasets créés\n"
        f"- {res['stats']['nb_reuses']} reuses créées\n"
        f"- {res['stats']['nb_dataservices']} dataservices créés\n"
    )
    if period == "daily":
        recap += (
            f"- {res['stats']['nb_orgas']} orgas créées\n"
            f"- {res['stats']['nb_discussions']} discussions créées\n"
            f"- {res['stats']['nb_users']} utilisateurs créés"
        )
    return recap


@task()
def publish_mattermost_period(today: str, period: str, scope: str, **context):
    report_url = context["ti"].xcom_pull(
        key="report_url", task_ids=f"run_notebook_and_save_to_s3_{scope}_{period}"
    )
    stats = get_stats_period(today, period, scope)
    if not stats:
        return
    message = f"{period.title()} Digest : {report_url} \n{stats}"
    channel = (
        MATTERMOST_DATAGOUV_ACTIVITES
        if scope == "general"
        else MATTERMOST_DATASERVICES_ONLY
    )
    send_message(message, channel)


@task()
def send_email_report_period(today: str, period: str, scope: str, **context):
    report_url = context["ti"].xcom_pull(
        key="report_url", task_ids=f"run_notebook_and_save_to_s3_{scope}_{period}"
    )
    message = get_stats_period(today, period, scope) + "<br/><br/>" + report_url
    send_mail_datagouv(
        email_user=SECRET_MAIL_DATAGOUV_BOT_USER,
        email_password=SECRET_MAIL_DATAGOUV_BOT_PASSWORD,
        email_recipients=SECRET_MAIL_DATAGOUV_BOT_RECIPIENTS_PROD,
        subject=f"{period.title()} digest of " + today,
        message=message,
    )
