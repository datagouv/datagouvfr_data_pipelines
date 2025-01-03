import re

import requests
from IPython.core.display import display, HTML
from datagouvfr_data_pipelines.utils.datagouv import (
    get_last_items,
    get_latest_comments,
    check_duplicated_orga,
)

URL_PATTERN = "https?:\/\/[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}(\/[^ \n]*)?"


def show_html(html):
    display(HTML(html))


def make_link(text, link):
    return f"<a href='{link}' target='_blank'>{text}</a>"


def show_link(text, link):
    show_html(make_link(text, link))


def fullname(user):
    return user["first_name"] + " " + user["last_name"]


def is_first_content(publisher_id, publisher_type, content_type):
    r = requests.get(
        f"https://www.data.gouv.fr/api/1/{publisher_type}/{publisher_id}/",
        headers={"X-fields": "metrics"},
    )
    if not r.ok:
        return False
    return r.json()["metrics"][content_type] < 2


def get_url(about):
    if not about:
        return
    searched = re.search(URL_PATTERN, about)
    if not searched:
        return
    return searched.group(0)


def show_users(start_date, end_date=None):
    users = get_last_items("users", start_date, end_date, date_key="since",)

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


def accorde(object_class, nb):
    return (
        params[object_class]["label_plural"]
        if nb > 1
        else params[object_class]['label_singular']
    )


def show_objects(object_class, start_date, end_date=None):
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
            + " par " + make_link(owner, owner_url)
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


def show_orgas(start_date, end_date=None):
    orgs = get_last_items("organizations", start_date, end_date)

    show_html(
        f"<h3>{len(orgs)} organisation{'s' if len(orgs) > 1 else ''} "
        f"créée{'s' if len(orgs) > 1 else ''}</h3>"
    )
    duplicated_orgas = []
    other_orgas = []

    for org in orgs:
        is_duplicated, url_dup = check_duplicated_orga(org["slug"])
        if is_duplicated:
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


def show_discussions(start_date, end_date=None):
    discussions = get_latest_comments(start_date, end_date)

    show_html(
        f"<h3>{len(discussions)} commentaire{'s' if len(discussions) > 1 else ''} "
        f"créée{'s' if len(discussions) > 1 else ''}</h3>"
    )

    for d in discussions:
        subject = d['discussion_subject']
        comment = d['comment']
        try:
            object_title = requests.get(
                f'https://www.data.gouv.fr/api/1/{subject["class"].lower()}s/{subject["id"]}/'
            ).json()['title']
        except:
            object_title = None
        url = "#"
        if subject["class"] in ["Dataset", "Reuse"]:
            url = f"https://www.data.gouv.fr/fr/{subject['class'].lower()}s/{subject['id']}/"
        user = make_link(fullname(comment["posted_by"]), comment["posted_by"]["page"])
        to_be_shown = object_title + f" ({subject['class']})" if object_title else subject['class']
        show_html(
            f"{d['discussion_title']} sur {make_link(to_be_shown, url)} par {user}"
        )
        show_html(f"<pre>{comment['content']}</pre>")
        show_html("<hr/>")

    return len(discussions), discussions
