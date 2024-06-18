import requests
from IPython.core.display import display, HTML
from datagouvfr_data_pipelines.utils.datagouv import (
    get_last_items,
    get_latest_comments,
    check_duplicated_orga,
)


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
    ).json()
    return r["metrics"][content_type] < 2


def show_users(start_date, end_date=None):
    users = get_last_items("users", start_date, end_date, date_key="since",)

    show_html(
        f"<h3>{len(users)} utilisateur{'s' if len(users) > 1 else ''} "
        f"créé{'s' if len(users) > 1 else ''}</h3>"
    )

    for user in users:
        html = make_link(fullname(user), user["page"])
        if user["website"]:
            html += " avec comme site "
            html += make_link(user["website"], user["website"])
        show_html(html)
    return len(users), users


def show_datasets(start_date, end_date=None):
    datasets = get_last_items(
        "datasets",
        start_date,
        end_date,
        date_key="internal.created_at_internal"
    )

    show_html(
        f"<h3>{len(datasets)} jeu{'x' if len(datasets) > 1 else ''} "
        f"de données créé{'s' if len(datasets) > 1 else ''}</h3>"
    )
    first_datasets = []
    other_datasets = []

    for dataset in datasets:
        is_first = None
        if not dataset["organization"] and not dataset["owner"]:
            owner = "- orphan -"
            owner_url = "#"
        else:
            if dataset["organization"]:
                owner = dataset["organization"]["name"]
                owner_url = dataset["organization"]["page"]
                is_first = is_first_content(
                    dataset["organization"]["id"],
                    "organizations",
                    "datasets",
                )
            else:
                owner = fullname(dataset["owner"])
                owner_url = dataset["owner"]["page"]
                is_first = is_first_content(
                    dataset["owner"]["id"],
                    "users",
                    "datasets",
                )
        html = make_link(dataset["title"], dataset["page"])
        html += " par "
        html += make_link(owner, owner_url)
        if is_first:
            first_datasets.append(html)
        else:
            other_datasets.append(html)
    if first_datasets:
        show_html(
            f"<h4>Dont {len(first_datasets)} "
            f"premier{'s' if len(first_datasets) > 1 else ''} "
            f"jeu{'x' if len(first_datasets) > 1 else ''} de données :</h4>"
        )
        for d in first_datasets:
            show_html(d)
        if other_datasets:
            show_html(
                f"<h4>Et {len(other_datasets)} autre{'s' if len(other_datasets) > 1 else ''} "
                f"jeu{'x' if len(other_datasets) > 1 else ''} de données :</h4>"
            )
            for d in other_datasets:
                show_html(d)
    else:
        for d in other_datasets:
            show_html(d)
    return len(datasets), datasets


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


def show_reuses(start_date, end_date=None):
    reuses = get_last_items("reuses", start_date, end_date)

    show_html(
        f"<h3>{len(reuses)} réutilisation{'s' if len(reuses) > 1 else ''} "
        f"créée{'s' if len(reuses) > 1 else ''}</h3>"
    )
    first_reuses = []
    other_reuses = []

    for reuse in reuses:
        owner = None
        owner_url = "#"
        is_first = None
        if reuse["organization"]:
            owner = reuse["organization"]["name"]
            owner_url = reuse["organization"]["page"]
            is_first = is_first_content(
                reuse["organization"]["id"],
                "organizations",
                "reuses",
            )
        elif reuse["owner"]:
            owner = fullname(reuse["owner"])
            owner_url = reuse["owner"]["page"]
            is_first = is_first_content(
                reuse["owner"]["id"],
                "users",
                "reuses",
            )
        html = make_link(reuse['title'], reuse['page'])
        html += " par "
        html += make_link(owner, owner_url)
        if is_first:
            first_reuses.append(html)
        else:
            other_reuses.append(html)
    if first_reuses:
        show_html(
            f"<h4>Dont {len(first_reuses)} "
            f"première{'s' if len(first_reuses) > 1 else ''} "
            f"réutilisation{'s' if len(first_reuses) > 1 else ''} :</h4>"
        )
        for r in first_reuses:
            show_html(r)
        if other_reuses:
            show_html(
                f"<h4>Et {len(other_reuses)} autre{'s' if len(other_reuses) > 1 else ''} "
                f"réutilisation{'s' if len(other_reuses) > 1 else ''} :</h4>")
            for r in other_reuses:
                show_html(r)
    else:
        for r in other_reuses:
            show_html(r)
    return len(reuses), reuses


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
