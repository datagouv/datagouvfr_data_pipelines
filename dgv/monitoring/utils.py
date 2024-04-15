import requests
from IPython.core.display import display, HTML
from datagouvfr_data_pipelines.utils.datagouv import (
    get_last_items,
    get_latest_comments,
)


def show_html(html):
    display(HTML(html))


def make_link(text, link):
    return f"<a href='{link}' target='_blank'>{text}</a>"


def show_link(text, link):
    show_html(make_link(text, link))


def fullname(user):
    return user["first_name"] + " " + user["last_name"]


def show_users(start_date, end_date=None):
    users = get_last_items("users", start_date, end_date, date_key="since",)

    show_html(f"<h3>{len(users)} utilisateurs créés</h3>")

    for user in users:
        show_link(fullname(user), user["page"])
    return len(users), users


def show_datasets(start_date, end_date=None):
    datasets = get_last_items("datasets", start_date, end_date,
                              date_key="internal.created_at_internal")

    show_html(f"<h3>{len(datasets)} jeux de données créés</h3>")

    for dataset in datasets:
        if not dataset["organization"] and not dataset["owner"]:
            owner = "- orphan -"
            owner_url = "#"
        else:
            owner = dataset["organization"]["name"] if dataset["organization"] \
                else fullname(dataset["owner"])
            owner_url = dataset["organization"]["page"] if dataset["organization"] \
                else dataset["owner"]["page"]
        html = make_link(dataset["title"], dataset["page"])
        html += " por "
        html += make_link(owner, owner_url)
        show_html(html)
    return len(datasets), datasets


def show_orgas(start_date, end_date=None):
    orgs = get_last_items("organizations", start_date, end_date)

    show_html(f"<h3>{len(orgs)} organisations créées</h3>")

    for org in orgs:
        show_link(org["name"], org["page"])
    return len(orgs), orgs


def show_reuses(start_date, end_date=None):
    reuses = get_last_items("reuses", start_date, end_date)

    show_html(f"<h3>{len(reuses)} réutilisations créées</h3>")

    for reuse in reuses:
        owner = None
        if reuse["organization"]:
            owner = reuse["organization"]["name"]
        elif reuse["owner"]:
            owner = fullname(reuse["owner"])
        owner_url = "#"
        if owner:
            owner_url = reuse["organization"]["page"] if reuse["organization"] else reuse["owner"]["page"]
        html = make_link(reuse['title'], reuse['page'])
        html += " par "
        html += make_link(owner, owner_url)
        show_html(html)
    return len(reuses), reuses


def show_discussions(start_date, end_date=None):
    discussions = get_latest_comments(start_date, end_date)

    show_html(f"<h3>{len(discussions)} commentaires créées</h3>")

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
