import dateutil
from IPython.core.display import display, HTML
from datagouvfr_data_pipelines.utils.datagouv import (
    DATAGOUV_URL,
    get_all_from_api_query,
    get_last_items,
)


def show_html(html):
    display(HTML(html))


def make_link(text, link):
    return "<a href='%s' target='_blank'>%s</a>" % (link, text)


def show_link(text, link):
    show_html(make_link(text, link))


def fullname(user):
    return "%s %s" % (user["first_name"], user["last_name"])


def get_last_discussions(start_date, end_date=None):
    results = []
    data = get_all_from_api_query(
        f"{DATAGOUV_URL}/api/1/discussions/?sort=discussion__posted_on"
    )
    for d in data:
        _id, subject, title, comments = d["id"], d["subject"], d["title"], d["discussion"]
        createds = [dateutil.parser.parse(c["posted_on"]) for c in comments]
        created = max(createds)
        got_everything = (created.timestamp() < start_date.timestamp())
        if not got_everything:
            for comment in comments:
                if end_date:
                    if (
                        (dateutil.parser.parse(comment["posted_on"]).timestamp() >= start_date.timestamp()) &
                        (dateutil.parser.parse(comment["posted_on"]).timestamp() < end_date.timestamp())
                    ):
                        results.append((_id, subject, title, comment))
                else:
                    if dateutil.parser.parse(comment["posted_on"]) >= start_date:
                        results.append((_id, subject, title, comment))
        else:
            break
    return results


def show_users(start_date, end_date=None):
    users = get_last_items("users", start_date, end_date, date_key="since",)

    show_html("<h3>%s utilisateurs créés</h3>" % len(users))

    for user in users:
        show_link(fullname(user), user["page"])
    return len(users), users


def show_datasets(start_date, end_date=None):
    datasets = get_last_items("datasets", start_date, end_date,
                              date_key="internal.created_at_internal")

    show_html("<h3>%s jeux de données créés</h3>" % len(datasets))

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

    show_html("<h3>%s organisations créées</h3>" % len(orgs))

    for org in orgs:
        show_link(org["name"], org["page"])
    return len(orgs), orgs


def show_reuses(start_date, end_date=None):
    reuses = get_last_items("reuses", start_date, end_date)

    show_html("<h3>%s réutilisations créées</h3>" % len(reuses))

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
    discussions = get_last_discussions(start_date, end_date)

    show_html("<h3>%s commentaires créées</h3>" % len(discussions))

    for d in discussions:
        _id, subject, title, comment = d
        url = "#"
        if subject["class"] == "Dataset":
            url = "https://%s/fr/datasets/%s" % ("www.data.gouv.fr", subject["id"])
        elif subject["class"] == "Reuse":
            url = "https://%s/fr/reuses/%s" % ("www.data.gouv.fr", subject["id"])
        user = make_link(fullname(comment["posted_by"]), comment["posted_by"]["page"])
        show_html("%s sur %s par %s" % (title, make_link(subject["class"], url), user))
        show_html("<pre>%s</pre>" % comment["content"])
        show_html("<hr/>")

    return len(discussions), discussions
