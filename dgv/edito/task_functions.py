from datetime import date

import pandas as pd
import requests
from airflow.sdk import task

# import tweepy
from datagouvfr_data_pipelines.config import (
    # TWITTER_CONSUMER_KEY,
    # TWITTER_CONSUMER_KEY_SECRET,
    # TWITTER_ACCESS_TOKEN,
    # TWITTER_SECRET_TOKEN,
    TCHAP_ROOM_MODERATION_NOUVEAUTES,
)
from datagouvfr_data_pipelines.utils.datagouv import create_post
from datagouvfr_data_pipelines.utils.tchap import send_message
from dateutil.relativedelta import relativedelta

DATAGOUV_URL = "https://www.data.gouv.fr"
NOW = date.today()
LAST_MONTH_DATE = NOW + relativedelta(months=-1)
LAST_MONTH_DATE_FMT = LAST_MONTH_DATE.strftime("%Y-%m")
MONTHS = [
    "Janvier",
    "Février",
    "Mars",
    "Avril",
    "Mai",
    "Juin",
    "Juillet",
    "Août",
    "Septembre",
    "Octobre",
    "Novembre",
    "Décembre",
]
LAST_MONTH_DATE_STR_SHORT = f"{MONTHS[LAST_MONTH_DATE.month - 1]}"
LAST_MONTH_DATE_STR = (
    f"{MONTHS[LAST_MONTH_DATE.month - 1]} {LAST_MONTH_DATE.strftime('%Y')}"
)
# French elision: "d'avril", "d'août", "d'octobre" but "de mars". The previous code
# tested startswith("a"/"o") on a capitalised month name, so elision never triggered
# (it produced "de Avril").
LAST_MONTH_ELISION = "d'" if LAST_MONTH_DATE_STR[0].lower() in "ao" else "de "


# def tweet_featured_from_catalog(url, obj_type, phrase_intro):
#     authenticator = tweepy.OAuthHandler(
#         TWITTER_CONSUMER_KEY, TWITTER_CONSUMER_KEY_SECRET
#     )
#     authenticator.set_access_token(TWITTER_ACCESS_TOKEN, TWITTER_SECRET_TOKEN)

#     api = tweepy.API(authenticator, wait_on_rate_limit=True)

#     df = pd.read_csv(url, sep=";")
#     nb_items = df[(df["created_at"].str.match(LAST_MONTH_DATE_FMT))].shape[0]
#     df = df[
#         (df["created_at"].str.match(LAST_MONTH_DATE_FMT)) & (df["featured"] is True)
#     ]
#     df["title_bis"] = df["title"].apply(
#         lambda x: x[:215] + "[...]" if len(x) > 215 else x
#     )
#     df["tweet"] = (
#         df["title_bis"] + " https://data.gouv.fr/" + obj_type + "/" + df["id"]
#     )

#     tweets = list(df["tweet"].unique())

#     intro = (
#         "En "
#         + LAST_MONTH_DATE_STR_SHORT
#         + ", "
#         + str(nb_items)
#         + " "
#         + phrase_intro
#         + " sur data.gouv.fr. \n Découvrez nos coups de coeur dans ce fil #opendata \n 🔽🔽🔽🔽"
#     )

#     # tweets = intro + tweets
#     original_tweet = api.update_status(status=intro)

#     reply_tweet = original_tweet

#     for tweet in tweets:
#         reply_tweet = api.update_status(
#             status=tweet,
#             in_reply_to_status_id=reply_tweet.id,
#             auto_populate_reply_metadata=True,
#         )

#     return (
#         ":bird: Thread sur les "
#         + obj_type
#         + " du mois dernier publié [ici](https://twitter.com/DatagouvBot/status/"
#         + str(original_tweet.id)
#         + ")"
#     )


# def process_tweeting(**kwargs):
#     dataset_thread = tweet_featured_from_catalog(
#         "https://www.data.gouv.fr/api/1/datasets/r/f868cca6-8da1-4369-a78d-47463f19a9a3",
#         "datasets",
#         "jeux de données ont été publiés",
#     )
#     reuse_thread = tweet_featured_from_catalog(
#         "https://www.data.gouv.fr/api/1/datasets/r/970aafa0-3778-4d8b-b9d1-de937525e379",
#         "reuses",
#         "réutilisations ont été publiées",
#     )

#     kwargs["ti"].xcom_push(
#         key="published_threads", value=[dataset_thread, reuse_thread]
#     )


@task()
def create_edito_post(**context):
    # Get datasets and reuses from catalog
    mapping = {
        "datasets": {
            "catalog_id": "f868cca6-8da1-4369-a78d-47463f19a9a3",
        },
        "reuses": {
            "catalog_id": "970aafa0-3778-4d8b-b9d1-de937525e379",
        },
    }

    for object_type in mapping.keys():
        print("Gathering infos about", object_type)
        df = pd.read_csv(
            f"https://www.data.gouv.fr/api/1/datasets/r/{mapping[object_type]['catalog_id']}",
            sep=";",
            usecols=["id", "slug", "created_at", "featured"],
        )
        recent = df.loc[df.created_at.str.match(LAST_MONTH_DATE_FMT)]
        featured_ids = recent.loc[recent.featured, "id"].tolist()
        count = len(recent)
        ids = recent["id"].to_list()
        visits = []
        for i in ids:
            r = requests.get(
                f"https://metric-api.data.gouv.fr/api/{object_type}/data/"
                f"?metric_month__exact={LAST_MONTH_DATE_FMT}"
                f"&{object_type[:-1]}_id__exact={i}"
            ).json()["data"]
            if r:
                visits.append(r[0]["monthly_visit"])
            else:
                visits.append(0)
        visits = pd.DataFrame(
            {
                "id": ids,
                "visits": visits,
            }
        )
        visits.sort_values(by="visits", ascending=False, inplace=True)
        print(visits)
        mapping[object_type].update(
            {
                "featured_ids": featured_ids,
                "count": count,
                "trending_ids": visits["id"].to_list()[:6],
            }
        )
    print(mapping)

    # Generate blocs: a post is made of sections, each holding its title and its blocs. A
    # dataset or a reuse is a bloc of its own, laid out side by side through its width. The
    # datasets and the reuses of a selection share its section: their cards tell them apart.
    month = LAST_MONTH_DATE_STR.lower()

    def section(*blocs: dict) -> dict:
        return {"class": "SectionBloc", "blocs": list(blocs)}

    def title(title: str, subtitle: str | None = None) -> dict:
        return {"class": "TitleBloc", "title": title, "subtitle": subtitle}

    def datasets(ids: list[str]) -> list[dict]:
        blocs = [{"class": "DatasetBloc", "width": "1/2", "dataset": id} for id in ids]
        # The odd one out takes the whole row: otherwise the first reuse would fill the
        # half row left beside it.
        if len(blocs) % 2:
            del blocs[-1]["width"]
        return blocs

    def reuses(ids: list[str]) -> list[dict]:
        return [{"class": "ReuseBloc", "width": "1/3", "reuse": id} for id in ids]

    blocs = [
        section(
            title(
                "Nos coups de cœur du mois",
                f"Les jeux de données et réutilisations publiés en {month} "
                "qui ont retenu notre attention.",
            ),
            *datasets(mapping["datasets"]["featured_ids"]),
            *reuses(mapping["reuses"]["featured_ids"]),
        ),
        section(
            title(
                "Les tendances du mois",
                "Les jeux de données et réutilisations créés récemment les plus consultés "
                f"au mois {LAST_MONTH_ELISION}{month}.",
            ),
            *datasets(mapping["datasets"]["trending_ids"]),
            *reuses(mapping["reuses"]["trending_ids"]),
        ),
        section(
            title("Suivez l'actualité de la plateforme"),
            {
                "class": "MarkdownBloc",
                "width": "1/2",
                "content": (
                    "Le suivi des sorties ne constitue que le sommet de l'iceberg de l'activité "
                    "de data.gouv.fr. Pour ne rien manquer de l'actualité de data.gouv.fr et de "
                    "l'open data, abonnez-vous à notre infolettre. Et si vous souhaitez nous aider "
                    "à améliorer la plateforme en testant les nouveautés en avant-première, "
                    "devenez beta testeur."
                ),
            },
            {
                "class": "LinksListBloc",
                "width": "1/2",
                "links": [
                    {
                        "title": "S'abonner à l'infolettre",
                        "url": "https://f.info.data.gouv.fr/f/lp/infolettre-data-gouv-fr-landing-page/lk3q01y6",
                    },
                    {
                        "title": "Devenir beta testeur",
                        "url": "https://tally.so/r/mOalMA",
                    },
                ],
            },
        ),
    ]

    print(blocs)

    # Create a POST
    headline = (
        f"Vous lisez l'édition {LAST_MONTH_ELISION}{month} du suivi des sorties : "
        f"{mapping['datasets']['count']} jeux de données et {mapping['reuses']['count']} "
        "réutilisations ont été publiés ce mois-ci sur data.gouv.fr, voici ceux qui ont "
        "retenu notre attention et les plus consultés."
    )
    name = f"Suivi des sorties - {LAST_MONTH_DATE_STR}"

    data = create_post(
        name=name,
        headline=headline,
        body_type="blocs",
        blocs=blocs,
        tags=["suivi-des-sorties"],
    )

    post_id = data["id"]
    print(f"Article créé et éditable à {DATAGOUV_URL}/admin/posts/{post_id}")

    context["ti"].xcom_push(
        key="admin_post_url",
        value=(
            f"🗞️ Article du {name} créé et éditable [dans "
            f"l'espace admin]({DATAGOUV_URL}/admin/posts/{post_id})"
        ),
    )


@task()
def notification(**context):
    admin_post_url = context["ti"].xcom_pull(
        key="admin_post_url", task_ids="create_edito_post"
    )
    print(admin_post_url)

    send_message(
        "📣 " + admin_post_url,
        TCHAP_ROOM_MODERATION_NOUVEAUTES,
        ping=[
            "ludine",
            "antonin",
        ],
    )
