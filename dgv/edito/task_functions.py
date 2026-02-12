import pandas as pd
from datetime import date
from dateutil.relativedelta import relativedelta
import requests
from airflow.decorators import task
# import tweepy

from datagouvfr_data_pipelines.config import (
    # TWITTER_CONSUMER_KEY,
    # TWITTER_CONSUMER_KEY_SECRET,
    # TWITTER_ACCESS_TOKEN,
    # TWITTER_SECRET_TOKEN,
    MATTERMOST_DATAGOUV_EDITO,
)
from datagouvfr_data_pipelines.utils.datagouv import create_post
from datagouvfr_data_pipelines.utils.mattermost import send_message


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
        slugs = recent.loc[recent.featured, "slug"].values
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
                "slug": recent["slug"].to_list(),
                "visits": visits,
            }
        )
        visits.sort_values(by="visits", ascending=False, inplace=True)
        print(visits)
        mapping[object_type].update(
            {
                "featured_slugs": slugs,
                "count": count,
                "trending_slugs": visits["slug"].to_list()[:6],
            }
        )
    print(mapping)

    # Generate HTML
    def list_datasets(datasets):
        out = '<div class="my-4 not-prose space-y-4">\n'
        for slug in datasets:
            out += f'        <div data-udata-dataset="{slug}"></div>\n'
        out += "    </div>\n"
        return out

    def list_reuses(reuses):
        out = '<div class="my-4 not-prose grid gap-4 md:grid-cols-2">\n'
        for slug in reuses:
            out += f'        <div class="flex-1" data-udata-reuse="{slug}"></div>\n'
        out += "    </div>\n"
        return out

    content = f"""
        <h3>
            En {LAST_MONTH_DATE_STR}, {mapping["datasets"]["count"]}
             jeux de données et {mapping["reuses"]["count"]}
             réutilisations ont été publiés sur data.gouv.fr.
        </h3>
        <a
            href="http://activites-datagouv.app.etalab.studio/"
            target="_blank"
        >
            Découvrez plus de statistiques sur l'activité de la plateforme
        </a>.
        <p>
            Retrouvez-ici nos jeux de données et réutilisations coups de coeur du mois,&nbsp;
            ainsi que les publications récentes les plus populaires en {
        LAST_MONTH_DATE_STR
    }.
        </p>
        <div class="fr-my-6w">
            <h3 >Les jeux de données du mois</h3>
            <p>Les jeux de données qui ont retenu notre attention ce mois-ci :</p>
            {list_datasets(mapping["datasets"]["featured_slugs"])}
        </div>
        <div class="fr-my-6w">
            <h3>Les réutilisations du mois</h3>
            <p>Les réutilisations qui ont retenu notre attention ce mois-ci :</p>
            {list_reuses(mapping["reuses"]["featured_slugs"])}
        </div>
        <div class="fr-my-6w">
            <h3>Les tendances du mois sur data.gouv.fr</h3>
            <p>
                <i>
                    Il s'agit des jeux de données et des réutilisations créés récemment&nbsp;
                    les plus consultés au mois&nbsp;
                    {
        "d’"
        if LAST_MONTH_DATE_STR.startswith("a") or LAST_MONTH_DATE_STR.startswith("o")
        else "de "
    }
                    {LAST_MONTH_DATE_STR}.
                </i>
            </p>
            <p>Les jeux de données publiés ce mois-ci les plus populaires :</p>
            {list_datasets(mapping["datasets"]["trending_slugs"])}
            <p>Les réutilisations publiées ce mois-ci les plus populaires :</p>
            {list_reuses(mapping["reuses"]["trending_slugs"])}
        </div>
        <h3>Suivez l’actualité de la plateforme</h3>
        <p>
            Le suivi des sorties ne constitue que le sommet de l’iceberg de l’activité de data.gouv.fr.
            Pour ne rien manquer de l’actualité de data.gouv.fr et de l’open data,
            <a
                href="https://f.info.data.gouv.fr/f/lp/infolettre-data-gouv-fr-landing-page/lk3q01y6"
                target="_blank"
            >
                &nbsp;abonnez-vous à notre infolettre
            </a>.
            <br />
            Et si vous souhaitez nous aider à améliorer la plateforme en testant les nouveautés
             en avant première, n’hésitez pas à
            <a
                href="https://tally.so/r/mOalMA"
                target="_blank"
            >
                devenir beta testeur
            </a>.
        </p>
    """

    print(content)

    # Create a POST
    headline = (
        f"Vous lisez l’édition "
        f"{'d’' if LAST_MONTH_DATE_STR.startswith('a') or LAST_MONTH_DATE_STR.startswith('o') else 'de '}"
        f"{LAST_MONTH_DATE_STR} du suivi des sorties, un article dans lequel nous partageons les "
        f"publications de jeux de données et de réutilisations qui ont retenu notre attention."
    )
    name = f"Suivi des sorties - {LAST_MONTH_DATE_STR}"

    data = create_post(
        name=name,
        headline=headline,
        content=content,
        body_type="html",
        tags=["suivi-des-sorties"],
    )

    post_id = data["id"]
    print(f"Article créé et éditable à {DATAGOUV_URL}/admin/posts/{post_id}")

    context["ti"].xcom_push(
        key="admin_post_url",
        value=(
            f":rolled_up_newspaper: Article du {name} créé et éditable [dans "
            f"l'espace admin]({DATAGOUV_URL}/admin/posts/{post_id})"
        ),
    )


@task()
def publish_mattermost(**context):
    published_threads = context["ti"].xcom_pull(
        key="published_threads", task_ids="tweet_threads"
    )
    admin_post_url = context["ti"].xcom_pull(
        key="admin_post_url", task_ids="create_edito_post"
    )

    print(published_threads)
    print(admin_post_url)

    send_message(
        ":mega: @agarrone @ludine.pierquin \n - " + admin_post_url,
        MATTERMOST_DATAGOUV_EDITO,
    )
